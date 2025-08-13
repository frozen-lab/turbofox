from typing import Optional, Sequence
import os
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots

def plot_bench_csv(
    csv_path: str,
    x_col: str = "db_size",
    smooth_window: int = 1,
    latency_unit: str = "us",
    title: Optional[str] = None,
    save_html: Optional[str] = None,
    show: bool = True,
):
    # validations
    assert latency_unit in ("ns", "us", "ms"), "latency_unit must be 'ns', 'us', or 'ms'"
    assert x_col in ("db_size", "batch_index"), "x_col must be 'db_size' or 'batch_index'"

    df = pd.read_csv(csv_path)

    # Ensure expected columns
    for c in ("mean_ns", "p50_ns", "p95_ns", "ops_per_sec"):
        if c not in df.columns:
            raise ValueError(f"CSV missing expected column: {c}")

    # Convert numeric columns safely
    df["mean_ns"] = pd.to_numeric(df["mean_ns"], errors="coerce")
    df["p50_ns"] = pd.to_numeric(df["p50_ns"], errors="coerce")
    df["p95_ns"] = pd.to_numeric(df["p95_ns"], errors="coerce")
    df["ops_per_sec"] = pd.to_numeric(df["ops_per_sec"], errors="coerce")
    df[x_col] = pd.to_numeric(df[x_col], errors="coerce")

    # Drop rows with NaN x or ops
    df = df.dropna(subset=[x_col]).reset_index(drop=True)

    # Convert latency unit
    unit_div = {"ns": 1.0, "us": 1e3, "ms": 1e6}[latency_unit]
    df["mean"] = df["mean_ns"] / unit_div
    df["p50"] = df["p50_ns"] / unit_div
    df["p95"] = df["p95_ns"] / unit_div

    # Optional smoothing (simple rolling mean)
    if smooth_window > 1:
        df["mean_smooth"] = df["mean"].rolling(window=smooth_window, min_periods=1, center=True).mean()
        df["p50_smooth"] = df["p50"].rolling(window=smooth_window, min_periods=1, center=True).mean()
        df["p95_smooth"] = df["p95"].rolling(window=smooth_window, min_periods=1, center=True).mean()
        mean_col = "mean_smooth"
        p50_col = "p50_smooth"
        p95_col = "p95_smooth"
    else:
        mean_col = "mean"
        p50_col = "p50"
        p95_col = "p95"

    x = df[x_col]

    # Figure with secondary y axis
    fig = make_subplots(specs=[[{"secondary_y": True}]])

    # Latency traces
    fig.add_trace(
        go.Scatter(
            x=x,
            y=df[p95_col],
            mode="lines+markers",
            name=f"p95 ({latency_unit})",
            hovertemplate=f"%{{x}}<br>p95: %{{y:.2f}} {latency_unit}<extra></extra>",
        ),
        secondary_y=False,
    )
    fig.add_trace(
        go.Scatter(
            x=x,
            y=df[mean_col],
            mode="lines+markers",
            name=f"mean ({latency_unit})",
            hovertemplate=f"%{{x}}<br>mean: %{{y:.2f}} {latency_unit}<extra></extra>",
        ),
        secondary_y=False,
    )
    fig.add_trace(
        go.Scatter(
            x=x,
            y=df[p50_col],
            mode="lines+markers",
            name=f"p50 ({latency_unit})",
            hovertemplate=f"%{{x}}<br>p50: %{{y:.2f}} {latency_unit}<extra></extra>",
        ),
        secondary_y=False,
    )

    # ops/sec trace on secondary axis
    fig.add_trace(
        go.Bar(
            x=x,
            y=df["ops_per_sec"],
            name="ops/sec",
            opacity=0.45,
            hovertemplate="%{x}<br>ops/s: %{y:.2f}<extra></extra>",
        ),
        secondary_y=True,
    )

    # Layout
    if not title:
        title = os.path.basename(csv_path)

    fig.update_layout(
        title=title,
        xaxis_title=x_col,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        margin=dict(l=60, r=60, t=60, b=60),
        hovermode="x unified",
        template="plotly_white",
        height=520,
    )

    fig.update_yaxes(title_text=f"Latency ({latency_unit})", secondary_y=False)
    fig.update_yaxes(title_text="Ops / sec", secondary_y=True)

    if save_html:
        fig.write_html(save_html, include_plotlyjs="cdn")
        print(f"Saved interactive HTML to: {save_html}")

    if show:
        fig.show()

    return fig

# plot_bench_csv("bench_set.csv", x_col="db_size", smooth_window=3, latency_unit="us", save_html="bench_set.html")
# plot_bench_csv("bench_get.csv", x_col="db_size", smooth_window=3, latency_unit="us", save_html="bench_set.html")
# plot_bench_csv("bench_del.csv", x_col="db_size", smooth_window=3, latency_unit="us", save_html="bench_set.html")

