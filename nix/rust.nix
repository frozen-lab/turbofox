with (import <nixpkgs> { });

pkgs.mkShell {
  buildInputs = with pkgs; [
    gcc
    rustc
    cargo
    rustfmt
    rust-analyzer
    clippy
  ];
}
