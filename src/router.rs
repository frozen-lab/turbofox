#![allow(dead_code)]

use crate::shard::Shard;
use crate::Result;
use anyhow::ensure;
use std::{
    ops::Range,
    path::PathBuf,
    sync::{Arc, RwLock},
};

fn consolidate_ranges(mut ranges: Vec<Range<u32>>) -> (Vec<Range<u32>>, Vec<Range<u32>>) {
    ranges.sort_by(|a, b| {
        if a.start == b.start {
            b.end.cmp(&a.end)
        } else {
            a.start.cmp(&b.start)
        }
    });

    let mut removed = vec![];
    let mut i = 1;

    while i < ranges.len() {
        if ranges[i].start >= ranges[i - 1].start && ranges[i].end <= ranges[i - 1].end {
            removed.push(ranges.remove(i));
        } else {
            i += 1;
        }
    }

    (ranges, removed)
}

enum RouterNode {
    Leaf(Shard),
    Vertex(Arc<TurboRouter>, Arc<TurboRouter>),
}

impl RouterNode {
    fn span(&self) -> Range<u32> {
        match self {
            Self::Leaf(sh) => Range {
                start: sh.span.start,
                end: sh.span.end,
            },
            Self::Vertex(bottom, top) => bottom.span.start..top.span.end,
        }
    }
    fn len(&self) -> u32 {
        self.span().end - self.span().start
    }
}

pub(crate) struct TurboRouter {
    span: Range<u32>,
    node: RwLock<RouterNode>,
}

impl TurboRouter {
    pub const END_OF_SHARDS: u32 = 1u32 << 16;

    pub fn new(dirpath: PathBuf) -> Result<Self> {
        let mut shards = Self::load(&dirpath)?;

        if shards.is_empty() {
            shards = Self::create_initial_shards(&dirpath)?;
        }

        let root = Self::treeify(shards);

        Ok(Self {
            span: root.span(),
            node: RwLock::new(root),
        })
    }

    pub fn calc_num_shards(num_items: usize) -> u32 {
        Self::END_OF_SHARDS / Self::calc_step(num_items)
    }

    pub fn shared_op<T>(&self, shard_selector: u32, func: impl FnOnce(&Shard) -> Result<T>) -> Result<T> {
        match &*self.node.read().unwrap() {
            RouterNode::Leaf(sh) => func(sh),
            RouterNode::Vertex(bottom, top) => {
                if shard_selector < bottom.span.end {
                    bottom.shared_op(shard_selector, func)
                } else {
                    top.shared_op(shard_selector, func)
                }
            }
        }
    }

    pub fn clear(&self, dirpath: PathBuf) -> Result<()> {
        let mut guard = self.node.write().unwrap();

        for res in std::fs::read_dir(&dirpath)? {
            let entry = res?;
            let filename = entry.file_name();

            let Some(filename) = filename.to_str() else {
                continue;
            };

            let Ok(filetype) = entry.file_type() else {
                continue;
            };

            if !filetype.is_file() {
                continue;
            }

            if filename.starts_with("shard_")
                || filename.starts_with("compact_")
                || filename.starts_with("bottom_")
                || filename.starts_with("top_")
            {
                std::fs::remove_file(entry.path())?;
            }
        }

        let shards = Self::create_initial_shards(&dirpath)?;
        *guard = Self::treeify(shards);

        Ok(())
    }

    fn load(dirpath: &PathBuf) -> Result<Vec<Shard>> {
        let mut found_shards: Vec<Range<u32>> = vec![];

        for res in std::fs::read_dir(&dirpath)? {
            let entry = res?;
            let filename = entry.file_name();

            let Some(filename) = filename.to_str() else {
                continue;
            };

            let Ok(filetype) = entry.file_type() else {
                continue;
            };

            if !filetype.is_file() {
                continue;
            }

            if filename.starts_with("bottom_") || filename.starts_with("top_") || filename.starts_with("merge_") {
                std::fs::remove_file(entry.path())?;

                continue;
            } else if !filename.starts_with("shard_") {
                continue;
            }

            let Some((_, span)) = filename.split_once("_") else {
                continue;
            };

            let Some((start, end)) = span.split_once("-") else {
                continue;
            };

            let start = u32::from_str_radix(start, 16).expect(filename);
            let end = u32::from_str_radix(end, 16).expect(filename);

            ensure!(start < end && end <= Self::END_OF_SHARDS, "Bad span for {filename}");

            found_shards.push(start..end);
        }

        let (shards_to_keep, shards_to_remove) = consolidate_ranges(found_shards);

        for span in shards_to_remove {
            std::fs::remove_file(dirpath.join(format!("shard_{:04x}-{:04x}", span.start, span.end)))?;
        }

        let mut shards = vec![];

        for span in shards_to_keep {
            shards.push(Shard::open(&dirpath, span)?);
        }

        Ok(shards)
    }

    fn treeify(shards: Vec<Shard>) -> RouterNode {
        let mut nodes = vec![];
        let mut unit: u32 = Self::END_OF_SHARDS;

        {
            let mut spans_debug: Vec<Range<u32>> = vec![];

            for sh in shards {
                assert!(
                    spans_debug.is_empty() || spans_debug.last().unwrap().start != sh.span.start,
                    "two elements with the same start {spans_debug:?} {:?}",
                    sh.span
                );

                spans_debug.push(sh.span.clone());
                let n = RouterNode::Leaf(sh);

                if unit > n.len() {
                    unit = n.len();
                }

                nodes.push(n);
            }

            assert!(
                spans_debug.is_sorted_by(|a, b| a.start < b.start),
                "not sorted {spans_debug:?}"
            );
            assert!(unit >= 1 && unit.is_power_of_two(), "unit={unit}");
            assert!(nodes.len() > 0, "No shards to merge");
            assert!(nodes.len() > 1 || unit == Self::END_OF_SHARDS);
        }

        while unit < Self::END_OF_SHARDS {
            let mut i = 0;

            while i < nodes.len() - 1 {
                if nodes[i].len() == unit && nodes[i + 1].len() == unit {
                    let n0 = nodes.remove(i);
                    let n1 = nodes.remove(i);

                    nodes.insert(
                        i,
                        RouterNode::Vertex(Arc::new(Self::from_shardnode(n0)), Arc::new(Self::from_shardnode(n1))),
                    );
                } else {
                    i += 1;
                }
            }

            unit *= 2;
        }

        assert_eq!(nodes.len(), 1);

        nodes.remove(0)
    }

    fn from_shardnode(n: RouterNode) -> Self {
        Self {
            span: n.span(),
            node: RwLock::new(n),
        }
    }

    fn calc_step(num_items: usize) -> u32 {
        let step = (Self::END_OF_SHARDS as f64) / (num_items as f64 / Shard::EXPECTED_CAPACITY as f64).max(1.0);

        1 << (step as u32).ilog2()
    }

    fn create_initial_shards(dirpath: &PathBuf) -> Result<Vec<Shard>> {
        let step = Self::calc_step(0usize);

        let mut shards = vec![];
        let mut start = 0;

        while start < Self::END_OF_SHARDS {
            let end = start + step;

            shards.push(Shard::open(&dirpath, start..end)?);
            start = end;
        }

        Ok(shards)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_consolidate_ranges() {
        assert_eq!(consolidate_ranges(vec![0..16]), (vec![0..16], vec![]));
        assert_eq!(consolidate_ranges(vec![16..32, 0..16]), (vec![0..16, 16..32], vec![]));
        assert_eq!(
            consolidate_ranges(vec![16..32, 0..16, 0..32]),
            (vec![0..32], vec![0..16, 16..32])
        );
        assert_eq!(
            consolidate_ranges(vec![16..32, 0..16, 0..32, 48..64, 32..48, 50..60]),
            (vec![0..32, 32..48, 48..64], vec![0..16, 16..32, 50..60])
        );
    }
}
