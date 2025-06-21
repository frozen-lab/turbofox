mod hash;
mod shard;

pub(crate) const WIDTH: usize = 512;
pub(crate) const ROWS: usize = 64;
pub(crate) type Res<T> = std::io::Result<T>;
