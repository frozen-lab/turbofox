//! TurboFox is a persistent and efficient embedded KV database

#![deny(missing_docs)]
#![deny(unused_must_use)]
#![allow(unsafe_op_in_unsafe_fn)]

mod index;

/// TurboFox is a persistent and efficient embedded KV database
#[derive(Debug)]
pub struct TurboFox {
    _index: index::Index,
}
