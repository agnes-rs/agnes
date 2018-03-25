//! Dataframe library for Rust. Provides a `DataFrame` object for data views and manipulation.

#![warn(missing_docs)]

extern crate serde;
#[macro_use] extern crate serde_derive;
extern crate futures;
extern crate hyper;
extern crate tokio_core;
extern crate tokio_io;
extern crate native_tls;
extern crate hyper_tls;
extern crate csv;
extern crate encoding;
extern crate indexmap;
extern crate bit_vec;

pub mod source;
pub mod store;
pub mod field;
pub mod error;
pub mod view;
pub use view::{SerializeAsVec, DataView};
pub mod masked;
pub use masked::MaybeNa;
