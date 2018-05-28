/*!
Dataframe library for Rust. Provides data structs and utilities for data aggregation, manipulation,
and viewing.
*/

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
#[macro_use] extern crate prettytable;
extern crate csv_sniffer;
extern crate tempfile;

pub mod source;
#[macro_use] pub mod store;
pub mod field;
pub mod error;
pub mod view;
pub use view::{SerializeAsVec, DataView};
pub mod join;
pub use join::{Join, JoinKind};
pub mod frame;
pub use frame::Filter;
pub mod masked;
pub use masked::MaybeNa;
pub mod apply;
pub mod ops;
mod view_stats;
pub use view_stats::ViewStats;

#[cfg(test)] pub(crate) mod test_utils;
