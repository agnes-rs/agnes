/*!
Dataframe library for Rust. Provides data structs and utilities for data aggregation, manipulation,
and viewing.
*/

#![allow(missing_docs)]
#![deny(bare_trait_objects, unconditional_recursion)]
#![recursion_limit="256"]

extern crate serde;
extern crate num_traits;
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
#[macro_use] extern crate mashup;
extern crate typenum;

#[cfg(test)] extern crate rand;

#[macro_use] pub mod ops;
#[macro_use] pub mod cons;
// #[macro_use] pub mod data_types;
#[macro_use] pub mod features;
#[macro_use] pub mod label;
#[macro_use] pub mod fieldlist;
pub mod source;
pub mod store;
pub mod field;
pub mod error;
pub mod view;
// pub mod join;
pub mod frame;
pub mod access;
// pub mod apply;
pub mod select;
// pub mod view_stats;
// pub mod filter;
// pub mod reshape;
// #[macro_use] pub mod unique;

#[cfg(feature = "experimental")]
pub mod experimental;

#[cfg(test)] pub(crate) mod test_utils;
// #[cfg(test)] pub(crate) mod test_gen_data;
