/*!
Data management library for Rust. It provides data structs and utilities for data loading,
preprocessing, aggregation, manipulation, viewing, and serialization.

For a more complete description of `agnes` along with a feature list, usage information, example
code, and more, see the respository [README](https://github.com/jblondin/agnes). For a guide on how
to get started with `agnes`, click [here](https://wee.codes/agnes/guide.html).

## Primary Structures

`agnes` is designed to work with labeled heterogeneously-typed tabular data -- a group of fields
(columns) each with a label and (possibly-different) data type, where each field has the same number
of rows.

Labels in `agnes` are [unit-like](
https://doc.rust-lang.org/book/ch05-01-defining-structs.html#unit-like-structs-without-any-fields)
marker structs which only exist to uniquely identify, at the type level (i.e. compile time), a
field. The [tablespace](label/macro.tablespace.html) macro exists to define these labels.

The primary data storage structure in `agnes` is the [DataStore](store/struct.DataStore.html), which
is a list of [FieldData](field/struct.FieldData.html) objects which each contain the data for a
single field. The `DataStore` is intended to be the single point of storage for data loaded into a
program; furthermore, once data is added to a `DataStore` it is immutable.

The primary data structure used by the end user of this library is the
[DataView](view/struct.DataView.html), which references one or more
[DataFrame](frame/struct.DataFrame.html) objects, each of which holds a reference and provides
access to a single `DataStore`. The `DataView` struct can be considered to be a method of selecting
fields (columns) across one or more data sources, with the `DataFrame` struct used to select the
specific rows from those data sources (after, for example, a filtering or join operation).

The [FieldSelect](select/trait.FieldSelect.html) and
[SelectFieldByLabel](select/trait.SelectFieldByLabel.html) traits provide methods to select a single
field from a `DataView` to operate upon. They return a type that implements
[DataIndex](access/trait.DataIndex.html), which provides accessor methods to the data of that field
(an index-based method [get_datum](access/trait.DataIndex.html#method.get_datum) and an iterator
provided by [iter](access/trait.DataIndex.html#method.iter)).

## Design

`agnes` makes extensive use of heterogeneous [cons-lists](https://en.wikipedia.org/wiki/Cons#Lists)
to provide data structures that can hold data of varying types (as long as the types are known to
the user of the library at compile time). Much of this framework was originally inspired by the
[frunk](https://github.com/lloydmeta/frunk) Rust library and the
[HList](http://hackage.haskell.org/package/HList) Haskell library.

In the `DataStore` struct, a cons-list is used to hold a list of the the `FieldData` objects (each
type-parameterized on a potentially different type). The `DataView` struct has a cons-list of
labels referenced by that `DataView` along with another cons-list of `DataFrame`s for each data
source it references.

The basic cons-list implementation can be found in the [cons](cons/index.html) module. Additional
functionality for labeling cons-list elements and retrieving elements based on labels can be found
in the [labels](labels/index.html) module.

*/

#![warn(missing_docs)]
#![deny(bare_trait_objects, unconditional_recursion)]

extern crate bit_vec;
extern crate csv;
extern crate encoding;
extern crate futures;
extern crate hyper;
extern crate hyper_tls;
extern crate indexmap;
extern crate native_tls;
extern crate num_traits;
extern crate serde;
extern crate tokio_core;
extern crate tokio_io;
#[macro_use]
extern crate prettytable;
extern crate csv_sniffer;
extern crate tempfile;
// re-export typenum (since it's used in exported macros)
pub extern crate typenum;

#[cfg(test)]
extern crate rand;
#[cfg(test)]
extern crate serde_json;

#[macro_use]
pub mod cons;
#[macro_use]
pub mod partial;
#[macro_use]
pub mod label;
#[macro_use]
pub mod fieldlist;
#[macro_use]
pub mod store;
#[macro_use]
pub mod field;

#[cfg(feature = "test-utils")]
#[macro_use]
pub mod test_utils;

pub mod access;
pub mod error;
pub mod frame;
pub mod join;
#[cfg(feature = "ops")]
pub mod ops;
pub mod select;
pub mod source;
pub mod stats;
pub mod view;
pub mod view_stats;
// pub mod reshape;

#[cfg(feature = "experimental")]
pub mod experimental;

#[cfg(test)]
pub mod test_gen_data;
