/*!
A set of functions to be applied to `agnes` data structures.

Applying functions to agnes data structures can be done in two ways: the `MapFn` and `FieldMapFn`
traits provided in the `mapfn` submodule (soon to be deprecated), or the iterator-based method
using `DataIterator` in the `access` module.
*/

// #[macro_use] pub mod mapfn;

pub mod matches;
// pub use self::matches::*;

pub mod sort;
// pub use self::sort_order::*;

// mod add_to_ds;
// pub use self::add_to_ds::*;

// mod single_type;
// pub use self::single_type::*;

pub mod stats;

pub mod convert;
// pub use self::convert::*;

// mod unique;
// pub use self::unique::*;
