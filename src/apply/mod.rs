/*!
Framework for providing and applying functions to data within the `agnes` data structures in a
consistent, type-coherent manner.

Applying functions to agnes data structures can be done in two ways: the `MapFn` and `FieldMapFn`
traits provided in the `mapfn` submodule (soon to be deprecated), or the iterator-based method
using `DataIterator` in the `access` module.
*/

mod select;
pub use self::select::{Select, Field, Selection, GetFieldData, SelectionList};

#[macro_use] pub mod mapfn;

mod matches;
pub use self::matches::*;

mod sort_order;
pub use self::sort_order::*;

mod add_to_ds;
pub use self::add_to_ds::*;

mod single_type;
pub use self::single_type::*;

pub mod stats;

mod convert;
pub use self::convert::*;

mod unique;
pub use self::unique::*;
