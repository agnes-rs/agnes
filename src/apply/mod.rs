/*!
Framework for providing and applying functions to data within the `agnes` data structures in a
consistent, type-coherent manner.

The `MapFn` trait provides a framework for functions that apply to a single element in the data
structure.

The `FieldMapFn` trait provides a framework for functions that apply to a field (column) of data in
the data structure.
*/

mod select;
pub use self::select::*;

mod map;
pub use self::map::*;

mod matches;
pub use self::matches::*;

mod sort_order;
pub use self::sort_order::*;
