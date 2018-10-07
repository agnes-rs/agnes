/*!
A set of functions to be applied to `agnes` data structures.

Applying functions to agnes data structures can be done in two ways: the `MapFn` and `FieldMapFn`
traits provided in the `mapfn` submodule (soon to be deprecated), or the iterator-based method
using `DataIterator` in the `access` module.
*/

pub mod sort;
pub mod stats;
pub mod convert;
