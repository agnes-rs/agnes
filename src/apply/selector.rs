use field::FieldIdent;

/// Data selector for the `ApplyToElem` and `ApplyToField` methods.
pub trait Selector: Clone {
    /// The type of the selector (the information used to specify what the `FieldFn` or `ElemFn`
    /// operates upon).
    type IndexType;
    /// Returns the field / element selector details.
    fn index(&self) -> Self::IndexType;
}
/// A data selector unsing only a data index. Used to select a specific element among a
/// single column / field / vector for use with an `ElemFn`.
#[derive(Debug, Clone)]
pub struct IndexSelector(pub usize);
impl Selector for IndexSelector {
    type IndexType = usize;
    fn index(&self) -> usize { self.0 }
}
/// A data selector using both a data field identifier and the data index. Used to select a
/// specific element in a two-dimensional data structs (with both fields and elements) along with
/// a `FieldFn`.
#[derive(Debug, Clone)]
pub struct FieldIndexSelector<'a>(pub &'a FieldIdent, pub usize);
impl<'a> Selector for FieldIndexSelector<'a> {
    type IndexType = (&'a FieldIdent, usize);
    fn index(&self) -> (&'a FieldIdent, usize) { (self.0, self.1) }
}
/// A data selector using only a field identifier. Used to select a specific field to be passed to
/// `FieldFn`.
#[derive(Debug, Clone)]
pub struct FieldSelector<'a>(pub &'a FieldIdent);
impl<'a> Selector for FieldSelector<'a> {
    type IndexType = (&'a FieldIdent);
    fn index(&self) -> (&'a FieldIdent) { (self.0) }
}
/// A data selector with no data. Used to select an entire field with `FieldFn` when a data
/// structure only has a single field's data.
#[derive(Debug, Clone)]
pub struct NilSelector;
impl Selector for NilSelector {
    type IndexType = ();
    fn index(&self) -> () {}
}
