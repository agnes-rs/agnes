use apply::Selector;
use masked::MaybeNa;
use error::Result;

/// Trait implemented by data structures which wish to be able to support `FieldFn`s (type-dependent
/// functions that apply to an entire field).
pub trait ApplyToField<S: Selector> {
    /// Apply a `FieldFn` to a field selected with the provided `Selector`.
    fn apply_to_field<T: FieldFn>(&self, f: T, select: S) -> Result<T::Output>;
}
/// Trait implemented by pairs of data structures which wish to be abel to support `Field2Fn`s
/// (type-dependent functions that apply to fields from two data structures simultaneously).
pub trait ApplyToField2<S: Selector> {
    /// Apply a `Field2Fn` (a function that operates simultaneously on fields from two different
    /// sources) to fields selected with the provided `Selector`s.
    fn apply_to_field2<T: Field2Fn>(&self, f: T, select: (S, S)) -> Result<T::Output>;
}

/// Trait implemented by data structures that represent a single column / vector / field of data.
pub trait DataIndex<T: PartialOrd> {
    /// Returns the data (possibly NA) at the specified index, if it exists.
    fn get_data(&self, idx: usize) -> Result<MaybeNa<&T>>;
    /// Returns the length of this data field.
    fn len(&self) -> usize;
}
/// Trait for a type-dependent function that applies to a single field.
pub trait FieldFn {
    /// The desired output of this function.
    type Output;
    /// The method to use when working with unsigned (`u64`) data.
    fn apply_unsigned<T: DataIndex<u64>>(&mut self, field: &T) -> Self::Output;
    /// The method to use when working with signed (`i64`) data.
    fn apply_signed<T: DataIndex<i64>>(&mut self, field: &T) -> Self::Output;
    /// The method to use when working with text (`String`) data.
    fn apply_text<T: DataIndex<String>>(&mut self, field: &T) -> Self::Output;
    /// The method to use when working with boolean (`bool`) data.
    fn apply_boolean<T: DataIndex<bool>>(&mut self, field: &T) -> Self::Output;
    /// The method to use when working with floating-point (`f64`) data.
    fn apply_float<T: DataIndex<f64>>(&mut self, field: &T) -> Self::Output;
}
/// Trait for a type-dependent function that applies to a pair of fields.
pub trait Field2Fn {
    /// The desired output of this function.
    type Output;
    /// The method to use when working with unsigned (`u64`) data.
    fn apply_unsigned<T: DataIndex<u64>>(&mut self, field: &(&T, &T)) -> Self::Output;
    /// The method to use when working with signed (`i64`) data.
    fn apply_signed<T: DataIndex<i64>>(&mut self, field: &(&T, &T)) -> Self::Output;
    /// The method to use when working with text (`String`) data.
    fn apply_text<T: DataIndex<String>>(&mut self, field: &(&T, &T)) -> Self::Output;
    /// The method to use when working with boolean (`bool`) data.
    fn apply_boolean<T: DataIndex<bool>>(&mut self, field: &(&T, &T)) -> Self::Output;
    /// The method to use when working with floating-point (`f64`) data.
    fn apply_float<T: DataIndex<f64>>(&mut self, field: &(&T, &T)) -> Self::Output;
}
