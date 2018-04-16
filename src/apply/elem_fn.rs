use apply::Selector;
use masked::MaybeNa;
use error::Result;

/// Trait implemented by data structures which wish to be able to support `ElemFn`s (type-dependent
/// functions that apply to a specific element).
pub trait ApplyToElem<S: Selector> {
    /// Apply an `ElemFn` to an element selected with the provided `Selector`.
    fn apply_to_elem<T: ElemFn>(&self, f: T, select: S) -> Result<T::Output>;
}

/// Trait for a type-dependent function that applies to a specific element.
pub trait ElemFn {
    /// The desired output of this function.
    type Output;
    /// The method to use when working with unsigned (`u64`) data.
    fn apply_unsigned(&mut self, value: MaybeNa<&u64>) -> Self::Output;
    /// The method to use when working with signed (`i64`) data.
    fn apply_signed(&mut self, value: MaybeNa<&i64>) -> Self::Output;
    /// The method to use when working with text (`String`) data.
    fn apply_text(&mut self, value: MaybeNa<&String>) -> Self::Output;
    /// The method to use when working with boolean (`bool`) data.
    fn apply_boolean(&mut self, value: MaybeNa<&bool>) -> Self::Output;
    /// The method to use when working with floating-point (`f64`) data.
    fn apply_float(&mut self, value: MaybeNa<&f64>) -> Self::Output;
}