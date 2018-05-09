use std::marker::PhantomData;
use masked::{MaybeNa};
use apply::MapFn;
use field::DataType;

/// `MapFn` containing a predicate that acts upon a specific data type `T`. Will panic if used with
/// data different from expected.
pub struct SingleTypeFn<T, U: DataType, F: FnMut(&T) -> U> {
    f: F,
    arg_ty: PhantomData<T>,
    result_ty: PhantomData<U>
}
impl<T, U: DataType, F: FnMut(&T) -> U> SingleTypeFn<T, U, F> {
    /// Create a new `MapFn` for a specific type using the provided function.
    pub fn new(f: F) -> SingleTypeFn<T, U, F> {
        SingleTypeFn {
            f,
            arg_ty: PhantomData,
            result_ty: PhantomData
        }
    }
}
macro_rules! impl_value_map {
    ($name:tt, $ty:ty) => {
        fn $name(&mut self, value: MaybeNa<&$ty>) -> Self::Output { value.map(&mut self.f) }
    }
}
macro_rules! impl_unreachable {
    ($name:tt, $ty:ty) => {
        fn $name(&mut self, _: MaybeNa<&$ty>) -> Self::Output { unreachable![] }
    }
}
impl<U: DataType, F: FnMut(&u64) -> U> MapFn for SingleTypeFn<u64, U, F> {
    type Output = MaybeNa<U>;
    impl_value_map!(apply_unsigned, u64);
    impl_unreachable!(apply_signed, i64);
    impl_unreachable!(apply_text, String);
    impl_unreachable!(apply_boolean, bool);
    impl_unreachable!(apply_float, f64);
}
impl<U: DataType, F: FnMut(&i64) -> U> MapFn for SingleTypeFn<i64, U, F> {
    type Output = MaybeNa<U>;
    impl_unreachable!(apply_unsigned, u64);
    impl_value_map!(apply_signed, i64);
    impl_unreachable!(apply_text, String);
    impl_unreachable!(apply_boolean, bool);
    impl_unreachable!(apply_float, f64);
}
impl<U: DataType, F: FnMut(&String) -> U> MapFn for SingleTypeFn<String, U, F> {
    type Output = MaybeNa<U>;
    impl_unreachable!(apply_unsigned, u64);
    impl_unreachable!(apply_signed, i64);
    impl_value_map!(apply_text, String);
    impl_unreachable!(apply_boolean, bool);
    impl_unreachable!(apply_float, f64);
}
impl<U: DataType, F: FnMut(&bool) -> U> MapFn for SingleTypeFn<bool, U, F> {
    type Output = MaybeNa<U>;
    impl_unreachable!(apply_unsigned, u64);
    impl_unreachable!(apply_signed, i64);
    impl_unreachable!(apply_text, String);
    impl_value_map!(apply_boolean, bool);
    impl_unreachable!(apply_float, f64);
}
impl<U: DataType, F: FnMut(&f64) -> U> MapFn for SingleTypeFn<f64, U, F> {
    type Output = MaybeNa<U>;
    impl_unreachable!(apply_unsigned, u64);
    impl_unreachable!(apply_signed, i64);
    impl_unreachable!(apply_text, String);
    impl_unreachable!(apply_boolean, bool);
    impl_value_map!(apply_float, f64);
}
