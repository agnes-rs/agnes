use std::cmp::Ordering;
use apply::{Selector, ApplyToField, FieldFn, DataIndex};
use error::Result;

/// Helper trait / implementations retrieving the sort permutation for a field.
pub trait SortOrderBy<S: Selector> {
    /// Returns the sort permutation for the field specified with the `Selector.
    fn sort_order_by(&self, select: S) -> Result<Vec<usize>>;
}
impl<S: Selector, T> SortOrderBy<S> for T where T: ApplyToField<S> {
    fn sort_order_by(&self, select: S) -> Result<Vec<usize>> {
        self.apply_to_field(SortOrderFn {}, select)
    }
}

/// `FieldFn` function struct for retrieving the sort permutation order for a field.
pub struct SortOrderFn {}
macro_rules! impl_sort_order_fn {
    ($name:tt; $ty:ty) => {
        // ordering is (arbitrarily) going to be:
        // NA values, followed by everything else ascending
        fn $name<'a, T: DataIndex<$ty>>(&mut self, field: &T) -> Vec<usize> {
            let mut order = (0..field.len()).collect::<Vec<_>>();
            order.sort_unstable_by(|&a, &b| {
                // a, b are always in range, so unwraps are safe
                field.get_data(a).unwrap().cmp(&field.get_data(b).unwrap())
            });
            order
        }
    }
}
impl FieldFn for SortOrderFn {
    type Output = Vec<usize>;
    impl_sort_order_fn!(apply_unsigned; u64);
    impl_sort_order_fn!(apply_signed;   i64);
    impl_sort_order_fn!(apply_text;     String);
    impl_sort_order_fn!(apply_boolean;  bool);

    fn apply_float<'a, T: DataIndex<f64>>(&mut self, field: &T) -> Vec<usize> {
        let mut order = (0..field.len()).collect::<Vec<_>>();
        order.sort_unstable_by(|&a, &b| {
            // a, b are always in range, so unwraps are safe
            let (vala, valb) = (field.get_data(a).unwrap(), field.get_data(b).unwrap());
            vala.partial_cmp(&valb).unwrap_or_else(|| {
                // partial_cmp doesn't fail for MaybeNa::NA, unwraps safe
                let (vala, valb) = (vala.unwrap(), valb.unwrap());
                if vala.is_nan() && !valb.is_nan() {
                    Ordering::Less
                } else {
                    // since partial_cmp only fails for NAN, then !vala.is_nan() && valb.is_nan()
                    Ordering::Greater
                }
            })
        });
        order
    }
}