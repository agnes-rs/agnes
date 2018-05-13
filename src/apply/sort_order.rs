use std::cmp::Ordering;

use error::Result;
use field::FieldIdent;
use apply::{FieldMapFn, FieldApply, FieldApplyTo, DataIndex};

/// Helper trait retrieving the sort permutation for a field.
pub trait SortOrderBy {
    /// Returns the sort permutation for the field specified.
    fn sort_order_by(&self, ident: &FieldIdent) -> Result<Vec<usize>>;
}
impl<T> SortOrderBy for T where T: FieldApplyTo {
    fn sort_order_by(&self, ident: &FieldIdent) -> Result<Vec<usize>> {
        self.field_apply_to(
            &mut SortOrderFn {},
            ident
        )
    }
}
/// Helper trait retrieving the sort permutation for a structure.
pub trait SortOrder {
    /// Returns the sort permutation for the data in thie data structure.
    fn sort_order(&self) -> Result<Vec<usize>>;
}
impl<T> SortOrder for T where T: FieldApply {
    fn sort_order(&self) -> Result<Vec<usize>> {
        self.field_apply(&mut SortOrderFn {})
    }
}

field_map_fn![
    /// `FieldFn` function struct for retrieving the sort permutation order for a field.
    SortOrderFn { type Output = Vec<usize>; }
    fn [unsigned, signed, text, boolean](self, field) {
        // ordering is (arbitrarily) going to be:
        // NA values, followed by everything else ascending
        let mut order = (0..field.len()).collect::<Vec<_>>();
        order.sort_unstable_by(|&a, &b| {
            // a, b are always in range, so unwraps are safe
            field.get_data(a).unwrap().cmp(&field.get_data(b).unwrap())
        });
        order
    }
    fn float(self, field) {
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
];
