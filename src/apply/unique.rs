use std::collections::HashSet;

use masked::MaybeNa;
use field::FieldIdent;
use apply::mapfn::*;
use access::{FieldData, DataIndex};
use apply::Select;
use apply::sort_order::SortOrderFn;
use view::{DataView, IntoFieldList};
use error::*;

field_map_fn![
    /// `FieldMapFn` function struct for retrieving the indices of unique values in the data
    /// structure (i.e. a set of indices that will each point to a unique value in the data
    /// structure, with the set as a whole representing all possible values within the data).
    pub UniqueFn {
        type Output = Vec<usize>;
    }
    fn [unsigned, signed, boolean, text](self, field) {
        let mut set = HashSet::new();
        let mut indices = vec![];
        for i in 0..field.len() {
            let datum = field.get_data(i).unwrap();
            if !set.contains(&datum) {
                set.insert(datum);
                indices.push(i);
            }
        }
        indices
    }
    fn float(self, field) {
        let sorted = SortOrderFn {}.apply_float(field);
        let mut prev_value: Option<MaybeNa<&f64>> = None;
        let mut indices = vec![];
        for i in sorted {
            let datum = field.get_data(i).unwrap();
            if let Some(ref prev) = prev_value {
                if &datum == prev { continue; }
            }
            indices.push(i);
            prev_value = Some(datum);
        }
        indices
    }
];

/// Trait to retrieve a new `DataView` with a single field that contains the set of unique values
/// within the specified field.
pub trait Unique {
    /// Compute the unique values within the specified field, and return a `DataView` containing
    /// those values.
    fn unique<T: Into<FieldIdent>>(&self, ident: T) -> Result<DataView>;
}
impl Unique for DataView {
    fn unique<T: Into<FieldIdent>>(&self, ident: T) -> Result<DataView> {
        let ident = ident.into();
        let permutation = self.field_apply_to(&mut UniqueFn {}, &ident)?;
        let mut subview = self.v(ident);
        debug_assert_eq!(subview.frames.len(), 1);
        subview.frames[0].update_permutation(&permutation);
        Ok(subview)
    }
}

/// `FieldReduceFn` struct for computing unique values in a data structure when considering multiple
/// fields. Returns a set of indices that will each point to a unique value in the data structure,
/// with the set as a whole representing all possible combinations of values from these fields
/// within the data.
pub struct CompositeUniqueFn {}
impl<'a> FieldReduceFn<'a> for CompositeUniqueFn {
    type Output = Vec<usize>;
    fn reduce(&mut self, fields: Vec<FieldData<'a>>) -> Vec<usize> {
        let mut set = HashSet::new();
        let mut indices = vec![];
        if fields.len() == 0 {
            return indices;
        }
        for i in 0..fields[0].len() {
            let data = fields.iter().map(|rdi| rdi.get_datum(i)).collect::<Vec<_>>();
            if !set.contains(&data) {
                set.insert(data);
                indices.push(i);
            }
        }
        indices

    }
}

/// Trait to produce a new `DataView` which contains the unique combinations of values of the fields
/// specified.
pub trait CompositeUnique {
    /// Compute all combinations of values in the specified fields that exists in this `DataView`,
    /// and return a new `DataView` with those values.
    fn composite_unique<L: IntoFieldList>(&self, fields: L) -> Result<DataView>;
}
impl CompositeUnique for DataView {
    fn composite_unique<L: IntoFieldList>(&self, fields: L) -> Result<DataView> {
        let fields = fields.into_field_list();
        let permutation = fields
            .iter().map(|ident| self.select_one(ident)).collect::<Vec<_>>()
            .apply_field_reduce(&mut CompositeUniqueFn {})?;
        let mut subview = self.v(fields.clone());
        // debug_assert_eq!(subview.frames.len(), fields.len());
        for frame in &mut subview.frames {
            frame.update_permutation(&permutation);
        }
        Ok(subview)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use view::DataView;
    use store::DataStore;
    use masked::{MaskedData, MaybeNa};
    use test_utils::*;

    #[test]
    fn unique() {
        let dv: DataView = DataStore::with_data(
            vec![("Foo", MaskedData::from_masked_vec(vec![
                MaybeNa::Exists(0),
                MaybeNa::Exists(5),
                MaybeNa::Exists(5),
                MaybeNa::Exists(0),
                MaybeNa::Exists(3)
            ]))], None, None, None, None,
        ).into();
        let dv_unique = dv.unique("Foo").unwrap();
        unsigned::assert_dv_eq_vec(&dv_unique, &"Foo".into(),
            vec![0u64, 5, 3]
        );
    }
}
