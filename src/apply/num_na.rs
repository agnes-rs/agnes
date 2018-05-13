use apply::{DataIndex, FieldMapFn, FieldApplyTo};
use field::FieldIdent;
use error::*;

/// Helper trait / function for computing the number of NA values in a data structure.
pub trait NumNa {
    /// Compute the number of NA values in the specified field of this data structure.
    fn num_na(&self, ident: &FieldIdent) -> Result<usize>;
}
impl<T> NumNa for T where T: FieldApplyTo {
    fn num_na(&self, ident: &FieldIdent) -> Result<usize> {
        self.field_apply_to(&mut NumNaFn {}, ident)
    }
}

field_map_fn![
    NumNaFn { type Output = usize; }
    fn all(self, field) {
        (0..field.len()).fold(0, |acc, idx| {
            acc + if field.get_data(idx).unwrap().is_na() { 1 } else { 0 }
        })
    }
];

#[cfg(test)]
mod tests {
    use super::*;
    use view::DataView;
    use store::DataStore;
    use masked::{MaskedData, MaybeNa};

    #[test]
    fn num_na() {
        let dv: DataView = DataStore::with_data(
            None, None, None, None, vec![("Foo", MaskedData::from_masked_vec(vec![
                MaybeNa::Exists(0.0),
                MaybeNa::Exists(-5.0),
                MaybeNa::Na,
                MaybeNa::Na,
                MaybeNa::Exists(-3.0)
            ]))]
        ).into();
        println!("{}", dv);
        assert_eq!(dv.num_na(&"Foo".into()).unwrap(), 2);
    }
}
