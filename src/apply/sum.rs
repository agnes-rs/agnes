use masked::MaybeNa;
use apply::{DataIndex, FieldMapFn, FieldApplyTo};
use field::{DtZero, FieldType, DtValue, FieldIdent};
use error::*;

/// Helper trait for computing the sum of values in a data structure.
pub trait Sum {
    /// Compute the sum values in the specified field of this data structure.
    fn sum(&self, ident: &FieldIdent) -> Result<DtValue>;
}
impl<T> Sum for T where T: FieldApplyTo {
    fn sum(&self, ident: &FieldIdent) -> Result<DtValue> {
        self.field_apply_to(&mut SumFn {}, ident)?
    }
}

field_map_fn![
    SumFn { type Output = Result<DtValue>; }
    fn [signed, unsigned, float](self, field) {
        Ok((0..field.len()).fold(DType::dt_zero(), |acc, idx| {
                match field.get_data(idx).unwrap() {
                    MaybeNa::Exists(value) => acc + value,
                    MaybeNa::Na => acc
                }
        }).into())
    }
    fn boolean(self, field) {
        Ok((0..field.len()).fold(DType::dt_zero(), |acc, idx| {
                match field.get_data(idx).unwrap() {
                    MaybeNa::Exists(value) => acc + if *value { 1 } else { 0 },
                    MaybeNa::Na => acc
                }
        }).into())
    }
    fn text(self, field) {
        return Err(AgnesError::InvalidType(FieldType::Text, "sum".into()))
    }
];

#[cfg(test)]
mod tests {
    use super::*;
    use view::DataView;
    use store::DataStore;
    use masked::{MaskedData, MaybeNa};

    #[test]
    fn sum() {
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
        assert_eq!(dv.sum(&"Foo".into()).unwrap(), DtValue::Float(-8.0));
    }
}
