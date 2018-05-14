use masked::MaybeNa;
use apply::{DataIndex, FieldMapFn, FieldApplyTo};
use field::{DtZero, FieldType, DtValue, FieldIdent};
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

/// Helper trait / function for computing the number of non-NA values in a data structure.
pub trait NumExists {
    /// Compute the number of non-NA values in the specified field of this data structure.
    fn num_exists(&self, ident: &FieldIdent) -> Result<usize>;
}
impl<T> NumExists for T where T: FieldApplyTo {
    fn num_exists(&self, ident: &FieldIdent) -> Result<usize> {
        self.field_apply_to(&mut NumExistsFn {}, ident)
    }
}

field_map_fn![
    NumExistsFn { type Output = usize; }
    fn all(self, field) {
        (0..field.len()).fold(0, |acc, idx| {
            acc + if field.get_data(idx).unwrap().exists() { 1 } else { 0 }
        })
    }
];

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
        Ok((0..field.len()).fold(0u64, |acc, idx| {
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

/// Helper trait for computing the arithmetic mean of values in a data structure.
pub trait Mean {
    /// Compute the arithmetic mean of values of the specified field of this data structure.
    fn mean(&self, ident: &FieldIdent) -> Result<f64>;
}
impl<T> Mean for T where T: FieldApplyTo {
    fn mean(&self, ident: &FieldIdent) -> Result<f64> {
        let nexists = match self.num_exists(ident)? {
            0 => { return Ok(0.0); },
            val => val as f64,
        };
        let sum = match self.sum(ident)? {
            DtValue::Unsigned(u) => u as f64,
            DtValue::Signed(s)   => s as f64,
            DtValue::Float(f)    => f,
            _                    => unreachable![] // sum only returns signed, unsigned, float
        };
        Ok(sum / nexists)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use view::DataView;
    use store::DataStore;
    use masked::{MaskedData, MaybeNa};

    #[test]
    fn sum() {
        let dv: DataView = DataStore::with_data(
            vec![("Foo", MaskedData::from_masked_vec(vec![
                MaybeNa::Exists(0),
                MaybeNa::Exists(5),
                MaybeNa::Na,
                MaybeNa::Na,
                MaybeNa::Exists(3)
            ]))], None, None, None, None,
        ).into();
        assert_eq!(dv.sum(&"Foo".into()).unwrap(), DtValue::Unsigned(8));

        let dv: DataView = DataStore::with_data(
            None, vec![("Foo", MaskedData::from_masked_vec(vec![
                MaybeNa::Exists(0),
                MaybeNa::Exists(-5),
                MaybeNa::Na,
                MaybeNa::Na,
                MaybeNa::Exists(-3)
            ]))], None, None, None
        ).into();
        assert_eq!(dv.sum(&"Foo".into()).unwrap(), DtValue::Signed(-8));

        let dv: DataView = DataStore::with_data(
            None, None, None, vec![("Foo", MaskedData::from_masked_vec(vec![
                MaybeNa::Exists(true),
                MaybeNa::Exists(true),
                MaybeNa::Exists(false),
                MaybeNa::Na,
                MaybeNa::Exists(true)
            ]))], None
        ).into();
        assert_eq!(dv.sum(&"Foo".into()).unwrap(), DtValue::Unsigned(3));

        let dv: DataView = DataStore::with_data(
            None, None, None, None, vec![("Foo", MaskedData::from_masked_vec(vec![
                MaybeNa::Exists(0.0),
                MaybeNa::Exists(-5.0),
                MaybeNa::Na,
                MaybeNa::Na,
                MaybeNa::Exists(-3.0)
            ]))]
        ).into();
        assert_eq!(dv.sum(&"Foo".into()).unwrap(), DtValue::Float(-8.0));
    }

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
        assert_eq!(dv.num_na(&"Foo".into()).unwrap(), 2);
        assert_eq!(dv.num_exists(&"Foo".into()).unwrap(), 3);
    }

    #[test]
    fn mean() {
        let dv: DataView = DataStore::with_data(
            vec![("Foo", MaskedData::from_masked_vec(vec![
                MaybeNa::Exists(0),
                MaybeNa::Exists(9),
                MaybeNa::Na,
                MaybeNa::Na,
                MaybeNa::Exists(3)
            ]))], None, None, None, None,
        ).into();
        assert_eq!(dv.mean(&"Foo".into()).unwrap(), 4.0);

        let dv: DataView = DataStore::with_data(
            None, vec![("Foo", MaskedData::from_masked_vec(vec![
                MaybeNa::Exists(0),
                MaybeNa::Exists(-9),
                MaybeNa::Na,
                MaybeNa::Na,
                MaybeNa::Exists(-3)
            ]))], None, None, None
        ).into();
        assert_eq!(dv.mean(&"Foo".into()).unwrap(), -4.0);

        let dv: DataView = DataStore::with_data(
            None, None, None, vec![("Foo", MaskedData::from_masked_vec(vec![
                MaybeNa::Exists(true),
                MaybeNa::Exists(true),
                MaybeNa::Exists(false),
                MaybeNa::Na,
                MaybeNa::Exists(true)
            ]))], None
        ).into();
        assert_eq!(dv.mean(&"Foo".into()).unwrap(), 0.75);

        let dv: DataView = DataStore::with_data(
            None, None, None, None, vec![("Foo", MaskedData::from_masked_vec(vec![
                MaybeNa::Exists(0.0),
                MaybeNa::Exists(-9.0),
                MaybeNa::Na,
                MaybeNa::Na,
                MaybeNa::Exists(-3.0)
            ]))]
        ).into();
        assert_eq!(dv.mean(&"Foo".into()).unwrap(), -4.0);
    }
}
