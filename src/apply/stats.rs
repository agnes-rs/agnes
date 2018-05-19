use masked::MaybeNa;
use apply::{DataIndex, FieldMapFn, FieldApplyTo};
use field::{FieldType, DtValue, FieldIdent};
use error::*;

/// Trait for computing the number of NA values in a data structure.
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

/// Trait for computing the number of non-NA values in a data structure.
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

/// Trait to produce values to indicate the 'limits' of an Agnes data type: the zero value, the
/// minimum value, and the maximum value.
pub trait DtLimits {
    /// The type of the limit values for this Agnes data type.
    type Output;
    /// Provide the 'zero' value for this Agnes data type.
    fn dt_zero() -> Self::Output;
    /// Provide the maximum value for this Agnes data type.
    fn dt_max() -> Self::Output;
    /// Provide the minimum value for this Agnes data type.
    fn dt_min() -> Self::Output;
}
impl DtLimits for u64 {
    type Output = u64;
    fn dt_zero() -> u64 { 0 }
    fn dt_max() -> u64 { ::std::u64::MAX }
    fn dt_min() -> u64 { ::std::u64::MIN }
}
impl DtLimits for i64 {
    type Output = i64;
    fn dt_zero() -> i64 { 0 }
    fn dt_max() -> i64 { ::std::i64::MAX }
    fn dt_min() -> i64 { ::std::i64::MIN }
}
impl DtLimits for f64 {
    type Output = f64;
    fn dt_zero() -> f64 { 0.0 }
    fn dt_max() -> f64 { ::std::f64::INFINITY }
    fn dt_min() -> f64 { ::std::f64::NEG_INFINITY }
}
impl<'a, T> DtLimits for &'a T where T: DtLimits {
    type Output = <T as DtLimits>::Output;
    fn dt_zero() -> Self::Output { T::dt_zero() }
    fn dt_max() -> Self::Output { T::dt_max() }
    fn dt_min() -> Self::Output { T::dt_min() }
}


/// Trait for computing the sum of values in a data structure.
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

/// Trait for computing the arithmetic mean of values in a data structure.
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

/// Trait for computing the sample standard deviation of a field of a data structure.
pub trait StDev {
    /// Compute the sample standard deviation of specified field in this data structure.
    fn stdev(&self, ident: &FieldIdent) -> Result<f64>;
}
impl<T> StDev for T where T: FieldApplyTo {
    fn stdev(&self, ident: &FieldIdent) -> Result<f64> {
        self.var(&ident).map(|var| var.sqrt())
    }
}

/// Trait for computing the sample variance of a field of a data structure.
pub trait Var {
    /// Compute the sample variance of specified field in this data structure.
    fn var(&self, ident: &FieldIdent) -> Result<f64>;
}
impl<T> Var for T where T: FieldApplyTo {
    fn var(&self, ident: &FieldIdent) -> Result<f64> {
        let nexists = match self.num_exists(ident)? {
            0 => { return Ok(0.0); },
            val => val as f64,
        };
        let sum_sq = self.field_apply_to(&mut SumSqFn {}, ident)??;
        let mean = self.mean(ident)?;
        Ok(sum_sq / (nexists - 1.0) - nexists / (nexists - 1.0) * mean * mean)
    }
}

/// Trait for computing the population standard deviation of a field of a data structure.
pub trait StDevP {
    /// Compute the population standard deviation of specified field in this data structure.
    fn stdevp(&self, ident: &FieldIdent) -> Result<f64>;
}
impl<T> StDevP for T where T: FieldApplyTo {
    fn stdevp(&self, ident: &FieldIdent) -> Result<f64> {
        self.varp(ident).map(|var| var.sqrt())
    }
}

/// Trait for computing the population variance of a field of a data structure.
pub trait VarP {
    /// Compute the population variance of specified field in this data structure.
    fn varp(&self, ident: &FieldIdent) -> Result<f64>;
}
impl<T> VarP for T where T: FieldApplyTo {
    fn varp(&self, ident: &FieldIdent) -> Result<f64> {
        let nexists = match self.num_exists(ident)? {
            0 => { return Ok(0.0); },
            val => val as f64,
        };
        let sum_sq = self.field_apply_to(&mut SumSqFn {}, ident)??;
        let mean = self.mean(ident)?;
        Ok(sum_sq / nexists - mean * mean)
    }
}

field_map_fn![
    SumSqFn { type Output = Result<f64>; }
    fn [signed, unsigned, float](self, field) {
        Ok((0..field.len()).fold(0.0, |acc, idx| {
            match field.get_data(idx).unwrap() {
                MaybeNa::Exists(value) => acc + (value * value) as f64,
                MaybeNa::Na => acc
            }
        }).into())
    }
    fn boolean(self, field) {
        Ok((0..field.len()).fold(0.0, |acc, idx| {
            match field.get_data(idx).unwrap() {
                MaybeNa::Exists(value) => acc + if *value { 1.0 } else { 0.0 },
                MaybeNa::Na => acc
            }
        }).into())
    }
    fn text(self, field) {
        return Err(AgnesError::InvalidType(FieldType::Text, "sum".into()))
    }
];

/// Trait for computing the minimum values in a field of a data structure.
pub trait Min {
    /// Compute the minimum values of the specified field of this data structure.
    fn min(&self, ident: &FieldIdent) -> Result<DtValue>;
}
impl<T> Min for T where T: FieldApplyTo {
    fn min(&self, ident: &FieldIdent) -> Result<DtValue> {
        self.field_apply_to(&mut MinFn {}, ident)?
    }
}
field_map_fn![
    MinFn { type Output = Result<DtValue>; }
    fn [signed, unsigned, float](self, field) {
        Ok((0..field.len()).fold(DType::dt_max(), |acc, idx| {
            match field.get_data(idx).unwrap() {
                MaybeNa::Exists(value) => if *value < acc { *value } else { acc },
                MaybeNa::Na => acc
            }
        }).into())
    }
    fn boolean(self, field) {
        for idx in 0..field.len() {
            match field.get_data(idx).unwrap() {
                MaybeNa::Exists(value) => if !*value { return Ok(DtValue::Boolean(false)); },
                _ => {}
            }
        }
        Ok(DtValue::Boolean(true))
    }
    fn text(self, field) {
        Ok(((0..field.len()).fold(::std::usize::MAX, |acc, idx| {
            match field.get_data(idx).unwrap() {
                MaybeNa::Exists(value) => if value.len() < acc { value.len() } else { acc },
                MaybeNa::Na => acc
            }
        }) as u64).into())
    }
];

/// Trait for computing the maximum values in a field of a data structure.
pub trait Max {
    /// Compute the maximum values of the specified field of this data structure.
    fn max(&self, ident: &FieldIdent) -> Result<DtValue>;
}
impl<T> Max for T where T: FieldApplyTo {
    fn max(&self, ident: &FieldIdent) -> Result<DtValue> {
        self.field_apply_to(&mut MaxFn {}, ident)?
    }
}
field_map_fn![
    MaxFn { type Output = Result<DtValue>; }
    fn [signed, unsigned, float](self, field) {
        Ok((0..field.len()).fold(DType::dt_min(), |acc, idx| {
            match field.get_data(idx).unwrap() {
                MaybeNa::Exists(value) => if *value > acc { *value } else { acc },
                MaybeNa::Na => acc
            }
        }).into())
    }
    fn boolean(self, field) {
        for idx in 0..field.len() {
            match field.get_data(idx).unwrap() {
                MaybeNa::Exists(value) => if *value { return Ok(DtValue::Boolean(true)); },
                _ => {}
            }
        }
        Ok(DtValue::Boolean(false))
    }
    fn text(self, field) {
        Ok(((0..field.len()).fold(0, |acc, idx| {
            match field.get_data(idx).unwrap() {
                MaybeNa::Exists(value) => if value.len() > acc { value.len() } else { acc },
                MaybeNa::Na => acc
            }
        }) as u64).into())
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

    #[test]
    fn min() {
        let dv: DataView = DataStore::with_data(
            vec![("Foo", MaskedData::from_masked_vec(vec![
                MaybeNa::Exists(0),
                MaybeNa::Exists(9),
                MaybeNa::Na,
                MaybeNa::Na,
                MaybeNa::Exists(3)
            ]))], None, None, None, None,
        ).into();
        assert_eq!(dv.min(&"Foo".into()).unwrap(), DtValue::Unsigned(0));

        let dv: DataView = DataStore::with_data(
            None, vec![("Foo", MaskedData::from_masked_vec(vec![
                MaybeNa::Exists(0),
                MaybeNa::Exists(-9),
                MaybeNa::Na,
                MaybeNa::Na,
                MaybeNa::Exists(-3)
            ]))], None, None, None
        ).into();
        assert_eq!(dv.min(&"Foo".into()).unwrap(), DtValue::Signed(-9));

        let dv: DataView = DataStore::with_data(
            None, None, None, vec![("Foo", MaskedData::from_masked_vec(vec![
                MaybeNa::Exists(true),
                MaybeNa::Exists(true),
                MaybeNa::Exists(false),
                MaybeNa::Na,
                MaybeNa::Exists(true)
            ]))], None
        ).into();
        assert_eq!(dv.min(&"Foo".into()).unwrap(), DtValue::Boolean(false));

        let dv: DataView = DataStore::with_data(
            None, None, None, None, vec![("Foo", MaskedData::from_masked_vec(vec![
                MaybeNa::Exists(0.0),
                MaybeNa::Exists(-9.0),
                MaybeNa::Na,
                MaybeNa::Na,
                MaybeNa::Exists(-3.0)
            ]))]
        ).into();
        assert_eq!(dv.min(&"Foo".into()).unwrap(), DtValue::Float(-9.0));
    }

    #[test]
    fn max() {
        let dv: DataView = DataStore::with_data(
            vec![("Foo", MaskedData::from_masked_vec(vec![
                MaybeNa::Exists(0),
                MaybeNa::Exists(9),
                MaybeNa::Na,
                MaybeNa::Na,
                MaybeNa::Exists(3)
            ]))], None, None, None, None,
        ).into();
        assert_eq!(dv.max(&"Foo".into()).unwrap(), DtValue::Unsigned(9));

        let dv: DataView = DataStore::with_data(
            None, vec![("Foo", MaskedData::from_masked_vec(vec![
                MaybeNa::Exists(0),
                MaybeNa::Exists(-9),
                MaybeNa::Na,
                MaybeNa::Na,
                MaybeNa::Exists(-3)
            ]))], None, None, None
        ).into();
        assert_eq!(dv.max(&"Foo".into()).unwrap(), DtValue::Signed(0));

        let dv: DataView = DataStore::with_data(
            None, None, None, vec![("Foo", MaskedData::from_masked_vec(vec![
                MaybeNa::Exists(true),
                MaybeNa::Exists(true),
                MaybeNa::Exists(false),
                MaybeNa::Na,
                MaybeNa::Exists(true)
            ]))], None
        ).into();
        assert_eq!(dv.max(&"Foo".into()).unwrap(), DtValue::Boolean(true));

        let dv: DataView = DataStore::with_data(
            None, None, None, None, vec![("Foo", MaskedData::from_masked_vec(vec![
                MaybeNa::Exists(0.0),
                MaybeNa::Exists(-9.0),
                MaybeNa::Na,
                MaybeNa::Na,
                MaybeNa::Exists(-3.0)
            ]))]
        ).into();
        assert_eq!(dv.max(&"Foo".into()).unwrap(), DtValue::Float(0.0));
    }

    #[test]
    fn stdev() {
        let dv: DataView = DataStore::with_data(
            None, None, None, None,
            vec![("Foo", MaskedData::from_masked_vec(vec![
                MaybeNa::Exists(-5.0),
                MaybeNa::Exists(-4.0),
                MaybeNa::Na,
                MaybeNa::Exists(12.0),
                MaybeNa::Exists(3.0),
                MaybeNa::Na,
                MaybeNa::Exists(6.0),
                MaybeNa::Exists(0.0),
                MaybeNa::Exists(-3.1)
            ]))]
        ).into();
        assert!((dv.var(&"Foo".into()).unwrap() - 38.049048).abs() < 1e-6);
        assert!((dv.stdev(&"Foo".into()).unwrap() - 6.168391).abs() < 1e-6);
        assert!((dv.varp(&"Foo".into()).unwrap() - 32.613469).abs() < 1e-6);
        assert!((dv.stdevp(&"Foo".into()).unwrap() - 5.710820).abs() < 1e-6);
        assert!((dv.mean(&"Foo".into()).unwrap() - 1.271429).abs() < 1e-6);
        assert_eq!(dv.sum(&"Foo".into()).unwrap(), DtValue::Float(8.9));
    }
}
