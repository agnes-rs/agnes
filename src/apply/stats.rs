/*!
Statistical functions for `agnes` data structures.
*/

use std::ops::{Add, Mul};

use num_traits::ToPrimitive;

use apply::{Selection, GetFieldData};
use access::FieldData;
use field::{FieldType, DtValue};
use masked::MaybeNa;
use field::DataType;
use error::*;

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

impl<'a> FieldData<'a> {
    /// Compute the number of NA values in this field.
    pub fn num_na(&self) -> usize {
        fn count_na<T: DataType>(count: usize, value: MaybeNa<&T>) -> usize {
            if value.exists() { count } else { count + 1 }
        }
        match *self {
            FieldData::Unsigned(_) => self.data_iter::<u64>().fold(0usize, count_na),
            FieldData::Signed(_) => self.data_iter::<i64>().fold(0usize, count_na),
            FieldData::Text(_) => self.data_iter::<String>().fold(0usize, count_na),
            FieldData::Boolean(_) => self.data_iter::<bool>().fold(0usize, count_na),
            FieldData::Float(_) => self.data_iter::<f64>().fold(0usize, count_na),
        }
    }

    /// Compute the number of existing (non-NA) values in this field.
    pub fn num_exists(&self) -> usize {
        self.len() - self.num_na()
    }

    /// Compute the sum of values in this field.
    pub fn sum(&self) -> Result<DtValue> {
        fn numeric_sum<T: DataType + Add<Output=T> + Copy>(sum: T, value: MaybeNa<&T>) -> T {
            match value {
                MaybeNa::Exists(&ref value) => sum + *value,
                MaybeNa::Na => sum
            }
        }
        match *self {
            FieldData::Unsigned(_) =>
                Ok(DtValue::Unsigned(self.data_iter::<u64>().fold(u64::dt_zero(), numeric_sum))),
            FieldData::Signed(_) =>
                Ok(DtValue::Signed(self.data_iter::<i64>().fold(i64::dt_zero(), numeric_sum))),
            FieldData::Text(_) =>
                Err(AgnesError::InvalidType(FieldType::Text, "sum".into())),
            FieldData::Boolean(_) =>
                Ok(DtValue::Unsigned(self.data_iter::<bool>().fold(0u64, |sum, value| {
                    match value {
                        MaybeNa::Exists(value) => sum + if *value { 1 } else { 0 },
                        MaybeNa::Na => sum
                    }
                }))),
            FieldData::Float(_) =>
                Ok(DtValue::Float(self.data_iter::<f64>().fold(f64::dt_zero(), numeric_sum))),
        }
    }

    /// Compute the arithmetic mean of values in this field.
    pub fn mean(&self) -> Result<f64> {
        let nexists = match self.num_exists() {
            0 => { return Ok(0.0); },
            val => val as f64,
        };
        let sum = match self.sum()? {
            DtValue::Unsigned(u) => u as f64,
            DtValue::Signed(s)   => s as f64,
            DtValue::Float(f)    => f,
            _                    => unreachable![] // sum only returns signed, unsigned, float
        };
        Ok(sum / nexists)
    }

    /// Compute the sum of squares of values in this field.
    pub fn sum_sq(&self) -> Result<f64> {

        fn numeric_sum_sq<T: DataType + Mul<Output=T> + ToPrimitive + Copy>(
            sum: f64, value: MaybeNa<&T>) -> f64
        {
            match value {
                MaybeNa::Exists(&ref value) => sum + (*value * *value).to_f64().unwrap(),
                MaybeNa::Na => sum
            }
        }
        match *self {
            FieldData::Unsigned(_) =>
                Ok(self.data_iter::<u64>().fold(0.0, numeric_sum_sq)),
            FieldData::Signed(_) =>
                Ok(self.data_iter::<i64>().fold(0.0, numeric_sum_sq)),
            FieldData::Text(_) =>
                Err(AgnesError::InvalidType(FieldType::Text, "sum of squares".into())),
            FieldData::Boolean(_) =>
                Ok(self.data_iter::<bool>().fold(0.0, |sum, value| {
                    match value {
                        MaybeNa::Exists(value) => sum + if *value { 1.0 } else { 0.0 },
                        MaybeNa::Na => sum
                    }
                })),
            FieldData::Float(_) =>
                Ok(self.data_iter::<f64>().fold(0.0, numeric_sum_sq)),
        }
    }

    /// Compute the sample variance of the values in this field.
    pub fn var(&self) -> Result<f64> {
        let nexists = match self.num_exists() {
            0 => { return Ok(0.0); },
            val => val as f64,
        };
        let sum_sq = self.sum_sq()?;
        let mean = self.mean()?;
        Ok(sum_sq / (nexists - 1.0) - nexists / (nexists - 1.0) * mean * mean)
    }

    /// Compute the population variance of the values in this field.
    pub fn varp(&self) -> Result<f64> {
        let nexists = match self.num_exists() {
            0 => { return Ok(0.0); },
            val => val as f64,
        };
        let sum_sq = self.sum_sq()?;
        let mean = self.mean()?;
        Ok(sum_sq / nexists - mean * mean)
    }

    /// Compute the sample standard deviation of the values in this field.
    pub fn stdev(&self) -> Result<f64> {
        self.var().map(|var| var.sqrt())
    }

    /// Compute the population standard deviation of the values in this field.
    pub fn stdevp(&self) -> Result<f64> {
        self.varp().map(|var| var.sqrt())
    }

    /// Compute the minimum value of the values in this field.
    pub fn min(&self) -> DtValue {
        fn numeric_min<T: DataType + DtLimits + PartialOrd + Copy>(cur_min: T, value: MaybeNa<&T>)
            -> T
        {
            match value {
                MaybeNa::Exists(&ref value) => if *value < cur_min { *value } else { cur_min },
                MaybeNa::Na => cur_min
            }
        }
        match *self {
            FieldData::Unsigned(_) =>
                self.data_iter::<u64>().fold(u64::dt_max(), numeric_min).into(),
            FieldData::Signed(_) =>
                self.data_iter::<i64>().fold(i64::dt_max(), numeric_min).into(),
            FieldData::Text(_) =>
                (self.data_iter::<String>().fold(::std::usize::MAX, |cur_min, value| {
                    match value {
                        MaybeNa::Exists(&ref value) =>
                            if value.len() < cur_min { value.len() } else { cur_min },
                        MaybeNa::Na => cur_min
                    }
                }) as u64).into(),
            FieldData::Boolean(_) => {
                for value in self.data_iter::<bool>() {
                    match value {
                        MaybeNa::Exists(value) => if !*value {
                            return DtValue::Boolean(false);
                        },
                        _ => {}
                    }
                }
                DtValue::Boolean(true)
            },
            FieldData::Float(_) =>
                self.data_iter::<f64>().fold(f64::dt_max(), numeric_min).into(),
        }
    }

    /// Compute the maximum value of the values in this field.
    pub fn max(&self) -> DtValue {
        fn numeric_max<T: DataType + DtLimits + PartialOrd + Copy>(cur_max: T, value: MaybeNa<&T>)
            -> T
        {
            match value {
                MaybeNa::Exists(&ref value) => if *value > cur_max { *value } else { cur_max },
                MaybeNa::Na => cur_max
            }
        }
        match *self {
            FieldData::Unsigned(_) =>
                self.data_iter::<u64>().fold(u64::dt_min(), numeric_max).into(),
            FieldData::Signed(_) =>
                self.data_iter::<i64>().fold(i64::dt_min(), numeric_max).into(),
            FieldData::Text(_) =>
                (self.data_iter::<String>().fold(0, |cur_max, value| {
                    match value {
                        MaybeNa::Exists(&ref value) =>
                            if value.len() > cur_max { value.len() } else { cur_max },
                        MaybeNa::Na => cur_max
                    }
                }) as u64).into(),
            FieldData::Boolean(_) => {
                for value in self.data_iter::<bool>() {
                    match value {
                        MaybeNa::Exists(value) => if *value {
                            return DtValue::Boolean(true);
                        },
                        _ => {}
                    }
                }
                DtValue::Boolean(false)
            },
            FieldData::Float(_) =>
                self.data_iter::<f64>().fold(f64::dt_min(), numeric_max).into(),
        }
    }
}

impl<'a, D> Selection<'a, D> where Selection<'a, D>: GetFieldData<'a> {
    /// Compute the number of NA values in this field.
    pub fn num_na(&self) -> Result<usize> {
        self.get_field_data().map(|fd| fd.num_na())
    }

    /// Compute the number of existing (non-NA) values in this field.
    pub fn num_exists(&self) -> Result<usize> {
        self.get_field_data().map(|fd| fd.num_exists())
    }

    /// Compute the sum of values in this field.
    pub fn sum(&self) -> Result<DtValue> {
        self.get_field_data().and_then(|fd| fd.sum())
    }

    /// Compute the arithmetic mean of values in this field.
    pub fn mean(&self) -> Result<f64> {
        self.get_field_data().and_then(|fd| fd.mean())
    }

    /// Compute the sum of squares of values in this field.
    pub fn sum_sq(&self) -> Result<f64> {
        self.get_field_data().and_then(|fd| fd.sum_sq())
    }

    /// Compute the sample variance of the values in this field.
    pub fn var(&self) -> Result<f64> {
        self.get_field_data().and_then(|fd| fd.var())
    }

    /// Compute the population variance of the values in this field.
    pub fn varp(&self) -> Result<f64> {
        self.get_field_data().and_then(|fd| fd.varp())
    }

    /// Compute the sample standard deviation of the values in this field.
    pub fn stdev(&self) -> Result<f64> {
        self.get_field_data().and_then(|fd| fd.stdev())
    }

    /// Compute the population standard deviation of the values in this field.
    pub fn stdevp(&self) -> Result<f64> {
        self.get_field_data().and_then(|fd| fd.stdevp())
    }

    /// Compute the minimum value of the values in this field.
    pub fn min(&self) -> Result<DtValue> {
        self.get_field_data().map(|fd| fd.min())
    }

    /// Compute the maximum value of the values in this field.
    pub fn max(&self) -> Result<DtValue> {
        self.get_field_data().map(|fd| fd.max())
    }
}


#[cfg(test)]
mod tests {
    use view::DataView;
    use store::DataStore;
    use masked::{MaskedData, MaybeNa};
    use apply::Select;
    use field::DtValue;

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
        assert_eq!(dv.select_one("Foo").num_na().unwrap(), 2);
        assert_eq!(dv.select_one("Foo").num_exists().unwrap(), 3);
    }

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
        assert_eq!(dv.select_one("Foo").sum().unwrap(), DtValue::Unsigned(8));

        let dv: DataView = DataStore::with_data(
            None, vec![("Foo", MaskedData::from_masked_vec(vec![
                MaybeNa::Exists(0),
                MaybeNa::Exists(-5),
                MaybeNa::Na,
                MaybeNa::Na,
                MaybeNa::Exists(-3)
            ]))], None, None, None
        ).into();
        assert_eq!(dv.select_one("Foo").sum().unwrap(), DtValue::Signed(-8));

        let dv: DataView = DataStore::with_data(
            None, None, None, vec![("Foo", MaskedData::from_masked_vec(vec![
                MaybeNa::Exists(true),
                MaybeNa::Exists(true),
                MaybeNa::Exists(false),
                MaybeNa::Na,
                MaybeNa::Exists(true)
            ]))], None
        ).into();
        assert_eq!(dv.select_one("Foo").sum().unwrap(), DtValue::Unsigned(3));

        let dv: DataView = DataStore::with_data(
            None, None, None, None, vec![("Foo", MaskedData::from_masked_vec(vec![
                MaybeNa::Exists(0.0),
                MaybeNa::Exists(-5.0),
                MaybeNa::Na,
                MaybeNa::Na,
                MaybeNa::Exists(-3.0)
            ]))]
        ).into();
        assert_eq!(dv.select_one("Foo").sum().unwrap(), DtValue::Float(-8.0));
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
        assert_eq!(dv.select_one("Foo").mean().unwrap(), 4.0);

        let dv: DataView = DataStore::with_data(
            None, vec![("Foo", MaskedData::from_masked_vec(vec![
                MaybeNa::Exists(0),
                MaybeNa::Exists(-9),
                MaybeNa::Na,
                MaybeNa::Na,
                MaybeNa::Exists(-3)
            ]))], None, None, None
        ).into();
        assert_eq!(dv.select_one("Foo").mean().unwrap(), -4.0);

        let dv: DataView = DataStore::with_data(
            None, None, None, vec![("Foo", MaskedData::from_masked_vec(vec![
                MaybeNa::Exists(true),
                MaybeNa::Exists(true),
                MaybeNa::Exists(false),
                MaybeNa::Na,
                MaybeNa::Exists(true)
            ]))], None
        ).into();
        assert_eq!(dv.select_one("Foo").mean().unwrap(), 0.75);

        let dv: DataView = DataStore::with_data(
            None, None, None, None, vec![("Foo", MaskedData::from_masked_vec(vec![
                MaybeNa::Exists(0.0),
                MaybeNa::Exists(-9.0),
                MaybeNa::Na,
                MaybeNa::Na,
                MaybeNa::Exists(-3.0)
            ]))]
        ).into();
        assert_eq!(dv.select_one("Foo").mean().unwrap(), -4.0);
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
        assert_eq!(dv.select_one("Foo").min().unwrap(), DtValue::Unsigned(0));

        let dv: DataView = DataStore::with_data(
            None, vec![("Foo", MaskedData::from_masked_vec(vec![
                MaybeNa::Exists(0),
                MaybeNa::Exists(-9),
                MaybeNa::Na,
                MaybeNa::Na,
                MaybeNa::Exists(-3)
            ]))], None, None, None
        ).into();
        assert_eq!(dv.select_one("Foo").min().unwrap(), DtValue::Signed(-9));

        let dv: DataView = DataStore::with_data(
            None, None, None, vec![("Foo", MaskedData::from_masked_vec(vec![
                MaybeNa::Exists(true),
                MaybeNa::Exists(true),
                MaybeNa::Exists(false),
                MaybeNa::Na,
                MaybeNa::Exists(true)
            ]))], None
        ).into();
        assert_eq!(dv.select_one("Foo").min().unwrap(), DtValue::Boolean(false));

        let dv: DataView = DataStore::with_data(
            None, None, None, None, vec![("Foo", MaskedData::from_masked_vec(vec![
                MaybeNa::Exists(0.0),
                MaybeNa::Exists(-9.0),
                MaybeNa::Na,
                MaybeNa::Na,
                MaybeNa::Exists(-3.0)
            ]))]
        ).into();
        assert_eq!(dv.select_one("Foo").min().unwrap(), DtValue::Float(-9.0));
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
        assert_eq!(dv.select_one("Foo").max().unwrap(), DtValue::Unsigned(9));

        let dv: DataView = DataStore::with_data(
            None, vec![("Foo", MaskedData::from_masked_vec(vec![
                MaybeNa::Exists(0),
                MaybeNa::Exists(-9),
                MaybeNa::Na,
                MaybeNa::Na,
                MaybeNa::Exists(-3)
            ]))], None, None, None
        ).into();
        assert_eq!(dv.select_one("Foo").max().unwrap(), DtValue::Signed(0));

        let dv: DataView = DataStore::with_data(
            None, None, None, vec![("Foo", MaskedData::from_masked_vec(vec![
                MaybeNa::Exists(true),
                MaybeNa::Exists(true),
                MaybeNa::Exists(false),
                MaybeNa::Na,
                MaybeNa::Exists(true)
            ]))], None
        ).into();
        assert_eq!(dv.select_one("Foo").max().unwrap(), DtValue::Boolean(true));

        let dv: DataView = DataStore::with_data(
            None, None, None, None, vec![("Foo", MaskedData::from_masked_vec(vec![
                MaybeNa::Exists(0.0),
                MaybeNa::Exists(-9.0),
                MaybeNa::Na,
                MaybeNa::Na,
                MaybeNa::Exists(-3.0)
            ]))]
        ).into();
        assert_eq!(dv.select_one("Foo").max().unwrap(), DtValue::Float(0.0));
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
        assert!((dv.select_one("Foo").var().unwrap() - 38.049048).abs() < 1e-6);
        assert!((dv.select_one("Foo").stdev().unwrap() - 6.168391).abs() < 1e-6);
        assert!((dv.select_one("Foo").varp().unwrap() - 32.613469).abs() < 1e-6);
        assert!((dv.select_one("Foo").stdevp().unwrap() - 5.710820).abs() < 1e-6);
        assert!((dv.select_one("Foo").mean().unwrap() - 1.271429).abs() < 1e-6);
        assert_eq!(dv.select_one("Foo").sum().unwrap(), DtValue::Float(8.9));
    }
}
