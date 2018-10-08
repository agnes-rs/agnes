/*!
Statistical functions for `agnes` data structures.
*/

use std::ops::{Add, Mul};

use num_traits::AsPrimitive;

use access::{DataIterator, DataIndex};
use field::Value;
use data_types::Func;
use data_types::{DataType, DTypeList};
use num_traits;

pub struct NumNaFn;
impl<DTypes, T> Func<DTypes, T> for NumNaFn
    where DTypes: DTypeList,
          T: DataType<DTypes>
{
    type Output = usize;

    fn call(
        &mut self,
        data: &dyn DataIndex<DTypes, DType=T>,
    )
        -> usize
    {
        DataIterator::new(data)
            .fold(0usize, |count, value| if value.exists() { count } else { count + 1 })
    }
}

pub struct NumExistsFn;
impl<DTypes, T> Func<DTypes, T> for NumExistsFn
    where DTypes: DTypeList,
          T: DataType<DTypes>
{
    type Output = usize;

    fn call(
        &mut self,
        data: &dyn DataIndex<DTypes, DType=T>,
    )
        -> usize
    {
        DataIterator::new(data)
            .fold(0usize, |count, value| if value.exists() { count + 1 } else { count })
    }
}

pub trait NaCount<DTypes, T> {
    fn num_na(&self) -> usize;
    fn num_exists(&self) -> usize;
}
impl<DTypes, T, U> NaCount<DTypes, T> for U
    where DTypes: DTypeList,
          T: DataType<DTypes>,
          U: DataIndex<DTypes, DType=T>,
          NumExistsFn: Func<DTypes, T, Output=usize>,
          NumNaFn: Func<DTypes, T, Output=usize>
{
    fn num_na(&self) -> usize {
        NumNaFn.call(self)
    }
    fn num_exists(&self) -> usize {
        NumExistsFn.call(self)
    }
}

pub trait CanSum: for<'a> Add<&'a Self, Output=Self> + num_traits::Zero {}
impl CanSum for u64 {}
impl CanSum for u32 {}
impl CanSum for i64 {}
impl CanSum for i32 {}
impl CanSum for f64 {}
impl CanSum for f32 {}

pub struct SumFn;
impl<DTypes, T> Func<DTypes, T> for SumFn
    where DTypes: DTypeList,
          T: DataType<DTypes> + CanSum
{
    type Output = T;

    fn call(
        &mut self,
        data: &dyn DataIndex<DTypes, DType=T>,
    )
        -> T
    {
        DataIterator::new(data)
            .fold(T::zero(), |sum, value| {
                match value {
                    Value::Exists(value) => sum + value,
                    Value::Na => sum
                }
            })
    }
}
impl<DTypes> Func<DTypes, bool> for SumFn
    where DTypes: DTypeList,
          bool: DataType<DTypes>
    {
    type Output = u64;

    fn call(
        &mut self,
        data: &dyn DataIndex<DTypes, DType=bool>,
    )
        -> u64
    {
        DataIterator::new(data)
            .fold(0u64, |sum, value| {
                match value {
                    Value::Exists(value) => sum + if *value { 1 } else { 0 },
                    Value::Na => sum
                }
            })
    }
}

pub trait Sum<DTypes, T, Output> {
    fn sum(&self) -> Output;
}

impl<DTypes, T, U, Output> Sum<DTypes, T, Output> for U
    where DTypes: DTypeList,
          T: DataType<DTypes>,
          U: DataIndex<DTypes, DType=T>,
          SumFn: Func<DTypes, T, Output=Output>
{
    fn sum(&self) -> Output {
        SumFn.call(self)
    }
}

pub trait CanMean: AsPrimitive<f64> {}
impl CanMean for u64 {}
impl CanMean for u32 {}
impl CanMean for i64 {}
impl CanMean for i32 {}
impl CanMean for f64 {}
impl CanMean for f32 {}

pub struct MeanFn;
impl<DTypes, T: DataType<DTypes>> Func<DTypes, T> for MeanFn
    where DTypes: DTypeList,
          T: DataType<DTypes> + CanSum + CanMean,
{
    type Output = f64;

    fn call(
        &mut self,
        data: &dyn DataIndex<DTypes, DType=T>,
    )
        -> f64
    {
        let nexists = match NumExistsFn.call(data) {
            0 => { return 0.0; },
            val => val as f64,
        };
        SumFn.call(data).as_() / nexists
    }
}
impl<DTypes> Func<DTypes, bool> for MeanFn
    where DTypes: DTypeList,
          bool: DataType<DTypes>
{
    type Output = f64;

    fn call(
        &mut self,
        data: &dyn DataIndex<DTypes, DType=bool>,
    )
        -> f64
    {
        let nexists = match NumExistsFn.call(data) {
            0 => { return 0.0; },
            val => val as f64,
        };
        let sum: f64 = SumFn.call(data).as_();
        sum / nexists
    }
}

pub trait Mean<DTypes, T> {
    fn mean(&self) -> f64;
}

impl<DTypes, T, U> Mean<DTypes, T> for U
    where DTypes: DTypeList,
          T: DataType<DTypes>,
          MeanFn: Func<DTypes, T, Output=f64>,
          U: DataIndex<DTypes, DType=T>
{
    fn mean(&self) -> f64 {
        MeanFn.call(self)
    }
}

pub trait CanSumSq: CanSum + Clone + for<'a> Mul<&'a Self, Output=Self> {}
impl CanSumSq for u64 {}
impl CanSumSq for u32 {}
impl CanSumSq for i64 {}
impl CanSumSq for i32 {}
impl CanSumSq for f64 {}
impl CanSumSq for f32 {}


pub struct SumSqFn;
impl<DTypes, T: DataType<DTypes>> Func<DTypes, T> for SumSqFn
    where DTypes: DTypeList,
          T: DataType<DTypes> + CanSumSq,
{
    type Output = T;

    fn call(
        &mut self,
        data: &dyn DataIndex<DTypes, DType=T>,
    )
        -> T
    {
        DataIterator::new(data)
            .fold(T::zero(), |sum, value| {
                match value {
                    Value::Exists(value) => sum + value.clone() * value,
                    Value::Na => sum
                }
            })
    }
}
impl<DTypes> Func<DTypes, bool> for SumSqFn
    where DTypes: DTypeList,
          bool: DataType<DTypes>
{
    type Output = u64;

    fn call(
        &mut self,
        data: &dyn DataIndex<DTypes, DType=bool>,
    )
        -> u64
    {
        DataIterator::new(data)
            .fold(0u64, |sum, value| {
                match value {
                    Value::Exists(value) => sum + if *value { 1 } else { 0 },
                    Value::Na => sum
                }
            })
    }
}

pub trait SumSq<DTypes, T, Output> {
    fn sum_sq(&self) -> Output;
}

impl<DTypes, T, Output, U> SumSq<DTypes, T, Output> for U
    where DTypes: DTypeList,
          T: DataType<DTypes>,
          SumSqFn: Func<DTypes, T, Output=Output>,
          U: DataIndex<DTypes, DType=T>
{
    fn sum_sq(&self) -> Output {
        SumSqFn.call(self)
    }
}

pub struct VarFn;
impl<DTypes, T> Func<DTypes, T> for VarFn
    where DTypes: DTypeList,
          T: DataType<DTypes>,
          SumSqFn: Func<DTypes, T>,
          <SumSqFn as Func<DTypes, T>>::Output: AsPrimitive<f64>,
          MeanFn: Func<DTypes, T>,
          <MeanFn as Func<DTypes, T>>::Output: AsPrimitive<f64>
{
    type Output = f64;

    fn call(
        &mut self,
        data: &dyn DataIndex<DTypes, DType=T>,
    )
        -> f64
    {
        let nexists = match NumExistsFn.call(data) {
            0 => { return 0.0; },
            val => val as f64,
        };
        let sum_sq = SumSqFn.call(data);
        let mean = MeanFn.call(data).as_();
        sum_sq.as_() / (nexists - 1.0) - nexists / (nexists - 1.0) * mean * mean
    }
}

pub struct VarPFn;
impl<DTypes, T: DataType<DTypes>> Func<DTypes, T> for VarPFn
    where DTypes: DTypeList,
          T: DataType<DTypes>,
          SumSqFn: Func<DTypes, T>,
          <SumSqFn as Func<DTypes, T>>::Output: AsPrimitive<f64>,
          MeanFn: Func<DTypes, T>,
          <MeanFn as Func<DTypes, T>>::Output: AsPrimitive<f64>
{
    type Output = f64;

    fn call(
        &mut self,
        data: &dyn DataIndex<DTypes, DType=T>,
    )
        -> f64
    {
        let nexists = match NumExistsFn.call(data) {
            0 => { return 0.0; },
            val => val as f64,
        };
        let sum_sq = SumSqFn.call(data);
        let mean = MeanFn.call(data).as_();
        sum_sq.as_() / nexists - mean * mean
    }
}

pub struct StdevFn;
impl<DTypes, T> Func<DTypes, T> for StdevFn
    where DTypes: DTypeList,
          T: DataType<DTypes>,
          VarFn: Func<DTypes, T>,
          <VarFn as Func<DTypes, T>>::Output: AsPrimitive<f64>,
{
    type Output = f64;

    fn call(
        &mut self,
        data: &dyn DataIndex<DTypes, DType=T>,
    )
        -> f64
    {
        VarFn.call(data).as_().sqrt()
    }
}

pub struct StdevPFn;
impl<DTypes, T> Func<DTypes, T> for StdevPFn
    where DTypes: DTypeList,
          T: DataType<DTypes>,
          VarPFn: Func<DTypes, T>,
          <VarPFn as Func<DTypes, T>>::Output: AsPrimitive<f64>,
{
    type Output = f64;

    fn call(
        &mut self,
        data: &dyn DataIndex<DTypes, DType=T>,
    )
        -> f64
    {
        VarPFn.call(data).as_().sqrt()
    }
}

pub trait Variance<DTypes, T> {
    fn var(&self) -> f64;
    fn varp(&self) -> f64;
    fn stdev(&self) -> f64 {
        self.var().sqrt()
    }
    fn stdevp(&self) -> f64 {
        self.varp().sqrt()
    }
}

impl<DTypes, T, U> Variance<DTypes, T> for U
    where DTypes: DTypeList,
          T: DataType<DTypes>,
          VarFn: Func<DTypes, T, Output=f64>,
          VarPFn: Func<DTypes, T, Output=f64>,
          U: DataIndex<DTypes, DType=T>
{
    fn var(&self) -> f64 {
        VarFn.call(self)
    }
    fn varp(&self) -> f64 {
        VarPFn.call(self)
    }
}

pub trait CanMinMax: PartialOrd + num_traits::Bounded {}
impl CanMinMax for u64 {}
impl CanMinMax for u32 {}
impl CanMinMax for i64 {}
impl CanMinMax for i32 {}
impl CanMinMax for f64 {}
impl CanMinMax for f32 {}

pub struct MinFn;
impl<DTypes, T> Func<DTypes, T> for MinFn
    where DTypes: DTypeList,
          T: DataType<DTypes> + CanMinMax + Clone,
{
    type Output = T;
    fn call(
        &mut self,
        data: &dyn DataIndex<DTypes, DType=T>,
    )
        -> T
    {
        DataIterator::new(data)
            .fold(T::max_value(), |cur_min, value| {
                match value {
                    Value::Exists(value) =>
                        if value < &cur_min { value.clone() } else { cur_min },
                    Value::Na => cur_min
                }
            })
    }
}
impl<DTypes> Func<DTypes, bool> for MinFn
    where DTypes: DTypeList,
          bool: DataType<DTypes>
{
    type Output = bool;
    fn call(
        &mut self,
        data: &dyn DataIndex<DTypes, DType=bool>,
    )
        -> bool
    {
        for value in DataIterator::new(data) {
            if let Value::Exists(&value) = value { if !value { return false; } }
        }
        true
    }
}
impl<DTypes> Func<DTypes, String> for MinFn
    where DTypes: DTypeList,
          String: DataType<DTypes>
{
    type Output = u64;
    fn call(
        &mut self,
        data: &dyn DataIndex<DTypes, DType=String>,
    )
        -> u64
    {
        DataIterator::new(data)
            .fold(u64::max_value(), |cur_min, value| {
                match value {
                    Value::Exists(value) => {
                        let len = value.len() as u64;
                        if len < cur_min { len } else { cur_min }
                    },
                    Value::Na => cur_min
                }
            })
    }
}


pub struct MaxFn;
impl<DTypes, T> Func<DTypes, T> for MaxFn
    where DTypes: DTypeList,
          T: DataType<DTypes> + CanMinMax + Clone,
{
    type Output = T;
    fn call(
        &mut self,
        data: &dyn DataIndex<DTypes, DType=T>,
    )
        -> T
    {
        DataIterator::new(data)
            .fold(T::min_value(), |cur_max, value| {
                match value {
                    Value::Exists(value) =>
                        if value > &cur_max { value.clone() } else { cur_max },
                    Value::Na => cur_max,
                }
            })
    }
}
impl<DTypes> Func<DTypes, bool> for MaxFn
    where DTypes: DTypeList,
          bool: DataType<DTypes>
{
    type Output = bool;
    fn call(
        &mut self,
        data: &dyn DataIndex<DTypes, DType=bool>,
    )
        -> bool
    {
        for value in DataIterator::new(data) {
            if let Value::Exists(&value) = value { if value { return true; } }
        }
        false
    }
}
impl<DTypes> Func<DTypes, String> for MaxFn
    where DTypes: DTypeList,
          String: DataType<DTypes>
{
    type Output = u64;
    fn call(
        &mut self,
        data: &dyn DataIndex<DTypes, DType=String>,
    )
        -> u64
    {
        DataIterator::new(data)
            .fold(u64::min_value(), |cur_max, value| {
                match value {
                    Value::Exists(value) => {
                        let len = value.len() as u64;
                        if len > cur_max { len } else { cur_max }
                    },
                    Value::Na => cur_max,
                }
            })
    }
}

pub trait MinMax<DTypes, T, Output> {
    fn min(&self) -> Output;
    fn max(&self) -> Output;
}

impl<DTypes, T, Output, U> MinMax<DTypes, T, Output> for U
    where DTypes: DTypeList,
          T: DataType<DTypes>,
          MinFn: Func<DTypes, T, Output=Output>,
          MaxFn: Func<DTypes, T, Output=Output>,
          U: DataIndex<DTypes, DType=T>
{
    fn min(&self) -> Output {
        MinFn.call(self)
    }
    fn max(&self) -> Output {
        MaxFn.call(self)
    }
}

#[cfg(test)]
mod tests {
    use store::{DataStore,};
    use field::Value;
    use select::Field;
    // use field::DtValue;
    use super::*;
    use data_types::standard::*;

    #[test]
    fn num_na() {
        let dv: DataView = DataStore::empty().with_data_vec::<f64, _, _>("Foo", vec![
            Value::Exists(0.0),
            Value::Exists(-5.0),
            Value::Na,
            Value::Na,
            Value::Exists(-3.0)
        ]).unwrap().into();
        assert_eq!(dv.map("Foo", NumNaFn).unwrap(), 2);
        assert_eq!(dv.map("Foo", NumExistsFn).unwrap(), 3);
    }

    #[test]
    fn sum() {
        let dv: DataView = DataStore::empty().with_data_vec::<u64, _, _>("Foo", vec![
            Value::Exists(0u64),
            Value::Exists(5),
            Value::Na,
            Value::Na,
            Value::Exists(3)
        ]).unwrap().into();
        assert_eq!(dv.field::<u64, _>("Foo").unwrap().sum(), 8);

        let dv: DataView = DataStore::empty().with_data_vec::<i32, _, _>("Foo", vec![
            Value::Exists(0),
            Value::Exists(-5),
            Value::Na,
            Value::Na,
            Value::Exists(-3)
        ]).unwrap().into();
        assert_eq!(dv.field::<i32, _>("Foo").unwrap().sum(), -8);

        let dv: DataView = DataStore::empty().with_data_vec::<bool, _, _>("Foo", vec![
            Value::Exists(true),
            Value::Exists(true),
            Value::Exists(false),
            Value::Na,
            Value::Exists(true)
        ]).unwrap().into();
        assert_eq!(dv.field::<bool, _>("Foo").unwrap().sum(), 3);

        let dv: DataView = DataStore::empty().with_data_vec::<f64, _, _>("Foo", vec![
            Value::Exists(0.0),
            Value::Exists(-5.0),
            Value::Na,
            Value::Na,
            Value::Exists(-3.0)
        ]).unwrap().into();
        assert_eq!(dv.field::<f64, _>("Foo").unwrap().sum(), -8.0);
    }

    #[test]
    fn mean() {
        let dv: DataView = DataStore::empty().with_data_vec::<u32, _, _>("Foo", vec![
            Value::Exists(0u32),
            Value::Exists(9),
            Value::Na,
            Value::Na,
            Value::Exists(3)
        ]).unwrap().into();
        assert_eq!(dv.field::<u32, _>("Foo").unwrap().mean(), 4.0);

        let dv: DataView = DataStore::empty().with_data_vec::<i64, _, _>("Foo", vec![
            Value::Exists(0i64),
            Value::Exists(-9),
            Value::Na,
            Value::Na,
            Value::Exists(-3)
        ]).unwrap().into();
        assert_eq!(dv.field::<i64, _>("Foo").unwrap().mean(), -4.0);

        let dv: DataView = DataStore::empty().with_data_vec::<bool, _, _>("Foo", vec![
            Value::Exists(true),
            Value::Exists(true),
            Value::Exists(false),
            Value::Na,
            Value::Exists(true)
        ]).unwrap().into();
        assert_eq!(dv.field::<bool, _>("Foo").unwrap().mean(), 0.75);

        let dv: DataView = DataStore::empty().with_data_vec::<f64, _, _>("Foo", vec![
            Value::Exists(0.0),
            Value::Exists(-9.0),
            Value::Na,
            Value::Na,
            Value::Exists(-3.0)
        ]).unwrap().into();
        assert_eq!(dv.field::<f64, _>("Foo").unwrap().mean(), -4.0);
    }

    #[test]
    fn min() {
        let dv: DataView = DataStore::empty().with_data_vec::<u32, _, _>("Foo", vec![
            Value::Exists(0u32),
            Value::Exists(9),
            Value::Na,
            Value::Na,
            Value::Exists(3)
        ]).unwrap().into();
        assert_eq!(dv.field::<u32, _>("Foo").unwrap().min(), 0);

        let dv: DataView = DataStore::empty().with_data_vec::<i32, _, _>("Foo", vec![
            Value::Exists(0i32),
            Value::Exists(-9),
            Value::Na,
            Value::Na,
            Value::Exists(-3)
        ]).unwrap().into();
        assert_eq!(dv.field::<i32, _>("Foo").unwrap().min(), -9);

        let dv: DataView = DataStore::empty().with_data_vec::<bool, _, _>("Foo", vec![
            Value::Exists(true),
            Value::Exists(true),
            Value::Exists(false),
            Value::Na,
            Value::Exists(true)
        ]).unwrap().into();
        assert_eq!(dv.field::<bool, _>("Foo").unwrap().min(), false);

        let dv: DataView = DataStore::empty().with_data_vec::<f64, _, _>("Foo", vec![
            Value::Exists(0.0),
            Value::Exists(-9.0),
            Value::Na,
            Value::Na,
            Value::Exists(-3.0)
        ]).unwrap().into();
        assert_eq!(dv.field::<f64, _>("Foo").unwrap().min(), -9.0);
    }

    #[test]
    fn max() {
        let dv: DataView = DataStore::empty().with_data_vec::<u32, _, _>("Foo", vec![
            Value::Exists(0u32),
            Value::Exists(9),
            Value::Na,
            Value::Na,
            Value::Exists(3)
        ]).unwrap().into();
        assert_eq!(dv.field::<u32, _>("Foo").unwrap().max(), 9);

        let dv: DataView = DataStore::empty().with_data_vec::<i64, _, _>("Foo", vec![
            Value::Exists(0i64),
            Value::Exists(-9),
            Value::Na,
            Value::Na,
            Value::Exists(-3)
        ]).unwrap().into();
        assert_eq!(dv.field::<i64, _>("Foo").unwrap().max(), 0);

        let dv: DataView = DataStore::empty().with_data_vec::<bool, _, _>("Foo", vec![
            Value::Exists(true),
            Value::Exists(true),
            Value::Exists(false),
            Value::Na,
            Value::Exists(true)
        ]).unwrap().into();
        assert_eq!(dv.field::<bool, _>("Foo").unwrap().max(), true);

        let dv: DataView = DataStore::empty().with_data_vec::<f64, _, _>("Foo", vec![
            Value::Exists(0.0),
            Value::Exists(-9.0),
            Value::Na,
            Value::Na,
            Value::Exists(-3.0)
        ]).unwrap().into();
        assert_eq!(dv.field::<f64, _>("Foo").unwrap().max(), 0.0);
    }

    #[test]
    fn stdev() {
        let dv: DataView = DataStore::empty().with_data_vec::<f64, _, _>("Foo", vec![
            Value::Exists(-5.0),
            Value::Exists(-4.0),
            Value::Na,
            Value::Exists(12.0),
            Value::Exists(3.0),
            Value::Na,
            Value::Exists(6.0),
            Value::Exists(0.0),
            Value::Exists(-3.1)
        ]).unwrap().into();
        assert!((dv.field::<f64, _>("Foo").unwrap().var() - 38.049048).abs() < 1e-6);
        assert!((dv.field::<f64, _>("Foo").unwrap().stdev() - 6.168391).abs() < 1e-6);
        assert!((dv.field::<f64, _>("Foo").unwrap().varp() - 32.613469).abs() < 1e-6);
        assert!((dv.field::<f64, _>("Foo").unwrap().stdevp() - 5.710820).abs() < 1e-6);
        assert!((dv.field::<f64, _>("Foo").unwrap().mean() - 1.271429).abs() < 1e-6);
        assert_eq!(dv.field::<f64, _>("Foo").unwrap().sum(), 8.9);
    }
}
