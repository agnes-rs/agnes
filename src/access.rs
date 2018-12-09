
/*!
Traits for accessing data within agnes data structures. Includes `DataIndex` for index-based access
and `DataIterator` for iterator access.
*/
use std::cmp::Ordering;
use std::fmt::Debug;
use std::marker::PhantomData;

use error::*;
use field::{Value};

/// Trait that provides access to values in a data field.
pub trait DataIndex: Debug
{
    /// The data type contained within this field.
    type DType;

    /// Returns the data (possibly NA) at the specified index, if it exists.
    fn get_datum(&self, idx: usize) -> Result<Value<&Self::DType>>;
    /// Returns the length of this data field.
    fn len(&self) -> usize;
    /// Returns whether or not this field is empty.
    fn is_empty(&self) -> bool { self.len() == 0 }
    /// Returns an iterator over the values in this field.
    fn iter(&self) -> DataIterator<Self::DType> where Self: Sized {
        DataIterator::new(self)
    }
    /// Copies existing values in this field into a new `Vec`.
    ///
    /// If this field has missing values, this method will return a vector of length less than that
    /// returned by the `len` method.
    fn to_vec(&self) -> Vec<Self::DType>
        where
            Self: Sized,
            Self::DType: Clone
    {
        self.iter().filter_map(|value| {
            match value {
                Value::Exists(value) => Some(value.clone()),
                Value::Na => None
            }
        }).collect()
    }
    /// Copies values (missing or existing) in this field into a new `Vec`.
    fn to_value_vec(&self) -> Vec<Value<Self::DType>>
        where
            Self: Sized,
            Self::DType: Clone
    {
        self.iter().map(|value| value.cloned()).collect()
    }
}
/// Trait that provides mutable access to values in a data field.
pub trait DataIndexMut: DataIndex
{
    /// Add a value to this field.
    fn push(&mut self, value: Value<Self::DType>);
}

/// Iterator over the data in a data structure that implement DataIndex.
pub struct DataIterator<'a, T>
    where T: 'a
{
    data: &'a dyn DataIndex<DType=T>,
    cur_idx: usize,
    phantom: PhantomData<T>
}
impl<'a, T> DataIterator<'a, T>
    where T: 'a
{
    /// Create a new `DataIterator` from a type that implements `DataIndex`.
    pub fn new(data: &'a dyn DataIndex<DType=T>) -> DataIterator<'a, T> {
        DataIterator {
            data,
            cur_idx: 0,
            phantom: PhantomData
        }
    }
}
impl<'a, T> Iterator for DataIterator<'a, T>
    where T: 'a
{
    type Item = Value<&'a T>;

    fn next(&mut self) -> Option<Value<&'a T>> {
        if self.cur_idx < self.data.len() {
            let out = Some(self.data.get_datum(self.cur_idx).unwrap());
            self.cur_idx += 1;
            out
        } else {
            None
        }
    }
}

pub trait SortOrder
{
    fn sort_order(&self) -> Vec<usize>;
}

impl<DI> SortOrder
    for DI
    where
        DI: DataIndex,
        <DI as DataIndex>::DType: Ord
{
    fn sort_order(&self) -> Vec<usize>
    {
        let mut order = (0..self.len()).collect::<Vec<_>>();
        order.sort_by(|&left, &right| {
            // a, b are always in range, so unwraps are safe
            self.get_datum(left).unwrap().cmp(&self.get_datum(right).unwrap())
        });
        order
    }
}

pub trait SortOrderUnstable
{
    fn sort_order_unstable(&self) -> Vec<usize>;
}

impl<DI> SortOrderUnstable
    for DI
    where
        DI: DataIndex,
        <DI as DataIndex>::DType: Ord
{
    fn sort_order_unstable(&self) -> Vec<usize>
    {
        let mut order = (0..self.len()).collect::<Vec<_>>();
        order.sort_unstable_by(|&left, &right| {
            // a, b are always in range, so unwraps are safe
            self.get_datum(left).unwrap().cmp(&self.get_datum(right).unwrap())
        });
        order
    }
}

pub trait SortOrderComparator<F>
{
    fn sort_order_by(&self, compare: F) -> Vec<usize>;
}

impl<DI, F> SortOrderComparator<F>
    for DI
    where
        DI: DataIndex,
        F: FnMut(Value<&DI::DType>, Value<&DI::DType>) -> Ordering
{
    fn sort_order_by(&self, mut compare: F) -> Vec<usize> {
        let mut order = (0..self.len()).collect::<Vec<_>>();
        order.sort_by(|&left, &right| {
            compare(self.get_datum(left).unwrap(), self.get_datum(right).unwrap())
        });
        order
    }
}

pub trait SortOrderUnstableComparator<F>
{
    fn sort_order_unstable_by(&self, compare: F) -> Vec<usize>;
}


impl<DI, F> SortOrderUnstableComparator<F>
    for DI
    where
        DI: DataIndex,
        F: FnMut(Value<&DI::DType>, Value<&DI::DType>) -> Ordering
{
    fn sort_order_unstable_by(&self, mut compare: F) -> Vec<usize> {
        let mut order = (0..self.len()).collect::<Vec<_>>();
        order.sort_unstable_by(|&left, &right| {
            compare(self.get_datum(left).unwrap(), self.get_datum(right).unwrap())
        });
        order
    }
}

pub trait FilterPerm<P>
{
    fn filter_perm(&self, predicate: P) -> Vec<usize>;
}

impl<DI, P> FilterPerm<P>
    for DI
    where
        DI: DataIndex,
        P: FnMut(Value<&DI::DType>) -> bool
{
    fn filter_perm(&self, mut predicate: P) -> Vec<usize>
    {
        let order = (0..self.len()).collect::<Vec<_>>();
        order.iter().filter(|&&idx| {
            predicate(self.get_datum(idx).unwrap())
        }).map(|&idx| idx).collect()
    }
}
