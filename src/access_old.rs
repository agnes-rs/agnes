/*!
Traits for accessing data within agnes data structures. Includes `DataIndex` for index-based access
and `DataIterator` for iterator access.
*/
use std::fmt::Debug;
use std::marker::PhantomData;

use error::*;
use field::Value;
use data_types::{DTypeList, DataType};

/// Trait that provides access to values in a data field.
pub trait DataIndex<DTypes>: Debug
    where DTypes: DTypeList,
{
    /// The data type contained within this field.
    type DType: DataType<DTypes>;

    /// Returns the data (possibly NA) at the specified index, if it exists.
    fn get_datum(&self, idx: usize) -> Result<Value<&Self::DType>>;
    /// Returns the length of this data field.
    fn len(&self) -> usize;
    /// Returns whether or not this field is empty.
    fn is_empty(&self) -> bool { self.len() == 0 }
    /// Returns an iterator over the values in this field.
    fn iter(&self) -> DataIterator<DTypes, Self::DType> where Self: Sized {
        DataIterator::new(self)
    }
}
/// Trait that provides mutable access to values in a data field.
pub trait DataIndexMut<DTypes>: DataIndex<DTypes>
    where DTypes: DTypeList
{
    /// Add a value to this field.
    fn push(&mut self, value: Value<Self::DType>);
}

/// Iterator over the data in a data structure that implement DataIndex.
pub struct DataIterator<'a, DTypes, T>
    where DTypes: 'a + DTypeList,
          T: 'a + DataType<DTypes>
{
    data: &'a dyn DataIndex<DTypes, DType=T>,
    cur_idx: usize,
    phantom: PhantomData<T>
}
impl<'a, DTypes, T> DataIterator<'a, DTypes, T>
    where DTypes: DTypeList,
          T: 'a + DataType<DTypes>,
{
    /// Create a new `DataIterator` from a type that implements `DataIndex`.
    pub fn new(data: &'a dyn DataIndex<DTypes, DType=T>) -> DataIterator<'a, DTypes, T> {
        DataIterator {
            data,
            cur_idx: 0,
            phantom: PhantomData
        }
    }
}
impl<'a, DTypes, T> Iterator for DataIterator<'a, DTypes, T>
    where DTypes: DTypeList,
          T: 'a + DataType<DTypes>,
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

/// Either an owned data structure or reference to a data structure that implements `DataIndex`.
#[derive(Debug)]
pub enum OwnedOrRef<'a, DTypes, T>
    where DTypes: 'a + DTypeList,
          T: 'a + DataType<DTypes>
{
    /// A boxed data structure that implemented `DataIndex`.
    Owned(Box<dyn DataIndex<DTypes, DType=T> + 'a>),
    /// A reference to a data structure that implements `DataIndex`.
    Ref(&'a dyn DataIndex<DTypes, DType=T>),
}
impl<'a, DTypes, T> DataIndex<DTypes> for OwnedOrRef<'a, DTypes, T>
    where DTypes: DTypeList,
          T: 'a + DataType<DTypes>
{
    type DType = T;

    fn get_datum(&self, idx: usize) -> Result<Value<&T>> {
        match *self {
            OwnedOrRef::Owned(ref data) => data.get_datum(idx),
            OwnedOrRef::Ref(ref data) => data.get_datum(idx),
        }
    }
    fn len(&self) -> usize {
        match *self {
            OwnedOrRef::Owned(ref data) => data.len(),
            OwnedOrRef::Ref(ref data) => data.len(),
        }
    }
}
