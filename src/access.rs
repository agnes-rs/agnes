/*!
Traits for accessing data within agnes data structures.

The [DataIndex](trait.DataIndex.html) trait provides index-based access to a field's data as well
as method which generates a [DataIterator](struct.DataIterator.html).
*/
use std::fmt::Debug;
use std::marker::PhantomData;
use std::rc::Rc;

use error::*;
use field::Value;
use frame::Framed;

/// Trait that provides access to values in a data field.
pub trait DataIndex: Debug {
    /// The data type contained within this field.
    type DType;

    /// Returns the data (possibly NA) at the specified index, if it exists.
    fn get_datum(&self, idx: usize) -> Result<Value<&Self::DType>>;

    /// Returns the length of this data field.
    fn len(&self) -> usize;

    /// Returns whether or not this field is empty.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns an iterator over the values in this field.
    fn iter(&self) -> DataIterator<Self::DType>
    where
        Self: Sized,
    {
        DataIterator::new(self)
    }

    /// Returns a new `DataIndex`-implementing object which provides access to the values in this
    /// field as permuted by `permutation`. `permutation` is a slice of indices into this
    /// `DataIndex`.
    fn permute(self, permutation: &[usize]) -> Framed<Self::DType, Self>
    where
        Self: Sized,
    {
        Framed::new(Rc::new(permutation.to_vec().into()), self)
    }

    /// Copies existing values in this field into a new `Vec`.
    ///
    /// If this field has missing values, this method will return a vector of length less than that
    /// returned by the `len` method.
    fn to_vec(&self) -> Vec<Self::DType>
    where
        Self: Sized,
        Self::DType: Clone,
    {
        self.iter()
            .filter_map(|value| match value {
                Value::Exists(value) => Some(value.clone()),
                Value::Na => None,
            })
            .collect()
    }

    /// Copies values (missing or existing) in this field into a new `Vec`.
    fn to_value_vec(&self) -> Vec<Value<Self::DType>>
    where
        Self: Sized,
        Self::DType: Clone,
    {
        self.iter().map(|value| value.cloned()).collect()
    }
}
/// Trait that provides mutable access to values in a data field.
pub trait DataIndexMut: DataIndex {
    /// Add a value to this field.
    fn push(&mut self, value: Value<Self::DType>);

    /// Take the value at the specified index from this field, replacing it with an NA.
    fn take_datum(&mut self, idx: usize) -> Result<Value<Self::DType>>
    where
        Self::DType: Default;

    /// Returns a draining iterator of the vaules in this `DataIndexMut`.
    fn drain(&mut self) -> DrainIterator<Self::DType>
    where
        Self: Sized,
    {
        DrainIterator::new(self)
    }
}

/// Iterator over the data in a data structure that implement DataIndex.
pub struct DataIterator<'a, T>
where
    T: 'a,
{
    data: &'a dyn DataIndex<DType = T>,
    cur_idx: usize,
    phantom: PhantomData<T>,
}
impl<'a, T> DataIterator<'a, T>
where
    T: 'a,
{
    /// Create a new `DataIterator` from a type that implements `DataIndex`.
    pub fn new(data: &'a dyn DataIndex<DType = T>) -> DataIterator<'a, T> {
        DataIterator {
            data,
            cur_idx: 0,
            phantom: PhantomData,
        }
    }

    /// Returns an iterator applying function `F` to the stored values (where they exist) to this
    /// `DataIterator`. Equivalent to `iter.map(|x: Value<&'a T>| x.map(f))`.
    pub fn map_existing<B, F>(self, f: F) -> ValueMap<'a, T, Self, F>
    where
        Self: Iterator<Item = Value<&'a T>>,
        F: FnMut(&'a T) -> B,
    {
        ValueMap {
            iter: self,
            f,
            _t: PhantomData,
        }
    }
}

impl<'a, T> Iterator for DataIterator<'a, T>
where
    T: 'a,
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

/// Mapping iterator applying function `F` to the data in a data structure that implement DataIndex.
/// `T` is the data type held within this data structure, and `I` is the base iterator that is being
/// mapped over.
#[derive(Clone)]
pub struct ValueMap<'a, T, I, F> {
    iter: I,
    f: F,
    _t: PhantomData<&'a T>,
}

impl<'a, B, T, I, F> Iterator for ValueMap<'a, T, I, F>
where
    I: Iterator<Item = Value<&'a T>>,
    F: FnMut(&'a T) -> B,
{
    type Item = Value<B>;

    #[inline]
    fn next(&mut self) -> Option<Value<B>> {
        self.iter.next().map(|value| value.map(&mut self.f))
    }
}

/// Draining iterator over the data in a data structure that implements DataIndex.
pub struct DrainIterator<'a, T>
where
    T: 'a,
{
    data: &'a mut dyn DataIndexMut<DType = T>,
    cur_idx: usize,
    phantom: PhantomData<T>,
}

impl<'a, T> DrainIterator<'a, T>
where
    T: 'a,
{
    /// Create a new `DrainIterator` from a type that implements `DataIndex`.
    pub fn new(data: &'a mut dyn DataIndexMut<DType = T>) -> DrainIterator<'a, T> {
        DrainIterator {
            data,
            cur_idx: 0,
            phantom: PhantomData,
        }
    }
}

impl<'a, T> Iterator for DrainIterator<'a, T>
where
    T: 'a + Default,
{
    type Item = Value<T>;

    fn next(&mut self) -> Option<Value<T>> {
        if self.cur_idx < self.data.len() {
            let out = Some(self.data.take_datum(self.cur_idx).unwrap());
            self.cur_idx += 1;
            out
        } else {
            None
        }
    }
}

/// Trait to provide the number of rows of this data structure.
pub trait NRows {
    /// Return the number of rows in this data structure.
    fn nrows(&self) -> usize;
}
impl<DI> NRows for DI
where
    DI: DataIndex,
{
    fn nrows(&self) -> usize {
        self.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use field::FieldData;

    #[test]
    fn convert() {
        let field_data = FieldData::from_field_vec(vec![
            Value::Exists(2u64),
            Value::Exists(5),
            Value::Na,
            Value::Exists(1),
            Value::Exists(8),
        ]);
        let new_field_data = field_data
            .iter()
            .map_existing(|u| *u as i64)
            .collect::<FieldData<i64>>();
        assert_eq!(
            new_field_data.to_value_vec(),
            vec![
                Value::Exists(2i64),
                Value::Exists(5),
                Value::Na,
                Value::Exists(1),
                Value::Exists(8),
            ]
        );
    }
}
