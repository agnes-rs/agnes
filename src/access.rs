/*!
Traits for accessing data within agnes data structures. Includes `DataIndex` for index-based access
and `DataIterator` for iterator access.
*/
use std::cmp::Ordering;
use std::fmt::Debug;
use std::marker::PhantomData;

use error::*;
use field::Value;

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

    fn permute<'a, 'b>(
        &'a self,
        permutation: &'b [usize],
    ) -> Result<DataIterator<'a, 'b, Self::DType>>
    where
        Self: Sized,
    {
        DataIterator::with_permutation(self, permutation)
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
}

/// Iterator over the data in a data structure that implement DataIndex.
pub struct DataIterator<'a, 'b, T>
where
    T: 'a,
{
    data: &'a dyn DataIndex<DType = T>,
    permutation: Permutation<&'b [usize]>,
    cur_idx: usize,
    phantom: PhantomData<T>,
}
impl<'a, 'b, T> DataIterator<'a, 'b, T>
where
    T: 'a,
{
    /// Create a new `DataIterator` from a type that implements `DataIndex`.
    pub fn new(data: &'a dyn DataIndex<DType = T>) -> DataIterator<'a, 'b, T> {
        DataIterator {
            data,
            permutation: Permutation::default(),
            cur_idx: 0,
            phantom: PhantomData,
        }
    }
    pub fn with_permutation(
        data: &'a dyn DataIndex<DType = T>,
        permutation: &'b [usize],
    ) -> Result<DataIterator<'a, 'b, T>> {
        if permutation.len() > 0 && permutation.iter().max().unwrap() >= &data.len() {
            return Err(AgnesError::LengthMismatch {
                expected: data.len(),
                actual: permutation.len(),
            });
        }
        Ok(DataIterator {
            data,
            permutation: Permutation {
                perm: Some(permutation),
            },
            cur_idx: 0,
            phantom: PhantomData,
        })
    }
}

impl<'a, 'b, T> Iterator for DataIterator<'a, 'b, T>
where
    T: 'a,
{
    type Item = Value<&'a T>;

    fn next(&mut self) -> Option<Value<&'a T>> {
        // use permutation length as length of iterator when permutation exists, otherwise use
        // data length
        if self.permutation.is_permuted() && self.cur_idx < self.permutation.len().unwrap()
            || !self.permutation.is_permuted() && self.cur_idx < self.data.len()
        {
            let out = Some(
                self.data
                    .get_datum(self.permutation.map_index(self.cur_idx))
                    .unwrap(),
            );
            self.cur_idx += 1;
            out
        } else {
            None
        }
    }
}

#[derive(Debug, Clone)]
pub struct Permutation<I> {
    perm: Option<I>,
}
impl<I> Default for Permutation<I> {
    fn default() -> Permutation<I> {
        Permutation { perm: None }
    }
}

impl Permutation<Vec<usize>> {
    pub(crate) fn update(&mut self, new_permutation: &[usize]) {
        // check if we already have a permutation
        self.perm = match self.perm {
            Some(ref prev_perm) => {
                // we already have a permutation, map the filter indices through it
                Some(
                    new_permutation
                        .iter()
                        .map(|&new_idx| prev_perm[new_idx])
                        .collect(),
                )
            }
            None => Some(new_permutation.iter().map(|&idx| idx).collect()),
        };
    }
}

macro_rules! impl_permutation_len {
    ($($t:ty)*) => {$(
        impl Permutation<$t>
        {
            /// Returns the re-organized index of a requested index.
            pub fn map_index(&self, requested: usize) -> usize
            {
                match self.perm
                {
                    Some(ref perm) => perm[requested],
                    None => requested
                }
            }
            pub fn len(&self) -> Option<usize>
            {
                self.perm.as_ref().map(|perm| perm.len())
            }
            pub fn is_permuted(&self) -> bool { self.perm.is_some() }
        }
    )*}
}
impl_permutation_len![&[usize] Vec<usize>];

pub trait SortOrder {
    fn sort_order(&self) -> Vec<usize>;
}

impl<DI> SortOrder for DI
where
    DI: DataIndex,
    <DI as DataIndex>::DType: Ord,
{
    fn sort_order(&self) -> Vec<usize> {
        let mut order = (0..self.len()).collect::<Vec<_>>();
        order.sort_by(|&left, &right| {
            // a, b are always in range, so unwraps are safe
            self.get_datum(left)
                .unwrap()
                .cmp(&self.get_datum(right).unwrap())
        });
        order
    }
}

pub trait SortOrderUnstable {
    fn sort_order_unstable(&self) -> Vec<usize>;
}

impl<DI> SortOrderUnstable for DI
where
    DI: DataIndex,
    <DI as DataIndex>::DType: Ord,
{
    fn sort_order_unstable(&self) -> Vec<usize> {
        let mut order = (0..self.len()).collect::<Vec<_>>();
        order.sort_unstable_by(|&left, &right| {
            // a, b are always in range, so unwraps are safe
            self.get_datum(left)
                .unwrap()
                .cmp(&self.get_datum(right).unwrap())
        });
        order
    }
}

pub trait SortOrderComparator<F> {
    fn sort_order_by(&self, compare: F) -> Vec<usize>;
}

impl<DI, F> SortOrderComparator<F> for DI
where
    DI: DataIndex,
    F: FnMut(Value<&DI::DType>, Value<&DI::DType>) -> Ordering,
{
    fn sort_order_by(&self, mut compare: F) -> Vec<usize> {
        let mut order = (0..self.len()).collect::<Vec<_>>();
        order.sort_by(|&left, &right| {
            compare(
                self.get_datum(left).unwrap(),
                self.get_datum(right).unwrap(),
            )
        });
        order
    }
}

pub trait SortOrderUnstableComparator<F> {
    fn sort_order_unstable_by(&self, compare: F) -> Vec<usize>;
}

impl<DI, F> SortOrderUnstableComparator<F> for DI
where
    DI: DataIndex,
    F: FnMut(Value<&DI::DType>, Value<&DI::DType>) -> Ordering,
{
    fn sort_order_unstable_by(&self, mut compare: F) -> Vec<usize> {
        let mut order = (0..self.len()).collect::<Vec<_>>();
        order.sort_unstable_by(|&left, &right| {
            compare(
                self.get_datum(left).unwrap(),
                self.get_datum(right).unwrap(),
            )
        });
        order
    }
}

pub fn sort_float(left: &f64, right: &f64) -> Ordering {
    left.partial_cmp(&right).unwrap_or_else(|| {
        if left.is_nan() && !right.is_nan() {
            Ordering::Less
        } else {
            // since partial_cmp only fails for NAN, then !self.is_nan() && other.is_nan()
            Ordering::Greater
        }
    })
}
pub fn sort_float_values(left: Value<&f64>, right: Value<&f64>) -> Ordering {
    match (left, right) {
        (Value::Na, Value::Na) => Ordering::Equal,
        (Value::Na, Value::Exists(_)) => Ordering::Less,
        (Value::Exists(_), Value::Na) => Ordering::Greater,
        (Value::Exists(ref left), Value::Exists(ref right)) => sort_float(left, right),
    }
}

pub trait FilterPerm<P> {
    fn filter_perm(&self, predicate: P) -> Vec<usize>;
}

impl<DI, P> FilterPerm<P> for DI
where
    DI: DataIndex,
    P: FnMut(Value<&DI::DType>) -> bool,
{
    fn filter_perm(&self, mut predicate: P) -> Vec<usize> {
        let order = (0..self.len()).collect::<Vec<_>>();
        order
            .iter()
            .filter(|&&idx| predicate(self.get_datum(idx).unwrap()))
            .map(|&idx| idx)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use field::FieldData;

    #[test]
    fn sort_order_no_na() {
        let field_data: FieldData<u64> = FieldData::from_vec(vec![2u64, 5, 3, 1, 8]);
        let sorted_order = field_data.sort_order();
        assert_eq!(sorted_order, vec![3, 0, 2, 1, 4]);

        let field_data: FieldData<f64> = FieldData::from_vec(vec![2.0, 5.4, 3.1, 1.1, 8.2]);
        let sorted_order = field_data.sort_order_by(sort_float_values);
        assert_eq!(sorted_order, vec![3, 0, 2, 1, 4]);

        let field_data: FieldData<f64> =
            FieldData::from_vec(vec![2.0, ::std::f64::NAN, 3.1, 1.1, 8.2]);
        let sorted_order = field_data.sort_order_by(sort_float_values);
        assert_eq!(sorted_order, vec![1, 3, 0, 2, 4]);

        let field_data: FieldData<f64> =
            FieldData::from_vec(vec![2.0, ::std::f64::NAN, 3.1, ::std::f64::INFINITY, 8.2]);
        let sorted_order = field_data.sort_order_by(sort_float_values);
        assert_eq!(sorted_order, vec![1, 0, 2, 4, 3]);
    }

    #[test]
    fn sort_order_na() {
        let field_data = FieldData::from_field_vec(vec![
            Value::Exists(2u64),
            Value::Exists(5),
            Value::Na,
            Value::Exists(1),
            Value::Exists(8),
        ]);
        let sorted_order = field_data.sort_order();
        assert_eq!(sorted_order, vec![2, 3, 0, 1, 4]);

        let field_data = FieldData::from_field_vec(vec![
            Value::Exists(2.1),
            Value::Exists(5.5),
            Value::Na,
            Value::Exists(1.1),
            Value::Exists(8.2930),
        ]);
        let sorted_order = field_data.sort_order_by(sort_float_values);
        assert_eq!(sorted_order, vec![2, 3, 0, 1, 4]);

        let field_data = FieldData::from_field_vec(vec![
            Value::Exists(2.1),
            Value::Exists(::std::f64::NAN),
            Value::Na,
            Value::Exists(1.1),
            Value::Exists(8.2930),
        ]);
        let sorted_order = field_data.sort_order_by(sort_float_values);
        assert_eq!(sorted_order, vec![2, 1, 3, 0, 4]);

        let field_data = FieldData::from_field_vec(vec![
            Value::Exists(2.1),
            Value::Exists(::std::f64::NAN),
            Value::Na,
            Value::Exists(::std::f64::INFINITY),
            Value::Exists(8.2930),
        ]);
        let sorted_order = field_data.sort_order_by(sort_float_values);
        assert_eq!(sorted_order, vec![2, 1, 0, 4, 3]);
    }
}
