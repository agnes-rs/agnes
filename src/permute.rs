/*!
Structures, traits, and implementations for handling data permutations.

[Permutation](struct.Permutation.html) objects represent the index order for a data collection if
a non-original order exists. This can be used to represent a possible sorting (where all indices are
included) or filtering (where a strict subset of the indices are included) of the data set.

This module also contains traits and methods for sorting data sets.
*/
use std::cmp::Ordering;

use access::DataIndex;
use field::Value;

/// A structure containing information about the permutation status of a field. `I` represents the
/// underlying permutation implementation type (such as `Vec<usize>` or &[usize]).
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct Permutation<I> {
    perm: Option<I>,
}
impl<I> Default for Permutation<I> {
    fn default() -> Permutation<I> {
        Permutation { perm: None }
    }
}
impl<'a> From<&'a [usize]> for Permutation<&'a [usize]> {
    fn from(orig: &'a [usize]) -> Permutation<&'a [usize]> {
        Permutation { perm: Some(orig) }
    }
}

impl Permutation<Vec<usize>> {
    /// Update this permutation with new values from `new_permutation`.
    pub fn update(&mut self, new_permutation: &[usize]) {
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
            /// Returns the length of this permutation, if it exists. `None` means that no
            /// permutation exists (the full field in its original order can be used).
            pub fn len(&self) -> Option<usize>
            {
                self.perm.as_ref().map(|perm| perm.len())
            }
            /// Returns whether or not a permutation actually exists.
            pub fn is_permuted(&self) -> bool { self.perm.is_some() }
        }
    )*}
}
impl_permutation_len![&[usize] Vec<usize>];

/// Trait providing function to compute and return the sorted permutation order. This sort is stable
/// (preserves original order of equal elements).
pub trait SortOrder {
    /// Returns the stable sorted permutation order as `Vec<usize>`
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

/// Trait providing function to compute and return the sorted permutation order. This sort is
/// unstable (does not preserve original order of equal elements, but may be faster than the stable
/// version).
pub trait SortOrderUnstable {
    /// Returns the unstable sorted permutation order (`Vec<usize>`).
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

/// Trait providing function to compute and return the sorted permutation order using a comparator.
/// This sort is stable (preserves original order of equal elements).
pub trait SortOrderComparator<F> {
    /// Returns the stable sorted permutation order (`Vec<usize>`) using the specified comparator.
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

/// Trait providing function to compute and return the sorted permutation order. This sort is
/// unstable (does not preserve original order of equal elements, but may be faster than the stable
/// version).
pub trait SortOrderUnstableComparator<F> {
    /// Returns the unstable sorted permutation order (`Vec<usize>`) using the specified comparator.
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

/// Helper sorting method for floating-point (f32) values
pub fn sort_f32(left: &f32, right: &f32) -> Ordering {
    left.partial_cmp(&right).unwrap_or_else(|| {
        if left.is_nan() && !right.is_nan() {
            Ordering::Less
        } else {
            // since partial_cmp only fails for NAN, then !self.is_nan() && other.is_nan()
            Ordering::Greater
        }
    })
}
/// Helper sorting method for floating-point (Value<&f32>) values.
pub fn sort_f32_values(left: Value<&f32>, right: Value<&f32>) -> Ordering {
    match (left, right) {
        (Value::Na, Value::Na) => Ordering::Equal,
        (Value::Na, Value::Exists(_)) => Ordering::Less,
        (Value::Exists(_), Value::Na) => Ordering::Greater,
        (Value::Exists(ref left), Value::Exists(ref right)) => sort_f32(left, right),
    }
}

/// Helper sorting method for floating-point (f64) values
pub fn sort_f64(left: &f64, right: &f64) -> Ordering {
    left.partial_cmp(&right).unwrap_or_else(|| {
        if left.is_nan() && !right.is_nan() {
            Ordering::Less
        } else {
            // since partial_cmp only fails for NAN, then !self.is_nan() && other.is_nan()
            Ordering::Greater
        }
    })
}
/// Helper sorting method for floating-point (Value<&f64>) values.
pub fn sort_f64_values(left: Value<&f64>, right: Value<&f64>) -> Ordering {
    match (left, right) {
        (Value::Na, Value::Na) => Ordering::Equal,
        (Value::Na, Value::Exists(_)) => Ordering::Less,
        (Value::Exists(_), Value::Na) => Ordering::Greater,
        (Value::Exists(ref left), Value::Exists(ref right)) => sort_f64(left, right),
    }
}

/// Trait providing method to provide an index permutation of values that match a predicate.
pub trait FilterPerm<P> {
    /// Returns the permutation indices of this field which match the specified `predicate`.
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
        let sorted_order = field_data.sort_order_by(sort_f64_values);
        assert_eq!(sorted_order, vec![3, 0, 2, 1, 4]);

        let field_data: FieldData<f64> =
            FieldData::from_vec(vec![2.0, ::std::f64::NAN, 3.1, 1.1, 8.2]);
        let sorted_order = field_data.sort_order_by(sort_f64_values);
        assert_eq!(sorted_order, vec![1, 3, 0, 2, 4]);

        let field_data: FieldData<f64> =
            FieldData::from_vec(vec![2.0, ::std::f64::NAN, 3.1, ::std::f64::INFINITY, 8.2]);
        let sorted_order = field_data.sort_order_by(sort_f64_values);
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
        let sorted_order = field_data.sort_order_by(sort_f64_values);
        assert_eq!(sorted_order, vec![2, 3, 0, 1, 4]);

        let field_data = FieldData::from_field_vec(vec![
            Value::Exists(2.1),
            Value::Exists(::std::f64::NAN),
            Value::Na,
            Value::Exists(1.1),
            Value::Exists(8.2930),
        ]);
        let sorted_order = field_data.sort_order_by(sort_f64_values);
        assert_eq!(sorted_order, vec![2, 1, 3, 0, 4]);

        let field_data = FieldData::from_field_vec(vec![
            Value::Exists(2.1),
            Value::Exists(::std::f64::NAN),
            Value::Na,
            Value::Exists(::std::f64::INFINITY),
            Value::Exists(8.2930),
        ]);
        let sorted_order = field_data.sort_order_by(sort_f64_values);
        assert_eq!(sorted_order, vec![2, 1, 0, 4, 3]);
    }
}
