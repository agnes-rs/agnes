//! Missing value handling structs.

use serde::ser::{Serialize, Serializer, SerializeSeq};

use bit_vec::BitVec;
use apply::*;

/// Missing value container.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum MaybeNa<T: PartialOrd> {
    /// Indicates a missing (NA) value.
    Na,
    /// Indicates an existing value.
    Exists(T)
}
impl<T: ToString + PartialOrd> ToString for MaybeNa<T> {
    fn to_string(&self) -> String {
        match *self {
            MaybeNa::Na => "NA".into(),
            MaybeNa::Exists(ref t) => t.to_string()
        }
    }
}
impl<T: PartialOrd> MaybeNa<T> {
    /// Unwrap a `MaybeNa`, revealing the data contained within. Panics if called on an `Na` value.
    pub fn unwrap(self) -> T {
        match self {
            MaybeNa::Na => { panic!("unwrap() called on NA value"); },
            MaybeNa::Exists(t) => t
        }
    }
    /// Test if a `MaybeNa` contains a value.
    pub fn exists(&self) -> bool {
        match *self {
            MaybeNa::Exists(_) => true,
            MaybeNa::Na => false,
        }
    }
    /// Test if a `MaybeNa` is NA.
    pub fn is_na(&self) -> bool {
        match *self {
            MaybeNa::Exists(_) => false,
            MaybeNa::Na => true,
        }
    }
}
impl<'a, T: PartialOrd + Clone> MaybeNa<&'a T> {
    /// Create a owner `MaybeNa` out of a reference-holding `MaybeNa` using `clone()`.
    pub fn cloned(self) -> MaybeNa<T> {
        match self {
            MaybeNa::Exists(t) => MaybeNa::Exists(t.clone()),
            MaybeNa::Na => MaybeNa::Na
        }
    }
}

/// Data vector along with bit-vector-based mask indicating whether or not values exist.
#[derive(Debug, Clone)]
pub struct MaskedData<T> {
    mask: BitVec,
    data: Vec<T>
}
impl<T: PartialOrd> MaskedData<T> {
    /// Length of this data vector
    pub fn len(&self) -> usize {
        assert_eq!(self.mask.len(), self.data.len());
        self.data.len()
    }
    /// Get the value at the given index. Return `None` if `index` is out of bounds, or a `MaybeNa`
    /// Object with the value (or indicator that value is missing).
    pub fn get(&self, index: usize) -> Option<MaybeNa<&T>> {
        if index >= self.data.len() {
            None
        } else {
            if self.mask[index] {
                Some(MaybeNa::Exists(&self.data[index]))
            } else {
                Some(MaybeNa::Na)
            }
        }
    }
    /// Interpret `MaskedData` as a `Vec` of `MaybeNa` objects.
    pub fn as_vec(&self) -> Vec<MaybeNa<&T>> {
        self.data.iter().enumerate().map(|(idx, value)| {
            if self.mask[idx] {
                MaybeNa::Exists(value)
            } else {
                MaybeNa::Na
            }
        }).collect()
    }
}
impl<T: Default + PartialOrd> MaskedData<T> {
    /// Create new empty `MaskedData` struct.
    pub fn new() -> MaskedData<T> {
        MaskedData {
            data: vec![],
            mask: BitVec::new()
        }
    }
    /// Create new masked data vector with single element.
    pub fn new_with_elem(value: MaybeNa<T>) -> MaskedData<T> {
        if let MaybeNa::Exists(v) = value {
            MaskedData {
                data: vec!(v),
                mask: BitVec::from_elem(1, true)
            }
        } else {
            MaskedData {
                data: vec![T::default()],
                mask: BitVec::from_elem(1, false)
            }
        }
    }
    /// Add a new value (or an indication of a missing one) to the data vector
    pub fn push(&mut self, value: MaybeNa<T>) {
        if let MaybeNa::Exists(v) = value {
            self.data.push(v);
            self.mask.push(true);
        } else {
            self.data.push(T::default());
            self.mask.push(false);
        }
    }
    /// Create a `MaskedData` struct from a vector of non-NA values. Resulting `MaskedData` struct
    /// will have no `MaybeNa::Na` values.
    pub fn from_vec<U: Into<T>>(mut v: Vec<U>) -> MaskedData<T> {
        MaskedData {
            mask: BitVec::from_elem(v.len(), true),
            data: v.drain(..).map(|value| value.into()).collect(),
        }
    }
    /// Create a `MaskedData` struct from a vector of masked values.
    pub fn from_masked_vec(mut v: Vec<MaybeNa<T>>) -> MaskedData<T> {
        let mut ret = MaskedData::new();
        for elem in v.drain(..) {
            ret.push(elem);
        }
        ret
    }
}
impl<T: PartialOrd + Default, U: Into<T>> From<Vec<U>> for MaskedData<T> {
    fn from(other: Vec<U>) -> MaskedData<T> {
        MaskedData::from_vec(other)
    }
}

macro_rules! impl_masked_data_index {
    ($($ty:ty)*) => {$(
        impl DataIndex<$ty> for MaskedData<$ty> {
            fn get_data(&self, idx: usize) -> Option<MaybeNa<&$ty>> {
                self.get(idx)
            }
            fn len(&self) -> usize {
                self.len()
            }
        }
    )*}
}
impl_masked_data_index!(u64 i64 String bool f64);

impl ApplyToElem<IndexSelector> for MaskedData<u64> {
    fn apply_to_elem<F: ElemFn>(&self, mut f: F, select: IndexSelector) -> Option<F::Output> {
        self.get(select.index()).map(|value| f.apply_unsigned(value))
    }
}
impl ApplyToElem<IndexSelector> for MaskedData<i64> {
    fn apply_to_elem<F: ElemFn>(&self, mut f: F, select: IndexSelector) -> Option<F::Output> {
        self.get(select.index()).map(|value| f.apply_signed(value))
    }
}
impl ApplyToElem<IndexSelector> for MaskedData<String> {
    fn apply_to_elem<F: ElemFn>(&self, mut f: F, select: IndexSelector) -> Option<F::Output> {
        self.get(select.index()).map(|value| f.apply_text(value))
    }
}
impl ApplyToElem<IndexSelector> for MaskedData<bool> {
    fn apply_to_elem<F: ElemFn>(&self, mut f: F, select: IndexSelector) -> Option<F::Output> {
        self.get(select.index()).map(|value| f.apply_boolean(value))
    }
}
impl ApplyToElem<IndexSelector> for MaskedData<f64> {
    fn apply_to_elem<F: ElemFn>(&self, mut f: F, select: IndexSelector) -> Option<F::Output> {
        self.get(select.index()).map(|value| f.apply_float(value))
    }
}

impl ApplyToField<NilSelector> for MaskedData<u64> {
    fn apply_to_field<F: FieldFn>(&self, mut f: F, _: NilSelector) -> Option<F::Output> {
        Some(f.apply_unsigned(self))
    }
}
impl ApplyToField<NilSelector> for MaskedData<i64> {
    fn apply_to_field<F: FieldFn>(&self, mut f: F, _: NilSelector) -> Option<F::Output> {
        Some(f.apply_signed(self))
    }
}
impl ApplyToField<NilSelector> for MaskedData<String> {
    fn apply_to_field<F: FieldFn>(&self, mut f: F, _: NilSelector) -> Option<F::Output> {
        Some(f.apply_text(self))
    }
}
impl ApplyToField<NilSelector> for MaskedData<bool> {
    fn apply_to_field<F: FieldFn>(&self, mut f: F, _: NilSelector) -> Option<F::Output> {
        Some(f.apply_boolean(self))
    }
}
impl ApplyToField<NilSelector> for MaskedData<f64> {
    fn apply_to_field<F: FieldFn>(&self, mut f: F, _: NilSelector) -> Option<F::Output> {
        Some(f.apply_float(self))
    }
}

impl<'a, 'b> ApplyToField2<NilSelector> for (&'a MaskedData<u64>, &'b MaskedData<u64>) {
    fn apply_to_field2<F: Field2Fn>(&self, mut f: F, _: (NilSelector, NilSelector))
        -> Option<F::Output>
    {
        Some(f.apply_unsigned(self))
    }
}
impl<'a, 'b> ApplyToField2<NilSelector> for (&'a MaskedData<i64>, &'b MaskedData<i64>) {
    fn apply_to_field2<F: Field2Fn>(&self, mut f: F, _: (NilSelector, NilSelector))
        -> Option<F::Output>
    {
        Some(f.apply_signed(self))
    }
}
impl<'a, 'b> ApplyToField2<NilSelector> for (&'a MaskedData<String>, &'b MaskedData<String>) {
    fn apply_to_field2<F: Field2Fn>(&self, mut f: F, _: (NilSelector, NilSelector))
        -> Option<F::Output>
    {
        Some(f.apply_text(self))
    }
}
impl<'a, 'b> ApplyToField2<NilSelector> for (&'a MaskedData<bool>, &'b MaskedData<bool>) {
    fn apply_to_field2<F: Field2Fn>(&self, mut f: F, _: (NilSelector, NilSelector))
        -> Option<F::Output>
    {
        Some(f.apply_boolean(self))
    }
}
impl<'a, 'b> ApplyToField2<NilSelector> for (&'a MaskedData<f64>, &'b MaskedData<f64>) {
    fn apply_to_field2<F: Field2Fn>(&self, mut f: F, _: (NilSelector, NilSelector))
        -> Option<F::Output>
    {
        Some(f.apply_float(self))
    }
}

impl<T: Serialize> Serialize for MaskedData<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error> where S: Serializer {
        let mut seq = serializer.serialize_seq(Some(self.data.len()))?;
        for (mask, elem) in self.mask.iter().zip(self.data.iter()) {
            if mask {
                seq.serialize_element(elem)?;
            } else {
                seq.serialize_element("null")?;
            }
        }
        seq.end()
    }
}

// pub trait GetData<T: PartialOrd> {
//     fn get_data(&self, idx: usize) -> Option<MaybeNa<&T>>;
// }


// /// Common enum for different kinds of data vectors that can be held in a field.
// pub enum FieldData<'a> {
//     /// Field data vector containing unsigned data.
//     Unsigned(&'a MaskedData<u64>),
//     /// Field data vector containing signed data.
//     Signed(&'a MaskedData<i64>),
//     /// Field data vector containing text data.
//     Text(&'a MaskedData<String>),
//     /// Field data vector containing boolean data.
//     Boolean(&'a MaskedData<bool>),
//     /// Field data vector containing floating-point data.
//     Float(&'a MaskedData<f64>),
// }
// impl<'a> FieldData<'a> {
//     /// Length of the data vector.
//     pub fn len(&self) -> usize {
//         match *self {
//             FieldData::Unsigned(v) => v.data.len(),
//             FieldData::Signed(v)   => v.data.len(),
//             FieldData::Text(v)     => v.data.len(),
//             FieldData::Boolean(v)  => v.data.len(),
//             FieldData::Float(v)    => v.data.len(),
//         }
//     }
//     /// Whether this data's field is empty
//     pub fn is_empty(&self) -> bool {
//         self.len() == 0
//     }
//     /// Returns the `FieldType` for this field.
//     pub fn get_field_type(&self) -> FieldType {
//         match *self {
//             FieldData::Unsigned(_)  => FieldType::Unsigned,
//             FieldData::Signed(_)    => FieldType::Signed,
//             FieldData::Text(_)      => FieldType::Text,
//             FieldData::Boolean(_)   => FieldType::Boolean,
//             FieldData::Float(_)     => FieldType::Float,
//         }
//     }
// }

// impl<'a> ApplyToElem for FieldData<'a> {
//     fn apply_to_elem<T: ElemFn>(&self, mut f: T, idx: usize) -> Option<T::Output> {
//         match *self {
//             FieldData::Unsigned(v) => v.get(idx)
//                 .map(|value| f.apply_unsigned(value)),
//             FieldData::Signed(v)   => v.get(idx)
//                 .map(|value| f.apply_signed(value)),
//             FieldData::Text(v)     => v.get(idx)
//                 .map(|value| f.apply_text(value)),
//             FieldData::Boolean(v)  => v.get(idx)
//                 .map(|value| f.apply_boolean(value)),
//             FieldData::Float(v)    => v.get(idx)
//                 .map(|value| f.apply_float(value)),
//         }
//     }
// }
// impl<'a> ApplyToField for FieldData<'a> {
//     fn apply_to_field<T: FieldFn>(&self, f: T) -> Option<T::Output> {
//         match *self {
//             FieldData::Unsigned(v) => f.apply_unsigned(v),
//             FieldData::Signed(v)   => f.apply_signed(v),
//             FieldData::Text(v)     => f.apply_text(v),
//             FieldData::Boolean(v)  => f.apply_boolean(v),
//             FieldData::Float(v)    => f.apply_float(v),
//         }
//     }
// }

// impl<'a> Serialize for FieldData<'a> {
//     fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error> where S: Serializer {
//         match *self {
//             FieldData::Unsigned(v) => v.serialize(serializer),
//             FieldData::Signed(v)   => v.serialize(serializer),
//             FieldData::Text(v)     => v.serialize(serializer),
//             FieldData::Boolean(v)  => v.serialize(serializer),
//             FieldData::Float(v)    => v.serialize(serializer),
//         }
//     }
// }
// macro_rules! impl_from_masked_data {
//     ($($variant:path: $data_type:ty)*) => {$(
//         impl<'a> From<&'a MaskedData<$data_type>> for FieldData<'a> {
//             fn from(other: &'a MaskedData<$data_type>) -> FieldData<'a> {
//                 $variant(other)
//             }
//         }
//     )*}
// }
// impl_from_masked_data!(
//     FieldData::Unsigned: u64
//     FieldData::Signed:   i64
//     FieldData::Text:     String
//     FieldData::Boolean:  bool
//     FieldData::Float:    f64
// );
