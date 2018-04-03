//! Missing value handling structs.

use serde::ser::{Serialize, Serializer, SerializeSeq};

use bit_vec::BitVec;
use field::FieldType;

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
    pub fn unwrap(self) -> T {
        match self {
            MaybeNa::Na => { panic!("unwrap() called on NA value"); },
            MaybeNa::Exists(t) => t
        }
    }
}
impl<'a, T: PartialOrd + Clone> MaybeNa<&'a T> {
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
}
impl<T: Default + PartialOrd> MaskedData<T> {
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
    pub fn from_vec(v: Vec<T>) -> MaskedData<T> {
        MaskedData {
            mask: BitVec::from_elem(v.len(), true),
            data: v,
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

/// Common enum for different kinds of data vectors that can be held in a field.
pub enum FieldData<'a> {
    /// Field data vector containing unsigned data.
    Unsigned(&'a MaskedData<u64>),
    /// Field data vector containing signed data.
    Signed(&'a MaskedData<i64>),
    /// Field data vector containing text data.
    Text(&'a MaskedData<String>),
    /// Field data vector containing boolean data.
    Boolean(&'a MaskedData<bool>),
    /// Field data vector containing floating-point data.
    Float(&'a MaskedData<f64>),
}
impl<'a> FieldData<'a> {
    /// Length of the data vector.
    pub fn len(&self) -> usize {
        match *self {
            FieldData::Unsigned(v) => v.data.len(),
            FieldData::Signed(v)   => v.data.len(),
            FieldData::Text(v)     => v.data.len(),
            FieldData::Boolean(v)  => v.data.len(),
            FieldData::Float(v)    => v.data.len(),
        }
    }
    /// Whether this data's field is empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    /// Returns the `FieldType` for this field.
    pub fn get_field_type(&self) -> FieldType {
        match *self {
            FieldData::Unsigned(_)  => FieldType::Unsigned,
            FieldData::Signed(_)    => FieldType::Signed,
            FieldData::Text(_)      => FieldType::Text,
            FieldData::Boolean(_)   => FieldType::Boolean,
            FieldData::Float(_)     => FieldType::Float,
        }
    }
}

impl<'a> Serialize for FieldData<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error> where S: Serializer {
        match *self {
            FieldData::Unsigned(v) => v.serialize(serializer),
            FieldData::Signed(v)   => v.serialize(serializer),
            FieldData::Text(v)     => v.serialize(serializer),
            FieldData::Boolean(v)  => v.serialize(serializer),
            FieldData::Float(v)    => v.serialize(serializer),
        }
    }
}
