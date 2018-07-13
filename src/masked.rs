//! Missing value handling structs.

use std::mem;
use std::hash::{Hash, Hasher};
use std::fmt;

use serde::ser::{Serialize, Serializer, SerializeSeq};

use field::DataType;
use bit_vec::BitVec;
use apply::*;
use error;

/// Missing value container.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum MaybeNa<T: DataType> {
    /// Indicates a missing (NA) value.
    Na,
    /// Indicates an existing value.
    Exists(T)
}
impl<T: DataType> MaybeNa<T> {
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
    /// Returns a `MaybeNa` which contains a reference to the original underlying datum.
    pub fn as_ref<'a>(&'a self) -> MaybeNa<&'a T> {
        match *self {
            MaybeNa::Exists(ref val) => MaybeNa::Exists(&val),
            MaybeNa::Na => MaybeNa::Na
        }
    }
    /// Applies function `f` if this `MaybeNa` exists.
    pub fn map<U: DataType, F: FnMut(T) -> U>(self, mut f: F) -> MaybeNa<U> {
        match self {
            MaybeNa::Exists(val) => MaybeNa::Exists(f(val)),
            MaybeNa::Na => MaybeNa::Na
        }
    }
}
impl<'a, T: DataType + Clone> MaybeNa<&'a T> {
    /// Create a owner `MaybeNa` out of a reference-holding `MaybeNa` using `clone()`.
    pub fn cloned(self) -> MaybeNa<T> {
        match self {
            MaybeNa::Exists(t) => MaybeNa::Exists(t.clone()),
            MaybeNa::Na => MaybeNa::Na
        }
    }
}
impl<T: DataType> fmt::Display for MaybeNa<T> where T: fmt::Display {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            MaybeNa::Exists(ref t) => write!(f, "{}", t),
            MaybeNa::Na        => write!(f, "NA")
        }
    }
}
impl<'a, T: DataType> Hash for MaybeNa<T> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        mem::discriminant(self).hash(state);
        if let MaybeNa::Exists(ref t) = *self {
            t.dt_hash(state);
        }
    }
}

/// Trait for any type that can be convert into a `MaybeNa` type.
pub trait IntoMaybeNa {
    /// The `DataType` of the resulting `MaybeNa` type,
    type DType: DataType;
    /// Convert this type into a `MaybeNa`.
    fn into_maybena(self) -> MaybeNa<Self::DType>;
}
impl<D: DataType> IntoMaybeNa for MaybeNa<D> {
    type DType = D;
    fn into_maybena(self) -> MaybeNa<D> { self }
}
impl IntoMaybeNa for () {
    type DType = bool;
    fn into_maybena(self) -> MaybeNa<bool> { MaybeNa::Na }
}
impl<D: DataType> IntoMaybeNa for D {
    type DType = D;
    fn into_maybena(self) -> MaybeNa<D> { MaybeNa::Exists(self) }
}

/// Data vector along with bit-vector-based mask indicating whether or not values exist.
#[derive(Debug, Clone)]
pub struct MaskedData<T> {
    mask: BitVec,
    data: Vec<T>
}
impl<T: DataType> MaskedData<T> {
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
impl<T: Default + DataType> MaskedData<T> {
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
impl<T: DataType + Default, U: Into<T>> From<Vec<U>> for MaskedData<T> {
    fn from(other: Vec<U>) -> MaskedData<T> {
        MaskedData::from_vec(other)
    }
}

macro_rules! impl_masked_data_index {
    ($($ty:ty)*) => {$(
        impl DataIndex<$ty> for MaskedData<$ty> {
            fn get_data(&self, idx: usize) -> error::Result<MaybeNa<&$ty>> {
                self.get(idx).ok_or(error::AgnesError::IndexError { index: idx, len: self.len() })
            }
            fn len(&self) -> usize {
                self.len()
            }
        }
    )*}
}
impl_masked_data_index!(u64 i64 String bool f64);

impl<T: DataType> MaskedData<T> {
    /// Apply a `MapFn` to this data vector at the specified index.
    pub fn apply<F: MapFn>(&self, f: &mut F, idx: usize)
        -> error::Result<<F as ApplyToDatum<T>>::Output>
        where F: ApplyToDatum<T>
    {
        self.get(idx).map(|value| f.apply_to_datum(value))
            .ok_or(error::AgnesError::IndexError { index: idx, len: self.len() })
    }
}

macro_rules! impl_field_apply {
    ($($apply_fn:tt; $dtype:ty)*) => {$(

impl FieldApply for MaskedData<$dtype> {
    fn field_apply<F: FieldMapFn>(&self, f: &mut F) -> error::Result<F::Output> {
        Ok(f.$apply_fn(self))
    }
}

    )*}
}
impl_field_apply!(
    apply_unsigned; u64
    apply_signed;   i64
    apply_text;     String
    apply_boolean;  bool
    apply_float;    f64
);

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
