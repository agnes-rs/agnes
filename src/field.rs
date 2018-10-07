//! Field-level structs.

use std::marker::PhantomData;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::iter::FromIterator;
use std::mem;

use num_traits::Float;

use serde::ser::{Serialize, Serializer, SerializeSeq};

use data_types::{DTypeList, DataType, TypeSelector, DTypeSelector, CreateStorage};
use bit_vec::BitVec;
// use apply::mapfn::*;
use store::{IntoDataStore, DataStore, WithDataFromIter};
use access::{DataIterator, DataIndex, DataIndexMut};
use error;

/// (Possibly missing) data value container.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Value<T> {
    /// Indicates a missing (NA) value.
    Na,
    /// Indicates an existing value.
    Exists(T)
}
impl<T> Value<T> {
    /// Unwrap a `Value`, revealing the data contained within. Panics if called on an `Na` value.
    pub fn unwrap(self) -> T {
        match self {
            Value::Na => { panic!("unwrap() called on NA value"); },
            Value::Exists(t) => t
        }
    }
    /// Test if a `Value` contains a value.
    pub fn exists(&self) -> bool {
        match *self {
            Value::Exists(_) => true,
            Value::Na => false,
        }
    }
    /// Test if a `Value` is NA.
    pub fn is_na(&self) -> bool {
        match *self {
            Value::Exists(_) => false,
            Value::Na => true,
        }
    }
    /// Returns a `Value` which contains a reference to the original underlying datum.
    pub fn as_ref<'a>(&'a self) -> Value<&'a T> {
        match *self {
            Value::Exists(ref val) => Value::Exists(&val),
            Value::Na => Value::Na
        }
    }
    /// Applies function `f` if this `Value` exists.
    pub fn map<U, F: FnMut(T) -> U>(self, mut f: F) -> Value<U> {
        match self {
            Value::Exists(val) => Value::Exists(f(val)),
            Value::Na => Value::Na
        }
    }
}
impl<'a, T: Clone> Value<&'a T> {
    /// Create a owner `Value` out of a reference-holding `Value` using `clone()`.
    pub fn cloned(self) -> Value<T> {
        match self {
            Value::Exists(t) => Value::Exists(t.clone()),
            Value::Na => Value::Na
        }
    }
}
impl<'a, T> PartialEq<T> for Value<&'a T>
    where T: PartialEq<T>
{
    fn eq(&self, other: &T) -> bool {
        match *self {
            Value::Exists(&ref value) => value.eq(other),
            Value::Na => false,
        }
    }
}
impl<T> fmt::Display for Value<T> where T: fmt::Display {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Value::Exists(ref t) => write!(f, "{}", t),
            Value::Na        => write!(f, "NA")
        }
    }
}
impl<'a, T: Hash> Hash for Value<T> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        mem::discriminant(self).hash(state);
        if let Value::Exists(ref t) = *self {
            t.hash(state);
        }
    }
}
// impl<'a, T: DataTypeHash> Hash for Value<T> {
    // fn hash<H: Hasher>(&self, state: &mut H) {
    //     mem::discriminant(self).hash(state);
    //     if let Value::Exists(ref t) = *self {
    //         t.dt_hash(state);
    //     }
    // }
// }
impl<T> From<T> for Value<T> {
    fn from(orig: T) -> Value<T> { Value::Exists(orig) }
}
impl<'a, T> From<Value<&'a T>> for Value<T>
    where T: 'a + Clone
{
    fn from(orig: Value<&'a T>) -> Value<T> {
        orig.cloned()
    }
}

impl<T> Into<Option<T>> for Value<T> {
    fn into(self) -> Option<T> {
        match self {
            Value::Exists(value) => Some(value),
            Value::Na => None
        }
    }
}
impl<T> From<Option<T>> for Value<T> {
    fn from(orig: Option<T>) -> Value<T> {
        match orig {
            Some(value) => Value::Exists(value),
            None => Value::Na
        }
    }
}
// /// Trait for any type that can be convert into a `Value` type.
// pub trait IntoValue<DTypes> {
//     /// The `DataType` of the resulting `Value` type,
//     type DType: DataType<DTypes>;
//     /// Convert this type into a `Value`.
//     fn into_Value(self) -> Value<Self::DType>;
// }
// impl<DTypes, D: DataType<DTypes>> IntoValue<DTypes> for Value<D> {
//     type DType = D;
//     fn into_Value(self) -> Value<D> { self }
// }
// //TODO
// // impl<DTypes> IntoValue<DTypes> for () {
// //     type DType = bool;
// //     fn into_Value(self) -> Value<bool> { Value::Na }
// // }
// impl<DTypes, D: DataType<DTypes>> IntoValue<DTypes> for D {
//     type DType = D;
//     fn into_Value(self) -> Value<D> { Value::Exists(self) }
// }

/// Data vector containing the data for a single field (column) of an agnes data store.
///
/// To support NA types, a `FieldData` object is internally represented as a `Vec` of the
/// appropriate type, along with a bit mask to denote valid / missing values.
#[derive(Debug, Clone)]
pub struct FieldData<DTypes: DTypeList, T: DataType<DTypes>> {
    mask: BitVec,
    data: Vec<T>,
    _marker: PhantomData<DTypes>,
}
impl<DTypes, T> FieldData<DTypes, T>
    where DTypes: DTypeList,
          T: DataType<DTypes>
{
    /// Length of this data vector
    pub fn len(&self) -> usize {
        assert_eq!(self.mask.len(), self.data.len());
        self.data.len()
    }
    /// Get the value at the given index. Return `None` if `index` is out of bounds, or a `Value`
    /// Object with the value (or indicator that value is missing).
    pub fn get(&self, index: usize) -> Option<Value<&T>> {
        if index >= self.data.len() {
            None
        } else {
            if self.mask[index] {
                Some(Value::Exists(&self.data[index]))
            } else {
                Some(Value::Na)
            }
        }
    }
    fn set(&mut self, index: usize, value: Value<T>) -> bool {
        if index >= self.data.len() {
            false
        } else {
            match value {
                Value::Exists(value) => {
                    self.mask.set(index, true);
                    *self.data.get_mut(index).unwrap() = value;
                },
                Value::Na => {
                    self.mask.set(index, false);
                }
            }
            true
        }
    }
    /// Interpret `FieldData` as a `Vec` of `Value` objects.
    pub fn as_vec(&self) -> Vec<Value<&T>>
        where FieldData<DTypes, T>: DataIndex<DTypes, DType=T>
    {
        self.data.iter().enumerate().map(|(idx, value)| {
            if self.mask[idx] {
                Value::Exists(value)
            } else {
                Value::Na
            }
        }).collect()
    }

    // pub fn iter<'a>(&'a self) -> DataIterator<'a, T> where FieldData<DTypes, T>: DataIndex<T>
    // {
    //     DataIterator::new(self)
    // }
}
impl<DTypes, T> FieldData<DTypes, T>
    where DTypes: DTypeList,
          T: DataType<DTypes>
{
    /// Create new empty `FieldData` struct.
    pub fn new() -> FieldData<DTypes, T> {
        FieldData {
            data: vec![],
            mask: BitVec::new(),
            _marker: PhantomData,
        }
    }
    /// Create a `FieldData` struct from a vector of non-NA values. Resulting `FieldData` struct
    /// will have no `Value::Na` values.
    pub fn from_vec<U: Into<T>>(mut v: Vec<U>) -> FieldData<DTypes, T> {
        FieldData {
            mask: BitVec::from_elem(v.len(), true),
            data: v.drain(..).map(|value| value.into()).collect(),
            _marker: PhantomData
        }
    }
}
impl<DTypes, T> FieldData<DTypes, T>
    where DTypes: DTypeList,
          T: DataType<DTypes> + Default + Clone
{
    // /// Create new field data vector with single element.
    // pub fn new_with_elem(value: Value<T>) -> FieldData<DTypes, T> {
    //     match value {
    //         Value::Exists(v) => {
    //             FieldData {
    //                 data: vec!(v),
    //                 mask: BitVec::from_elem(1, true)
    //             }
    //         },
    //         Value::Na => {
    //             FieldData {
    //                 data: vec![T::default()],
    //                 mask: BitVec::from_elem(1, false)
    //             }
    //         }
    //     }
    // }
    /// Add a new value (or an indication of a missing one) to the data vector
    pub fn push_val(&mut self, value: Value<T>) {
        match value {
            Value::Exists(v) => {
                self.data.push(v);
                self.mask.push(true);
            },
            Value::Na => {
                self.data.push(T::default());
                self.mask.push(false);
            }
        }
    }
    pub fn push_ref(&mut self, value: Value<&T>) {
        match value {
            Value::Exists(v) => {
                self.data.push(v.clone());
                self.mask.push(true);
            },
            Value::Na => {
                self.data.push(T::default());
                self.mask.push(false)
            }
        }
    }
    /// Create a `FieldData` struct from a vector of field values.
    pub fn from_field_vec(mut v: Vec<Value<T>>) -> FieldData<DTypes, T> {
        let mut ret = FieldData::new();
        for elem in v.drain(..) {
            ret.push(elem);
        }
        ret
    }
}
impl<DTypes, T> FromIterator<Value<T>> for FieldData<DTypes, T>
    where T: DataType<DTypes> + Default + Clone,
          DTypes: DTypeList
{
    fn from_iter<I: IntoIterator<Item=Value<T>>>(iter: I) -> Self {
        let mut data = FieldData::new();
        for value in iter {
            data.push(value);
        }
        data
    }
}
impl<'a, DTypes, T> FromIterator<Value<&'a T>> for FieldData<DTypes, T>
    where T: 'a + DataType<DTypes> + Default + Clone,
          DTypes: DTypeList
{
    fn from_iter<I: IntoIterator<Item=Value<&'a T>>>(iter: I) -> Self {
        let mut data = FieldData::new();
        for value in iter {
            data.push(value.cloned());
        }
        data
    }
}
impl<DTypes, T> FromIterator<T> for FieldData<DTypes, T>
    where DTypes: DTypeList,
          T: DataType<DTypes>
{
    fn from_iter<I: IntoIterator<Item=T>>(iter: I) -> Self {
        let mut mask = BitVec::new();
        let mut data = vec![];
        for value in iter {
            mask.push(true);
            data.push(value.into());
        }
        FieldData {
            data,
            mask,
            _marker: PhantomData
        }
    }
}
impl<DTypes, T, U> From<Vec<U>> for FieldData<DTypes, T>
    where DTypes: DTypeList,
          T: DataType<DTypes>,
          U: Into<T>
{
    fn from(other: Vec<U>) -> FieldData<DTypes, T> {
        FieldData::from_vec(other)
    }
}

impl<DTypes, T> DataIndex<DTypes> for FieldData<DTypes, T>
    where DTypes: DTypeList,
          T: DataType<DTypes>
{
    type DType = T;

    fn get_datum(&self, idx: usize) -> error::Result<Value<&T>> {
        self.get(idx).ok_or(error::AgnesError::IndexError { index: idx, len: self.len() })
    }
    fn len(&self) -> usize {
        self.len()
    }
}
impl<DTypes, T> DataIndexMut<DTypes> for FieldData<DTypes, T>
    where DTypes: DTypeList,
          T: DataType<DTypes> + Default + Clone
{
    // fn set_datum(&mut self, idx: usize, value: Value<Self::DType>) -> error::Result<()> {
    //     if self.set(idx, value) {
    //         Ok(())
    //     } else {
    //         Err(error::AgnesError::IndexError { index: idx, len: self.len() })
    //     }
    // }
    fn push(&mut self, value: Value<Self::DType>) {
        self.push_val(value)
    }
}
// macro_rules! impl_field_data_index {
//     ($($ty:ty)*) => {$(
//         impl DataIndex<$ty> for FieldData<$ty> {
//             fn get_data(&self, idx: usize) -> error::Result<Value<&$ty>> {
//                 self.get(idx).ok_or(error::AgnesError::IndexError { index: idx, len: self.len() })
//             }
//             fn len(&self) -> usize {
//                 self.len()
//             }
//         }
//     )*}
// }
// impl_field_data_index!(u64 i64 String bool f64);

// impl<T: DataType> FieldData<DTypes, T> {
//     /// Apply a `MapFn` to this data vector at the specified index.
//     pub fn apply<F: MapFn>(&self, f: &mut F, idx: usize)
//         -> error::Result<<F as ApplyToDatum<T>>::Output>
//         where F: ApplyToDatum<T>
//     {
//         self.get(idx).map(|value| f.apply_to_datum(value))
//             .ok_or(error::AgnesError::IndexError { index: idx, len: self.len() })
//     }
// }

// macro_rules! impl_field_apply {
//     ($($apply_fn:tt; $dtype:ty)*) => {$(

// impl FieldApply for FieldData<$dtype> {
//     fn field_apply<F: FieldMapFn>(&self, f: &mut F) -> error::Result<F::Output> {
//         Ok(f.$apply_fn(self))
//     }
// }

//     )*}
// }
// impl_field_apply!(
//     apply_unsigned; u64
//     apply_signed;   i64
//     apply_text;     String
//     apply_boolean;  bool
//     apply_float;    f64
// );

impl<DTypes, T> Serialize for FieldData<DTypes, T>
    where DTypes: DTypeList,
          T: DataType<DTypes> + Serialize
{
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

impl<DTypes, T> IntoDataStore<DTypes> for FieldData<DTypes, T>
    where DTypes: DTypeList,
          DTypes::Storage: CreateStorage + TypeSelector<DTypes, T> + DTypeSelector<DTypes, T>,
          T: 'static + DataType<DTypes> + Default + Clone
{
    fn into_datastore<I: Into<FieldIdent>>(self, ident: I) -> error::Result<DataStore<DTypes>> {
        DataStore::empty().with_data_from_iter(ident, self.iter())
    }
}

/// Identifier for a field in the source.
#[derive(Debug, Clone)]
pub enum FieldIdent {
    /// Unnamed field identifier, using the field index in the source file.
    Index(usize),
    /// Field name in the source file
    Name(String)
}
impl FieldIdent {
    /// Produce a string representation of the field identifier. Either the name if
    /// of the `FieldIdent::Name` variant, or the string "Field #" if using the `FieldIdent::Index`
    /// variant.
    pub fn to_string(&self) -> String {
        match *self {
            FieldIdent::Index(i) => format!("Field {}", i),
            FieldIdent::Name(ref s) => s.clone(),
        }
    }
}
impl fmt::Display for FieldIdent {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "{}", self.to_string())
    }
}
impl PartialEq for FieldIdent {
    fn eq(&self, other: &FieldIdent) -> bool {
        self.to_string().eq(&other.to_string())
    }
}
impl Eq for FieldIdent {}
impl Hash for FieldIdent {
    fn hash<H>(&self, state: &mut H) where H: Hasher {
        self.to_string().hash(state)
    }
}

impl From<usize> for FieldIdent {
    fn from(src: usize) -> FieldIdent {
        FieldIdent::Index(src)
    }
}
impl<'a> From<&'a str> for FieldIdent {
    fn from(src: &'a str) -> FieldIdent {
        FieldIdent::Name(src.to_string())
    }
}
impl From<String> for FieldIdent {
    fn from(src: String) -> FieldIdent {
        FieldIdent::Name(src)
    }
}
impl<'a, T> From<&'a T> for FieldIdent where FieldIdent: From<T>, T: Clone {
    fn from(src: &'a T) -> FieldIdent {
        FieldIdent::from(src.clone())
    }
}

// /// Marker trait for types supported by Agnes data structures
// pub trait DataType: Serialize + Display + Debug + Clone {
//     /// Printable name of this data type (for debugging / printout purposes)
//     fn name() -> &'static str;
// }
// impl DataType for u64 { fn name() -> &'static str { "u64" } }
// impl DataType for i64 { fn name() -> &'static str { "i64" } }
// impl DataType for String { fn name() -> &'static str { "String" } }
// impl DataType for bool { fn name() -> &'static str { "bool" } }
// impl DataType for f64 { fn name() -> &'static str { "f64" } }
// impl DataType for u32 { fn name() -> &'static str { "u32" } }
// impl DataType for i32 { fn name() -> &'static str { "i32" } }
// impl DataType for f32 { fn name() -> &'static str { "f32" } }

// impl<'a, T> DataType for &'a T where T: DataType {
//     fn name() -> &'static str {
//         T::name()
//     }
// }

/// Trait to provide common hashing ability for all `DataType`s. Hashes floating point by
/// decoding into mantissa, exponent, sign integers and hashing those.
pub trait DataTypeHash {
    /// Hash type into `Hasher`. Should function similarly to function in `std::hash::Hash` trait.
    fn dt_hash<H: Hasher>(&self, state: &mut H);
}
macro_rules! impl_datatypehash {
    ($($dtype:ty)*) => {$(

impl DataTypeHash for $dtype { fn dt_hash<H: Hasher>(&self, state: &mut H) { self.hash(state); } }

    )*}
}
impl_datatypehash![u64 i64 String bool u32 i32];

//TODO: handle Eq-Hash inequalities (0.0 / -0.0 should be equal and hash to same)
impl DataTypeHash for f64 {
    fn dt_hash<H: Hasher>(&self, state: &mut H) {
        self.integer_decode().hash(state);
    }
}
impl DataTypeHash for f32 {
    fn dt_hash<H: Hasher>(&self, state: &mut H) {
        self.integer_decode().hash(state);
    }
}

impl<'a, T> DataTypeHash for &'a T where T: DataTypeHash {
    fn dt_hash<H: Hasher>(&self, state: &mut H) {
        (*self).dt_hash(state);
    }
}


// /// Common enum for a single value of any of the valid Agnes data types.
// #[derive(Debug, Clone, PartialOrd, PartialEq)]
// pub enum DtValue {
//     /// Unsigned integer value
//     Unsigned(u64),
//     /// Signed integer value
//     Signed(i64),
//     /// Text value
//     Text(String),
//     /// Boolean value
//     Boolean(bool),
//     /// Floating-point value
//     Float(f64),
// }
// impl From<u64> for DtValue {
//     fn from(orig: u64) -> DtValue { DtValue::Unsigned(orig) }
// }
// impl From<i64> for DtValue {
//     fn from(orig: i64) -> DtValue { DtValue::Signed(orig) }
// }
// impl From<String> for DtValue {
//     fn from(orig: String) -> DtValue { DtValue::Text(orig) }
// }
// impl From<bool> for DtValue {
//     fn from(orig: bool) -> DtValue { DtValue::Boolean(orig) }
// }
// impl From<f64> for DtValue {
//     fn from(orig: f64) -> DtValue { DtValue::Float(orig) }
// }
// impl fmt::Display for DtValue {
//     fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
//         match *self {
//             DtValue::Unsigned(u) => write!(f, "{}", u),
//             DtValue::Signed(i) => write!(f, "{}", i),
//             DtValue::Text(ref s) => write!(f, "{}", s),
//             DtValue::Boolean(b) => write!(f, "{}", b),
//             DtValue::Float(fl) => write!(f, "{}", fl),
//         }
//     }
// }

/// Possibly-renamed field identifier
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RFieldIdent {
    /// Original field identifier
    pub ident: FieldIdent,
    /// Renamed name (if exists)
    pub rename: Option<String>,
}
impl RFieldIdent {
    /// Produce a string representation of this `RFieldIdent`. Uses the renamed name (if exists),
    /// of the result of `to_string` on the underlying `FieldIdent`.
    pub fn to_string(&self) -> String {
        self.rename.clone().unwrap_or(self.ident.to_string())
    }
    /// Produce a new `FieldIdent` using the `rename` value of this `RFieldIdent` (if exists), or
    /// simply a clone of the underlying `FieldIdent`.
    pub fn to_renamed_field_ident(&self) -> FieldIdent {
        match self.rename {
            Some(ref renamed) => FieldIdent::Name(renamed.clone()),
            None              => self.ident.clone()
        }
    }
}

/// Field identifier along with an associated type.
#[derive(Debug, Clone)]
pub struct TFieldIdent<T> {
    /// Field identifier (name or original column number)
    pub ident: FieldIdent,
    /// Field type
    // pub ty: FieldType
    phantom: PhantomData<T>
}
impl<T> TFieldIdent<T> {
    /// Create a new typed field identifier
    pub fn new(ident: FieldIdent) -> TFieldIdent<T> {
        TFieldIdent {
            ident: ident,
            phantom: PhantomData
        }
    }
}

// macro_rules! fields {
//     ($($name:expr => $ty:expr),*) => {{
//         use $crate::field::TypedFieldIdent;

//         vec![$(
//             TypedFieldIdent::new(
//                 FieldIdent::Name($name.to_string()),
//                 $ty
//             )
//         ),*]
//     }}
// }
