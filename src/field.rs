/*!
Data structures and implementations for field information, both identifiers (`FieldIdent`) and
field storage (`FieldData` and `Value`).
*/

use std::cmp::Ordering;
use std::fmt;
use std::fmt::Debug;
use std::hash::{Hash, Hasher};
use std::iter::FromIterator;
use std::marker::PhantomData;
use std::mem;

#[cfg(serialize)]
use serde::ser::{Serialize, SerializeSeq, Serializer};

use bit_vec::BitVec;
use store::DataRef;
// use store::{IntoDataStore, DataStore, WithDataFromIter};
use access::{DataIndex, DataIndexMut};
use error;

/// Marker trait for types supporting common traits (such as being displayed).
// pub trait DataType: Debug + Display {}
// impl<T> DataType for T where T: Debug + Display {}

/// (Possibly missing) data value container.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Value<T> {
    /// Indicates a missing (NA) value.
    Na,
    /// Indicates an existing value.
    Exists(T),
}
impl<T> Value<T> {
    /// Unwrap a `Value`, revealing the data contained within. Panics if called on an `Na` value.
    pub fn unwrap(self) -> T {
        match self {
            Value::Na => {
                panic!("unwrap() called on NA value");
            }
            Value::Exists(t) => t,
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
    pub fn as_ref(&self) -> Value<&T> {
        match *self {
            Value::Exists(ref val) => Value::Exists(&val),
            Value::Na => Value::Na,
        }
    }
    /// Applies function `f` if this `Value` exists.
    pub fn map<U, F: FnMut(T) -> U>(self, mut f: F) -> Value<U> {
        match self {
            Value::Exists(val) => Value::Exists(f(val)),
            Value::Na => Value::Na,
        }
    }
}
impl<'a, T: Clone> Value<&'a T> {
    /// Create a owner `Value` out of a reference-holding `Value` using `clone()`.
    pub fn cloned(self) -> Value<T> {
        match self {
            Value::Exists(t) => Value::Exists(t.clone()),
            Value::Na => Value::Na,
        }
    }
}

macro_rules! valref {
    ($value:expr) => {
        Value::Exists(&$value)
    };
}

impl<'a, T> PartialEq<T> for Value<&'a T>
where
    T: PartialEq<T>,
{
    fn eq(&self, other: &T) -> bool {
        match *self {
            Value::Exists(value) => value.eq(other),
            Value::Na => false,
        }
    }
}
impl<'a, T> PartialOrd<T> for Value<&'a T>
where
    T: PartialOrd<T>,
{
    fn partial_cmp(&self, other: &T) -> Option<Ordering> {
        match *self {
            Value::Exists(value) => value.partial_cmp(other),
            Value::Na => None,
        }
    }
}

impl<T> fmt::Display for Value<T>
where
    T: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Value::Exists(ref t) => write!(f, "{}", t),
            Value::Na => write!(f, "NA"),
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
impl<T> From<T> for Value<T> {
    fn from(orig: T) -> Value<T> {
        Value::Exists(orig)
    }
}
impl<'a, T> From<Value<&'a T>> for Value<T>
where
    T: 'a + Clone,
{
    fn from(orig: Value<&'a T>) -> Value<T> {
        orig.cloned()
    }
}

impl<T> Into<Option<T>> for Value<T> {
    fn into(self) -> Option<T> {
        match self {
            Value::Exists(value) => Some(value),
            Value::Na => None,
        }
    }
}
impl<T> From<Option<T>> for Value<T> {
    fn from(orig: Option<T>) -> Value<T> {
        match orig {
            Some(value) => Value::Exists(value),
            None => Value::Na,
        }
    }
}

/// Data vector containing the data for a single field (column) of an agnes data store.
///
/// To support NA types, a `FieldData` object is internally represented as a `Vec` of the
/// appropriate type, along with a bit mask to denote valid / missing values.
#[derive(Debug, Clone, PartialEq)]
pub struct FieldData<T> {
    mask: BitVec,
    data: Vec<T>,
}
impl<T> FieldData<T> {
    /// Returns the length of this data vector.
    pub fn len(&self) -> usize {
        assert_eq!(self.mask.len(), self.data.len());
        self.data.len()
    }
    /// Returns `true` if this field contains no values.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    /// Get the value at the given index. Return `None` if `index` is out of bounds, or a `Value`
    /// Object with the value (or indicator that value is missing).
    pub fn get(&self, index: usize) -> Option<Value<&T>> {
        if index >= self.data.len() {
            None
        } else if self.mask[index] {
            Some(Value::Exists(&self.data[index]))
        } else {
            Some(Value::Na)
        }
    }
    pub fn take(&mut self, index: usize) -> Option<Value<T>>
    where T: Default
    {
        if index >= self.data.len() {
            None
        } else if self.mask[index] {
            self.data.push(T::default());
            let value = self.data.swap_remove(index);
            self.mask.set(index, false);
            Some(Value::Exists(value))
        } else {
            Some(Value::Na)
        }
    }
    /// Interpret `FieldData` as a `Vec` of `Value` objects.
    pub fn as_vec(&self) -> Vec<Value<&T>>
    where
        FieldData<T>: DataIndex<DType = T>,
    {
        self.data
            .iter()
            .enumerate()
            .map(|(idx, value)| {
                if self.mask[idx] {
                    Value::Exists(value)
                } else {
                    Value::Na
                }
            })
            .collect()
    }
}
impl<T> Default for FieldData<T>
// where T: DataType,
{
    fn default() -> FieldData<T> {
        FieldData {
            data: vec![],
            mask: BitVec::new(),
        }
    }
}
impl<T> FieldData<T>
// where T: DataType
{
    /// Create a `FieldData` struct from a vector of non-NA values. Resulting `FieldData` struct
    /// will have no `Value::Na` values.
    pub fn from_vec<U: Into<T>>(mut v: Vec<U>) -> FieldData<T> {
        FieldData {
            mask: BitVec::from_elem(v.len(), true),
            data: v.drain(..).map(|value| value.into()).collect(),
        }
    }
}
impl<T> FieldData<T>
where
    T: Debug + Default,
{
    /// Add a new value (or an indication of a missing one) to the data vector.
    pub fn push_val(&mut self, value: Value<T>) {
        match value {
            Value::Exists(v) => {
                self.data.push(v);
                self.mask.push(true);
            }
            Value::Na => {
                self.data.push(T::default());
                self.mask.push(false);
            }
        }
    }
}
impl<T> FieldData<T>
where
    T: Debug + Default + Clone,
{
    /// Add a new value (passed by reference) to the data vector.
    pub fn push_ref(&mut self, value: Value<&T>) {
        match value {
            Value::Exists(v) => {
                self.data.push(v.clone());
                self.mask.push(true);
            }
            Value::Na => {
                self.data.push(T::default());
                self.mask.push(false)
            }
        }
    }
    /// Create a `FieldData` struct from a vector of field values.
    pub fn from_field_vec(mut v: Vec<Value<T>>) -> FieldData<T> {
        let mut ret = FieldData::default();
        for elem in v.drain(..) {
            ret.push(elem);
        }
        ret
    }
}
impl<T> FromIterator<Value<T>> for FieldData<T>
where
    T: Debug + Default,
{
    fn from_iter<I: IntoIterator<Item = Value<T>>>(iter: I) -> Self {
        let mut data = FieldData::default();
        for value in iter {
            data.push(value);
        }
        data
    }
}
impl<'a, T> FromIterator<Value<&'a T>> for FieldData<T>
where
    T: 'a + Debug + Default + Clone,
{
    fn from_iter<I: IntoIterator<Item = Value<&'a T>>>(iter: I) -> Self {
        let mut data = FieldData::default();
        for value in iter {
            data.push(value.cloned());
        }
        data
    }
}
impl<T> FromIterator<T> for FieldData<T> {
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        let mut mask = BitVec::new();
        let mut data = vec![];
        for value in iter {
            mask.push(true);
            data.push(value);
        }
        FieldData { data, mask }
    }
}
impl<T, U> From<Vec<U>> for FieldData<T>
where
    U: Into<T>,
{
    fn from(other: Vec<U>) -> FieldData<T> {
        FieldData::from_vec(other)
    }
}

impl<T> DataIndex for FieldData<T>
where
    T: Debug,
{
    type DType = T;

    fn get_datum(&self, idx: usize) -> error::Result<Value<&T>> {
        self.get(idx).ok_or(error::AgnesError::IndexError {
            index: idx,
            len: self.len(),
        })
    }
    fn len(&self) -> usize {
        self.len()
    }
}
impl<T> DataIndexMut for FieldData<T>
where
    T: Debug + Default,
{
    fn push(&mut self, value: Value<Self::DType>) {
        self.push_val(value)
    }
    fn take_datum(&mut self, idx: usize) -> error::Result<Value<T>>
    where T: Default {
        self.take(idx).ok_or(error::AgnesError::IndexError {
            index: idx,
            len: self.len(),
        })
    }
}

impl<T> DataIndex for DataRef<T>
where
    FieldData<T>: DataIndex<DType=T>,
    T: Debug
{
    type DType = T;

    fn get_datum(&self, idx: usize) -> error::Result<Value<&T>> {
        <FieldData<T> as DataIndex>::get_datum(&self.0, idx)
    }
    fn len(&self) -> usize {
        <FieldData<T> as DataIndex>::len(&self.0)
    }
}

#[cfg(serialize)]
impl<T> Serialize for FieldData<T>
where
    T: Serialize,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
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

// impl<T> IntoDataStore for FieldData<T>
//     where T: 'static + DataType + Default + Clone
// {
//     fn into_datastore<I: Into<FieldIdent>>(self, ident: I) -> error::Result<DataStore<DTypes>> {
//         DataStore::empty().with_data_from_iter(ident, self.iter())
//     }
// }

/// Identifier for a field in the source.
#[derive(Debug, Clone)]
pub enum FieldIdent {
    /// Unnamed field identifier, using the field index in the source file.
    Index(usize),
    /// Field name in the source file
    Name(String),
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
    fn hash<H>(&self, state: &mut H)
    where
        H: Hasher,
    {
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
impl<'a, T> From<&'a T> for FieldIdent
where
    FieldIdent: From<T>,
    T: Clone,
{
    fn from(src: &'a T) -> FieldIdent {
        FieldIdent::from(src.clone())
    }
}

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
        self.rename
            .clone()
            .unwrap_or_else(|| self.ident.to_string())
    }
    /// Produce a new `FieldIdent` using the `rename` value of this `RFieldIdent` (if exists), or
    /// simply a clone of the underlying `FieldIdent`.
    pub fn to_renamed_field_ident(&self) -> FieldIdent {
        match self.rename {
            Some(ref renamed) => FieldIdent::Name(renamed.clone()),
            None => self.ident.clone(),
        }
    }
}

/// Field identifier along with an associated type.
#[derive(Debug, Clone)]
pub struct TFieldIdent<T> {
    /// Field identifier (name or original column number)
    pub ident: FieldIdent,
    /// Field type
    phantom: PhantomData<T>,
}
impl<T> TFieldIdent<T> {
    /// Create a new typed field identifier
    pub fn new(ident: FieldIdent) -> TFieldIdent<T> {
        TFieldIdent {
            ident,
            phantom: PhantomData,
        }
    }
}
