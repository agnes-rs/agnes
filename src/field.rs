/*!
Data structures and implementations for fields.

Provides the [FieldData](struct.FieldData.html) struct for holding the data of a field and handling
missing values.
*/

use std::fmt;
use std::fmt::Debug;
use std::hash::{Hash, Hasher};
use std::iter::FromIterator;
use std::marker::PhantomData;

#[cfg(feature = "serialize")]
use serde::ser::{Serialize, SerializeSeq, Serializer};

use access::{DataIndex, DataIndexMut};
use bit_vec::BitVec;
use error;
use value::Value;

/// Data vector containing the data for a single field (column) of an agnes data store.
///
/// To support NA / missing values, a `FieldData` object is internally represented as a `Vec` of the
/// appropriate type, along with a bit mask to denote valid / missing values.
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct FieldData<T> {
    mask: Option<BitVec>,
    data: Vec<T>,
}
impl<T> FieldData<T> {
    /// Returns the length of this data vector.
    pub fn len(&self) -> usize {
        debug_assert!(self
            .mask
            .as_ref()
            .map_or(true, |mask| mask.len() == self.data.len()));
        self.data.len()
    }
    /// Returns `true` if this field contains no values.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    fn exists_at(&self, index: usize) -> bool {
        self.mask.as_ref().map_or(true, |mask| mask[index])
    }
    fn mask_set(&mut self, index: usize, value: bool) {
        if value {
            // if mask exists, set the `true` value, otherwise do nothing (since no mask means
            // we consider all values to exist already)
            self.mask.as_mut().map(|mask| mask.set(index, value));
        } else {
            // generate new mask if it doesn't exist, and set `false` value
            self.mask
                .get_or_insert(BitVec::from_elem(self.data.len(), true))
                .set(index, value);
        }
    }
    /// Get the value at the given index. Returns `None` if `index` is out of bounds, or a
    /// `Value` enum.
    pub fn get(&self, index: usize) -> Option<Value<&T>> {
        if index >= self.data.len() {
            None
        } else if self.exists_at(index) {
            Some(Value::Exists(&self.data[index]))
        } else {
            Some(Value::Na)
        }
    }
    /// Take the value at the given index. Returns `None` if `index` is out of bounds, or a
    /// [Value](enum.Value.html) enum. Replaces the taken value with `Value::Na`.
    pub fn take(&mut self, index: usize) -> Option<Value<T>>
    where
        T: Default,
    {
        if index >= self.data.len() {
            None
        } else if self.exists_at(index) {
            self.data.push(T::default());
            let value = self.data.swap_remove(index);
            self.mask_set(index, false);
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
                if self.exists_at(idx) {
                    Value::Exists(value)
                } else {
                    Value::Na
                }
            })
            .collect()
    }
    /// Create a new `FieldData` from a slice. Does not clone or reallocate the contained data (but
    /// does allocate the bit mask). Resulting `FieldData` struct will have no `Value::Na` values.
    pub fn from_boxed_slice(orig: Box<[T]>) -> Self {
        FieldData {
            mask: None,
            data: <[_]>::into_vec(orig),
        }
    }
}
impl<T> Default for FieldData<T> {
    fn default() -> FieldData<T> {
        FieldData {
            data: vec![],
            mask: None,
        }
    }
}
impl<T> FieldData<T> {
    /// Create a `FieldData` struct from a vector of non-NA values. Resulting `FieldData` struct
    /// will have no `Value::Na` values.
    pub fn from_vec<U: Into<T>>(mut v: Vec<U>) -> FieldData<T> {
        FieldData {
            mask: None,
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
                // if mask exists (which means there are NA values), then add a true to the end
                self.mask.as_mut().map(|mask| mask.push(true));
            }
            Value::Na => {
                let prev_len = self.data.len();
                self.data.push(T::default());
                // either get or create mask, and add a false to the end
                self.mask
                    .get_or_insert_with(|| BitVec::from_elem(prev_len, true))
                    .push(false);
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
                // if mask exists (which means there are NA values), then add a true to the end
                self.mask.as_mut().map(|mask| mask.push(true));
            }
            Value::Na => {
                let prev_len = self.data.len();
                self.data.push(T::default());
                // either get or create mask, and add a false to the end
                self.mask
                    .get_or_insert_with(|| BitVec::from_elem(prev_len, true))
                    .push(false);
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
        let mut data = vec![];
        for value in iter {
            data.push(value);
        }
        FieldData { data, mask: None }
    }
}
impl<T> From<Vec<T>> for FieldData<T> {
    fn from(other: Vec<T>) -> FieldData<T> {
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
    where
        T: Default,
    {
        self.take(idx).ok_or(error::AgnesError::IndexError {
            index: idx,
            len: self.len(),
        })
    }
}

#[cfg(feature = "serialize")]
impl<T> Serialize for FieldData<T>
where
    T: Serialize,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut seq = serializer.serialize_seq(Some(self.data.len()))?;
        match self.mask {
            Some(ref mask) => {
                for (mask, elem) in mask.iter().zip(self.data.iter()) {
                    if mask {
                        seq.serialize_element(elem)?;
                    } else {
                        seq.serialize_element("null")?;
                    }
                }
            }
            None => {
                for elem in self.data.iter() {
                    seq.serialize_element(elem)?;
                }
            }
        }
        seq.end()
    }
}
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json;

    #[test]
    fn field_serialize() {
        let field: FieldData<f64> = vec![5.0f64, 3.4, -1.3, 5.2, 6.0, -126.9].into();
        assert_eq!(
            serde_json::to_string(&field).unwrap(),
            "[5.0,3.4,-1.3,5.2,6.0,-126.9]"
        );
    }
}
