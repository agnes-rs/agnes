/*!
Provides the [Value](enum.Value.html) enum which provides the `Option`-like interface for
interacting with individual, possibly missing, values.
*/

use std::cmp::Ordering;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::mem;
use std::ops::{Add, Div, Mul, Sub};

#[cfg(feature = "serialize")]
use serde::ser::{Serialize, Serializer};

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
    /// Unwrap a `Value`, returning the contained value or a default.
    pub fn unwrap_or(self, def: T) -> T {
        match self {
            Value::Na => def,
            Value::Exists(t) => t,
        }
    }
    /// Unwrap a `Value`, returning the contained value or a default computed from a closure.
    pub fn unwrap_or_else<F>(self, f: F) -> T
    where
        F: FnOnce() -> T,
    {
        match self {
            Value::Na => f(),
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
    pub fn map<U, F>(self, f: F) -> Value<U>
    where
        F: FnOnce(T) -> U,
    {
        match self {
            Value::Exists(val) => Value::Exists(f(val)),
            Value::Na => Value::Na,
        }
    }
    /// Applies function `f` if this `Value` exists, or returns a default `def` if not
    pub fn map_or<U, F>(self, def: U, f: F) -> U
    where
        F: FnOnce(T) -> U,
    {
        match self {
            Value::Exists(val) => f(val),
            Value::Na => def,
        }
    }
    /// Applies function `f` if this `Value` exists, or computes a default using `def` if not
    pub fn map_or_else<U, D, F>(self, def: D, f: F) -> U
    where
        D: FnOnce() -> U,
        F: FnOnce(T) -> U,
    {
        match self {
            Value::Exists(val) => f(val),
            Value::Na => def(),
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

/// Small utility macro to construct a [Value](field/enum.Value.html) enum with a reference to
/// an existing value. Typically only used for tests.
#[macro_export]
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

macro_rules! impl_value_op {
    ($trait_name:tt $trait_fn:tt) => {
        impl<T, U> $trait_name<Value<U>> for Value<T>
        where
            T: $trait_name<U>,
        {
            type Output = Value<<T as $trait_name<U>>::Output>;

            fn $trait_fn(self, rhs: Value<U>) -> Self::Output {
                match (self, rhs) {
                    (Value::Exists(l), Value::Exists(r)) => Value::Exists(l.$trait_fn(r)),
                    _ => Value::Na,
                }
            }
        }
    };
}
impl_value_op![Add add];
impl_value_op![Sub sub];
impl_value_op![Mul mul];
impl_value_op![Div div];

#[cfg(feature = "serialize")]
impl<'a, T> Serialize for Value<&'a T>
where
    T: Serialize,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match *self {
            Value::Exists(ref val) => serializer.serialize_some(val),
            Value::Na => serializer.serialize_none(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json;

    #[test]
    fn value_serialize() {
        let val = 6.4f64;
        let value = Value::Exists(&val);
        assert_eq!(serde_json::to_string(&value).unwrap(), "6.4");

        let value: Value<&f64> = Value::Na;
        assert_eq!(serde_json::to_string(&value).unwrap(), "null");
    }
}
