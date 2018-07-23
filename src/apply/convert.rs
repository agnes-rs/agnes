use std::marker::PhantomData;

use field::{DataType};
use apply::mapfn::MapFn;
use masked::MaybeNa;

/// Conversion trait for converting between datatypes.
pub trait DtFrom<T> {
    /// Convert into this type.
    fn dt_from(orig: T) -> Self;
}

impl DtFrom<u64> for u64 {
    fn dt_from(orig: u64) -> u64 {
        orig
    }
}
impl DtFrom<i64> for u64 {
    fn dt_from(orig: i64) -> u64 {
        if orig < 0 { 0 } else { orig as u64 }
    }
}
impl DtFrom<String> for u64 {
    fn dt_from(orig: String) -> u64 {
        orig.parse().expect(&format!("failure parsing {} as u64", orig))
    }
}
impl DtFrom<bool> for u64 {
    fn dt_from(orig: bool) -> u64 {
        if orig { 1 } else { 0 }
    }
}
impl DtFrom<f64> for u64 {
    fn dt_from(orig: f64) -> u64 {
        if orig < 0.0 { 0 } else { orig as u64 }
    }
}

impl DtFrom<u64> for i64 {
    fn dt_from(orig: u64) -> i64 {
        orig as i64
    }
}
impl DtFrom<i64> for i64 {
    fn dt_from(orig: i64) -> i64 {
        orig
    }
}
impl DtFrom<String> for i64 {
    fn dt_from(orig: String) -> i64 {
        orig.parse().expect(&format!("failure parsing {} as i64", orig))
    }
}
impl DtFrom<bool> for i64 {
    fn dt_from(orig: bool) -> i64 {
        if orig { 1 } else { 0 }
    }
}
impl DtFrom<f64> for i64 {
    fn dt_from(orig: f64) -> i64 {
        orig as i64
    }
}

impl DtFrom<u64> for String {
    fn dt_from(orig: u64) -> String {
        format!("{}", orig)
    }
}
impl DtFrom<i64> for String {
    fn dt_from(orig: i64) -> String {
        format!("{}", orig)
    }
}
impl DtFrom<String> for String {
    fn dt_from(orig: String) -> String {
        orig
    }
}
impl DtFrom<bool> for String {
    fn dt_from(orig: bool) -> String {
        format!("{}", orig)
    }
}
impl DtFrom<f64> for String {
    fn dt_from(orig: f64) -> String {
        format!("{}", orig)
    }
}

impl DtFrom<u64> for bool {
    fn dt_from(orig: u64) -> bool {
        orig > 0
    }
}
impl DtFrom<i64> for bool {
    fn dt_from(orig: i64) -> bool {
        orig != 0
    }
}
impl DtFrom<String> for bool {
    fn dt_from(orig: String) -> bool {
        match &orig.to_lowercase()[..] {
            "true" | "t" | "1" | "yes" => true,
            "false" | "f" | "0" | "no" => false,
            _ => panic!(format!("failure parsing {} as bool", orig))
        }
    }
}
impl DtFrom<bool> for bool {
    fn dt_from(orig: bool) -> bool {
        orig
    }
}
impl DtFrom<f64> for bool {
    fn dt_from(orig: f64) -> bool {
        orig != 0.0
    }
}

impl DtFrom<u64> for f64 {
    fn dt_from(orig: u64) -> f64 {
        orig as f64
    }
}
impl DtFrom<i64> for f64 {
    fn dt_from(orig: i64) -> f64 {
        orig as f64
    }
}
impl DtFrom<String> for f64 {
    fn dt_from(orig: String) -> f64 {
        orig.parse().expect(&format!("failure parsing {} as f64", orig))
    }
}
impl DtFrom<bool> for f64 {
    fn dt_from(orig: bool) -> f64 {
        if orig { 1.0 } else { 0.0 }
    }
}
impl DtFrom<f64> for f64 {
    fn dt_from(orig: f64) -> f64 {
        orig
    }
}

impl<'a, T: Clone, U: DtFrom<T>> DtFrom<&'a T> for U {
    fn dt_from(orig: &'a T) -> U {
        U::dt_from(orig.clone())
    }
}

map_fn![
    /// `MapFn` for conversion into a new data type.
    pub ConvertFn<(T)> where (T: DataType + DtFrom<u64> + DtFrom<i64> + DtFrom<String>
        + DtFrom<bool> + DtFrom<f64>)
    {
        type Output = MaybeNa<T>;
        phantom: PhantomData<T>,
    }
    fn all(self, value) {
        value.map(T::dt_from)
    }
];
impl<T> ConvertFn<T> {
    /// Create a new conversion `MapFn`.
    pub fn new() -> ConvertFn<T> {
        ConvertFn { phantom: PhantomData }
    }
}
