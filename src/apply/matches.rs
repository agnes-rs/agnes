use masked::MaybeNa;
use apply::{ApplyToElem, ApplyTo, MapFn};
use error::Result;
use field::FieldIdent;
use view::DataView;
use frame::DataFrame;
use field::DataType;

/// `MapFn` function for matching unsigned integer values.
pub struct MatchesFnUnsigned<'a> {
    value: MaybeNa<&'a u64>
}
impl<'a> MapFn for MatchesFnUnsigned<'a> {
    type Output = bool;
    fn apply_unsigned(&mut self, value: MaybeNa<&u64>) -> bool { self.value == value }
    fn apply_signed(&mut self, _: MaybeNa<&i64>) -> bool { false }
    fn apply_text(&mut self, _: MaybeNa<&String>) -> bool { false }
    fn apply_boolean(&mut self, _: MaybeNa<&bool>) -> bool { false }
    fn apply_float(&mut self, _: MaybeNa<&f64>) -> bool { false }
}

/// `MapFn` function for matching signed integer values.
pub struct MatchesFnSigned<'a> {
    value: MaybeNa<&'a i64>
}
impl<'a> MapFn for MatchesFnSigned<'a> {
    type Output = bool;
    fn apply_unsigned(&mut self, _: MaybeNa<&u64>) -> bool { false }
    fn apply_signed(&mut self, value: MaybeNa<&i64>) -> bool { self.value == value }
    fn apply_text(&mut self, _: MaybeNa<&String>) -> bool { false }
    fn apply_boolean(&mut self, _: MaybeNa<&bool>) -> bool { false }
    fn apply_float(&mut self, _: MaybeNa<&f64>) -> bool { false }
}

/// `MapFn` function for matching text values.
pub struct MatchesFnText<'a> {
    value: MaybeNa<&'a String>
}
impl<'a> MapFn for MatchesFnText<'a> {
    type Output = bool;
    fn apply_unsigned(&mut self, _: MaybeNa<&u64>) -> bool { false }
    fn apply_signed(&mut self, _: MaybeNa<&i64>) -> bool { false }
    fn apply_text(&mut self, value: MaybeNa<&String>) -> bool { self.value == value }
    fn apply_boolean(&mut self, _: MaybeNa<&bool>) -> bool { false }
    fn apply_float(&mut self, _: MaybeNa<&f64>) -> bool { false }
}

/// `MapFn` function for matching boolean values.
pub struct MatchesFnBoolean<'a> {
    value: MaybeNa<&'a bool>
}
impl<'a> MapFn for MatchesFnBoolean<'a> {
    type Output = bool;
    fn apply_unsigned(&mut self, _: MaybeNa<&u64>) -> bool { false }
    fn apply_signed(&mut self, _: MaybeNa<&i64>) -> bool { false }
    fn apply_text(&mut self, _: MaybeNa<&String>) -> bool { false }
    fn apply_boolean(&mut self, value: MaybeNa<&bool>) -> bool { self.value == value }
    fn apply_float(&mut self, _: MaybeNa<&f64>) -> bool { false }
}

/// `MapFn` function for matching floating-point values.
pub struct MatchesFnFloat<'a> {
    value: MaybeNa<&'a f64>
}
impl<'a> MapFn for MatchesFnFloat<'a> {
    type Output = bool;
    fn apply_unsigned(&mut self, _: MaybeNa<&u64>) -> bool { false }
    fn apply_signed(&mut self, _: MaybeNa<&i64>) -> bool { false }
    fn apply_text(&mut self, _: MaybeNa<&String>) -> bool { false }
    fn apply_boolean(&mut self, _: MaybeNa<&bool>) -> bool { false }
    fn apply_float(&mut self, value: MaybeNa<&f64>) -> bool { self.value == value }
}

/// Helper trait / implementations for matching a value. Returns `true` if the selected element
/// matches the provided target value.
pub trait Matches<T> {
    /// Returns `true` if the element specified with the `Selector` matches the provided target
    /// value.
    fn matches(&self, target: T, ident: &FieldIdent, idx: usize) -> Result<bool>;
}


macro_rules! impl_dataview_matches {
    ($($dtype:ty, $match_fn:ident),*) => {$(

impl Matches<$dtype> for DataView {
    fn matches(&self, target: $dtype, ident: &FieldIdent, idx: usize) -> Result<bool> {
        self.apply_to_elem(
            &mut $match_fn { value: MaybeNa::Exists(&target) },
            ident,
            idx
        )
    }
}

    )*}
}

impl_dataview_matches!(
    u64,    MatchesFnUnsigned,
    i64,    MatchesFnSigned,
    String, MatchesFnText,
    bool,   MatchesFnBoolean,
    f64,    MatchesFnFloat
);

fn test_pred<T: DataType, F: Fn(&T) -> bool>(value: MaybeNa<&T>, f: &mut F) -> bool {
    match value {
        MaybeNa::Exists(&ref val) => (f)(val),
        MaybeNa::Na => false,
    }
}



/// `MapFn` function for finding an index set of unsigned integer values of a field that match
/// a predicate.
pub struct FilterFnUnsigned<F: Fn(&u64) -> bool> { f: F }
impl<F: Fn(&u64) -> bool> MapFn for FilterFnUnsigned<F> {
    type Output = bool;
    fn apply_unsigned(&mut self, value: MaybeNa<&u64>) -> bool { test_pred(value, &mut self.f) }
    fn apply_signed(&mut self, _: MaybeNa<&i64>) -> bool { false }
    fn apply_text(&mut self, _: MaybeNa<&String>) -> bool { false }
    fn apply_boolean(&mut self, _: MaybeNa<&bool>) -> bool { false }
    fn apply_float(&mut self, _: MaybeNa<&f64>) -> bool { false }
}

/// `MapFn` function for finding an index set of signed integer values of a field that match
/// a predicate.
pub struct FilterFnSigned<F: Fn(&i64) -> bool> { f: F }
impl<F: Fn(&i64) -> bool> MapFn for FilterFnSigned<F> {
    type Output = bool;
    fn apply_unsigned(&mut self, _: MaybeNa<&u64>) -> bool { false }
    fn apply_signed(&mut self, value: MaybeNa<&i64>) -> bool { test_pred(value, &mut self.f) }
    fn apply_text(&mut self, _: MaybeNa<&String>) -> bool { false }
    fn apply_boolean(&mut self, _: MaybeNa<&bool>) -> bool { false }
    fn apply_float(&mut self, _: MaybeNa<&f64>) -> bool { false }
}

/// `MapFn` function for finding an index set of text values of a field that match a predicate.
pub struct FilterFnText<F: Fn(&String) -> bool> { f: F }
impl<F: Fn(&String) -> bool> MapFn for FilterFnText<F> {
    type Output = bool;
    fn apply_unsigned(&mut self, _: MaybeNa<&u64>) -> bool { false }
    fn apply_signed(&mut self, _: MaybeNa<&i64>) -> bool { false }
    fn apply_text(&mut self, value: MaybeNa<&String>) -> bool { test_pred(value, &mut self.f) }
    fn apply_boolean(&mut self, _: MaybeNa<&bool>) -> bool { false }
    fn apply_float(&mut self, _: MaybeNa<&f64>) -> bool { false }
}

/// `MapFn function for finding an index set of boolean values of a field that match
/// a predicate.
pub struct FilterFnBoolean<F: Fn(&bool) -> bool> { f: F }
impl<F: Fn(&bool) -> bool> MapFn for FilterFnBoolean<F> {
    type Output = bool;
    fn apply_unsigned(&mut self, _: MaybeNa<&u64>) -> bool { false }
    fn apply_signed(&mut self, _: MaybeNa<&i64>) -> bool { false }
    fn apply_text(&mut self, _: MaybeNa<&String>) -> bool { false }
    fn apply_boolean(&mut self, value: MaybeNa<&bool>) -> bool { test_pred(value, &mut self.f) }
    fn apply_float(&mut self, _: MaybeNa<&f64>) -> bool { false }
}

/// `MapFn` function for finding an index set of floating-point values of a field that match
/// a predicate.
pub struct FilterFnFloat<F: Fn(&f64) -> bool> { f: F }
impl<F: Fn(&f64) -> bool> MapFn for FilterFnFloat<F> {
    type Output = bool;
    fn apply_unsigned(&mut self, _: MaybeNa<&u64>) -> bool { false }
    fn apply_signed(&mut self, _: MaybeNa<&i64>) -> bool { false }
    fn apply_text(&mut self, _: MaybeNa<&String>) -> bool { false }
    fn apply_boolean(&mut self, _: MaybeNa<&bool>) -> bool { false }
    fn apply_float(&mut self, value: MaybeNa<&f64>) -> bool { test_pred(value, &mut self.f) }
}

/// Helper trait / implementations for finding an index set of values in a field that match a
/// predicate. Returns a vector of indices of all elements in the field that pass the predicate.
pub trait GetFilter<T> {
    /// Returns vector of indices of all elements in the field specified with the `Selector` that
    /// pass the predicate.
    fn get_filter<F: Fn(&T) -> bool>(&self, pred: F, ident: &FieldIdent) -> Result<Vec<usize>>;
}

macro_rules! impl_dataframe_get_filter {
    ($dtype:ty, $filter_fn:ident) => {

impl GetFilter<$dtype> for DataFrame {
    fn get_filter<F: Fn(&$dtype) -> bool>(&self, pred: F, ident: &FieldIdent)
        -> Result<Vec<usize>>
    {
        Ok(self.apply_to(
            &mut $filter_fn { f: pred },
            ident
        )?.iter().enumerate()
          .filter_map(|(idx, &b)| if b { Some(idx) } else { None }).collect())
    }
}

    }
}
impl_dataframe_get_filter!(u64,    FilterFnUnsigned);
impl_dataframe_get_filter!(i64,    FilterFnSigned);
impl_dataframe_get_filter!(String, FilterFnText);
impl_dataframe_get_filter!(bool,   FilterFnBoolean);
impl_dataframe_get_filter!(f64,    FilterFnFloat);

/// Helper trait / implementations for matching a predicate to a field. Returns `true` if the
/// provided predicate returns true for all elements in the field.
pub trait MatchesAll<T> {
    /// Returns `true` if the all elements in the field specified with the `Selector` pass the
    /// predicate.
    fn matches_all<F: Fn(&T) -> bool>(&self, pred: F, field: &FieldIdent) -> Result<bool>;
}

macro_rules! impl_dataview_matches_all {
    ($dtype:ty, $filter_fn:ident) => {

impl MatchesAll<$dtype> for DataView
{
    fn matches_all<F: Fn(&$dtype) -> bool>(&self, pred: F, ident: &FieldIdent)
        -> Result<bool>
    {
        Ok(self.apply_to(
            &mut $filter_fn { f: pred },
            ident
        )?.iter().all(|&b| b))
    }
}

    }
}
impl_dataview_matches_all!(u64,    FilterFnUnsigned);
impl_dataview_matches_all!(i64,    FilterFnSigned);
impl_dataview_matches_all!(String, FilterFnText);
impl_dataview_matches_all!(bool,   FilterFnBoolean);
impl_dataview_matches_all!(f64,    FilterFnFloat);
