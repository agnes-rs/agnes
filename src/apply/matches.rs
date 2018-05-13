use masked::MaybeNa;
use apply::{ApplyToElem, ApplyTo, MapFn};
use error::Result;
use field::FieldIdent;
use view::DataView;
use frame::DataFrame;
use field::DataType;

map_fn![
    /// `MapFn` function for matching unsigned integer values.
    pub MatchesFnUnsigned<('a)> {
        type Output = bool;
        value: MaybeNa<&'a u64>
    }
    fn unsigned(self, value) { self.value == value }
    fn [signed, text, boolean, float](self, _) { false }
];

map_fn![
    /// `MapFn` function for matching signed integer values.
    pub MatchesFnSigned<('a)> {
        type Output = bool;
        value: MaybeNa<&'a i64>
    }
    fn signed(self, value) { self.value == value }
    fn [unsigned, text, boolean, float](self, _) { false }
];

map_fn![
    /// `MapFn` function for matching text values.
    pub MatchesFnText<('a)> {
        type Output = bool;
        value: MaybeNa<&'a String>
    }
    fn text(self, value) { self.value == value }
    fn [signed, unsigned, boolean, float](self, _) { false }
];

map_fn![
    /// `MapFn` function for matching boolean values.
    pub MatchesFnBoolean<('a)> {
        type Output = bool;
        value: MaybeNa<&'a bool>
    }
    fn boolean(self, value) { self.value == value }
    fn [unsigned, signed, text, float](self, _) { false }
];

map_fn![
    /// `MapFn` function for matching floating-point values.
    pub MatchesFnFloat<('a)> {
        type Output = bool;
        value: MaybeNa<&'a f64>
    }
    fn float(self, value) { self.value == value }
    fn [unsigned, signed, text, boolean](self, _) { false }
];

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

map_fn![
    /// `MapFn` function for finding an index set of unsigned integer values of a field that match
    /// a predicate.
    pub FilterFnUnsigned<(F)> where (F: Fn(&u64) -> bool) {
        type Output = bool;
        f: F
    }
    fn unsigned(self, value) { test_pred(value, &mut self.f) }
    fn [signed, text, boolean, float](self, _) { false }
];

map_fn![
    /// `MapFn` function for finding an index set of signed integer values of a field that match
    /// a predicate.
    pub FilterFnSigned<(F)> where (F: Fn(&i64) -> bool) {
        type Output = bool;
        f: F
    }
    fn signed(self, value) { test_pred(value, &mut self.f) }
    fn [unsigned, text, boolean, float](self, _) { false }
];

map_fn![
    /// `MapFn` function for finding an index set of text values of a field that match a predicate.
    pub FilterFnText<(F)> where (F: Fn(&String) -> bool) {
        type Output = bool;
        f: F
    }
    fn text(self, value) { test_pred(value, &mut self.f) }
    fn [unsigned, signed, boolean, float](self, _) { false }
];

map_fn![
    /// `MapFn function for finding an index set of boolean values of a field that match
    /// a predicate.
    pub FilterFnBoolean<(F)> where (F: Fn(&bool) -> bool) {
        type Output = bool;
        f: F
    }
    fn boolean(self, value) { test_pred(value, &mut self.f) }
    fn [unsigned, signed, text, float](self, _) { false }
];

map_fn![
    /// `MapFn` function for finding an index set of floating-point values of a field that match
    /// a predicate.
    pub FilterFnFloat<(F)> where (F: Fn(&f64) -> bool) {
        type Output = bool;
        f: F
    }
    fn float(self, value) { test_pred(value, &mut self.f) }
    fn [unsigned, signed, text, boolean](self, _) { false }
];

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
