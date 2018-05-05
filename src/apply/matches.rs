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

// impl<S: Selector, T> Matches<S, i64> for T where T: ApplyToElem<S> {
//     fn matches(&self, select: S, target: i64) -> Result<bool> {
//         self.apply_to_elem(
//             MatchesFnSigned { value: MaybeNa::Exists(&target) },
//             &select
//         )
//     }
// }
// impl<S: Selector, T> Matches<S, String> for T where T: ApplyToElem<S> {
//     fn matches(&self, select: S, target: String) -> Result<bool> {
//         self.apply_to_elem(
//             MatchesFnText { value: MaybeNa::Exists(&target) },
//             &select
//         )
//     }
// }
// impl<S: Selector, T> Matches<S, bool> for T where T: ApplyToElem<S> {
//     fn matches(&self, select: S, target: bool) -> Result<bool> {
//         self.apply_to_elem(
//             MatchesFnBoolean { value: MaybeNa::Exists(&target) },
//             &select
//         )
//     }
// }
// impl<S: Selector, T> Matches<S, f64> for T where T: ApplyToElem<S> {
//     fn matches(&self, select: S, target: f64) -> Result<bool> {
//         self.apply_to_elem(
//             MatchesFnFloat { value: MaybeNa::Exists(&target) },
//             &select
//         )
//     }
// }

fn test_pred<T: DataType, F: Fn(&T) -> bool>(value: MaybeNa<&T>, f: &mut F) -> bool {
    match value {
        MaybeNa::Exists(&ref val) => (f)(val),
        MaybeNa::Na => false,
    }
}



/// `FieldFn` function for finding an index set of unsigned integer values of a field that match
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
// impl<F: Fn(&u64) -> bool> FieldFn for FilterFnUnsigned<F> {
//     type Output = Vec<usize>;
//     fn apply_unsigned<T: DataIndex<u64>>(&mut self, field: &T) -> Vec<usize> {
//         (0..field.len()).filter(|&idx| {
            // match field.get_data(idx).unwrap() {
            //     MaybeNa::Exists(&ref val) => (self.f)(val),
            //     MaybeNa::Na => false,
            // }
//         }).collect()
//     }
//     fn apply_signed<T: DataIndex<i64>>(&mut self, _: &T) -> Vec<usize> { vec![] }
//     fn apply_text<T: DataIndex<String>>(&mut self, _: &T) -> Vec<usize> { vec![] }
//     fn apply_boolean<T: DataIndex<bool>>(&mut self, _: &T) -> Vec<usize> { vec![] }
//     fn apply_float<T: DataIndex<f64>>(&mut self, _: &T) -> Vec<usize> { vec![] }
// }
/// `FieldFn` function for finding an index set of signed integer values of a field that match
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


// impl<F: Fn(&i64) -> bool> FieldFn for FilterFnSigned<F> {
//     type Output = Vec<usize>;
//     fn apply_unsigned<T: DataIndex<u64>>(&mut self, _: &T) -> Vec<usize> { vec![] }
//     fn apply_signed<T: DataIndex<i64>>(&mut self, field: &T) -> Vec<usize> {
//         (0..field.len()).filter(|&idx| {
//             match field.get_data(idx).unwrap() {
//                 MaybeNa::Exists(&ref val) => (self.f)(val),
//                 MaybeNa::Na => false,
//             }
//         }).collect()
//     }
//     fn apply_text<T: DataIndex<String>>(&mut self, _: &T) -> Vec<usize> { vec![] }
//     fn apply_boolean<T: DataIndex<bool>>(&mut self, _: &T) -> Vec<usize> { vec![] }
//     fn apply_float<T: DataIndex<f64>>(&mut self, _: &T) -> Vec<usize> { vec![] }
// }
/// `FieldFn` function for finding an index set of text values of a field that match
/// a predicate.
pub struct FilterFnText<F: Fn(&String) -> bool> { f: F }
impl<F: Fn(&String) -> bool> MapFn for FilterFnText<F> {
    type Output = bool;
    fn apply_unsigned(&mut self, _: MaybeNa<&u64>) -> bool { false }
    fn apply_signed(&mut self, _: MaybeNa<&i64>) -> bool { false }
    fn apply_text(&mut self, value: MaybeNa<&String>) -> bool { test_pred(value, &mut self.f) }
    fn apply_boolean(&mut self, _: MaybeNa<&bool>) -> bool { false }
    fn apply_float(&mut self, _: MaybeNa<&f64>) -> bool { false }
}


// impl<F: Fn(&String) -> bool> FieldFn for FilterFnText<F> {
//     type Output = Vec<usize>;
//     fn apply_unsigned<T: DataIndex<u64>>(&mut self, _: &T) -> Vec<usize> { vec![] }
//     fn apply_signed<T: DataIndex<i64>>(&mut self, _: &T) -> Vec<usize> { vec![] }
//     fn apply_text<T: DataIndex<String>>(&mut self, field: &T) -> Vec<usize> {
//         (0..field.len()).filter(|&idx| {
//             match field.get_data(idx).unwrap() {
//                 MaybeNa::Exists(&ref val) => (self.f)(val),
//                 MaybeNa::Na => false,
//             }
//         }).collect()
//     }
//     fn apply_boolean<T: DataIndex<bool>>(&mut self, _: &T) -> Vec<usize> { vec![] }
//     fn apply_float<T: DataIndex<f64>>(&mut self, _: &T) -> Vec<usize> { vec![] }
// }
/// `FieldFn` function for finding an index set of boolean values of a field that match
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


// impl<F: Fn(&bool) -> bool> FieldFn for FilterFnBoolean<F> {
//     type Output = Vec<usize>;
//     fn apply_unsigned<T: DataIndex<u64>>(&mut self, _: &T) -> Vec<usize> { vec![] }
//     fn apply_signed<T: DataIndex<i64>>(&mut self, _: &T) -> Vec<usize> { vec![] }
//     fn apply_text<T: DataIndex<String>>(&mut self, _: &T) -> Vec<usize> { vec![] }
//     fn apply_boolean<T: DataIndex<bool>>(&mut self, field: &T) -> Vec<usize> {
//         (0..field.len()).filter(|&idx| {
//             match field.get_data(idx).unwrap() {
//                 MaybeNa::Exists(&ref val) => (self.f)(val),
//                 MaybeNa::Na => false,
//             }
//         }).collect()
//     }
//     fn apply_float<T: DataIndex<f64>>(&mut self, _: &T) -> Vec<usize> { vec![] }
// }
/// `FieldFn` function for finding an index set of floating-point values of a field that match
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


// impl<F: Fn(&f64) -> bool> FieldFn for FilterFnFloat<F> {
//     type Output = Vec<usize>;
//     fn apply_unsigned<T: DataIndex<u64>>(&mut self, _: &T) -> Vec<usize> { vec![] }
//     fn apply_signed<T: DataIndex<i64>>(&mut self, _: &T) -> Vec<usize> { vec![] }
//     fn apply_text<T: DataIndex<String>>(&mut self, _: &T) -> Vec<usize> { vec![] }
//     fn apply_boolean<T: DataIndex<bool>>(&mut self, _: &T) -> Vec<usize> { vec![] }
//     fn apply_float<T: DataIndex<f64>>(&mut self, field: &T) -> Vec<usize> {
//         (0..field.len()).filter(|&idx| {
//             match field.get_data(idx).unwrap() {
//                 MaybeNa::Exists(&ref val) => (self.f)(val),
//                 MaybeNa::Na => false,
//             }
//         }).collect()
//     }
// }


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

// impl<'a, U> GetFilter<u64> for U where U: ApplyToElem<FieldIndexSelector<'a>> + DataIndex<u64> {
//     fn get_filter<F: Fn(&u64) -> bool>(&self, select: FieldSelector, pred: F)
//         -> Result<Vec<usize>>
//     {
//         for i in 0..self.len() {
//             self.apply_to_elem(
//                 FilterFnUnsigned { f: pred },
//                 FieldIndexSelector { field: select.index(), index: i }
//             )
//         }
//     }
// }
// impl<'a, U> GetFilter<i64> for U where U: ApplyToElem<FieldIndexSelector<'a>> + DataIndex<i64> {
//     fn get_filter<F: Fn(&i64) -> bool>(&self, select: FieldSelector, pred: F)
//         -> Result<Vec<usize>>
//     {
//         self.apply_to_elem(
//             FilterFnSigned { f: pred },
//             select
//         )
//     }
// }
// impl<'a, U> GetFilter<String> for U
//     where U: ApplyToElem<FieldIndexSelector<'a>> + DataIndex<String>
// {
//     fn get_filter<F: Fn(&String) -> bool>(&self, select: FieldSelector, pred: F)
//         -> Result<Vec<usize>>
//     {
//         self.apply_to_elem(
//             FilterFnText { f: pred },
//             select
//         )
//     }
// }
// impl<'a, U> GetFilter<bool> for U where U: ApplyToElem<FieldIndexSelector<'a>> + DataIndex<bool> {
//     fn get_filter<F: Fn(&bool) -> bool>(&self, select: FieldSelector, pred: F)
//         -> Result<Vec<usize>>
//     {
//         self.apply_to_elem(
//             FilterFnBoolean { f: pred },
//             select
//         )
//     }
// }
// impl<'a, U> GetFilter<f64> for U where U: ApplyToElem<FieldIndexSelector<'a>> + DataIndex<f64> {
//     fn get_filter<F: Fn(&f64) -> bool>(&self, select: FieldSelector, pred: F)
//         -> Result<Vec<usize>>
//     {
//         self.apply_to_elem(
//             FilterFnFloat { f: pred },
//             select
//         )
//     }
// }


/*

/// `MapFn` function for matching all unsigned integer values of a field against a predicate.
pub struct MatchesAllFnUnsigned<F: Fn(&u64) -> bool> {
    f: F,
}
impl<F: Fn(&u64) -> bool> MapFn for MatchesAllFnUnsigned<F> {
    type Output = bool;
    fn apply_unsigned<T: DataIndex<u64>>(&mut self, field: &T) -> bool {
        (0..field.len()).all(|idx| match field.get_data(idx).unwrap() {
            MaybeNa::Exists(&ref val) => (self.f)(val),
            MaybeNa::Na => false
        })
    }
    fn apply_signed<T: DataIndex<i64>>(&mut self, _: &T) -> bool { false }
    fn apply_text<T: DataIndex<String>>(&mut self, _: &T) -> bool { false }
    fn apply_boolean<T: DataIndex<bool>>(&mut self, _: &T) -> bool { false }
    fn apply_float<T: DataIndex<f64>>(&mut self, _: &T) -> bool { false }
}

/// `MapFn` function for matching all signed integer values of a field against a predicate.
pub struct MatchesAllFnSigned<F: Fn(&i64) -> bool> {
    f: F,
}
impl<F: Fn(&i64) -> bool> MapFn for MatchesAllFnSigned<F> {
    type Output = bool;
    fn apply_unsigned<T: DataIndex<u64>>(&mut self, _: &T) -> bool { false }
    fn apply_signed<T: DataIndex<i64>>(&mut self, field: &T) -> bool {
        (0..field.len()).all(|idx| match field.get_data(idx).unwrap() {
            MaybeNa::Exists(&ref val) => (self.f)(val),
            MaybeNa::Na => false
        })
    }
    fn apply_text<T: DataIndex<String>>(&mut self, _: &T) -> bool { false }
    fn apply_boolean<T: DataIndex<bool>>(&mut self, _: &T) -> bool { false }
    fn apply_float<T: DataIndex<f64>>(&mut self, _: &T) -> bool { false }
}

/// `MapFn` function for matching all text values of a field against a predicate.
pub struct MatchesAllFnText<F: Fn(&String) -> bool> {
    f: F,
}
impl<F: Fn(&String) -> bool> MapFn for MatchesAllFnText<F> {
    type Output = bool;
    fn apply_unsigned<T: DataIndex<u64>>(&mut self, _: &T) -> bool { false }
    fn apply_signed<T: DataIndex<i64>>(&mut self, _: &T) -> bool { false }
    fn apply_text<T: DataIndex<String>>(&mut self, field: &T) -> bool {
        (0..field.len()).all(|idx| match field.get_data(idx).unwrap() {
            MaybeNa::Exists(&ref val) => (self.f)(val),
            MaybeNa::Na => false
        })
    }
    fn apply_boolean<T: DataIndex<bool>>(&mut self, _: &T) -> bool { false }
    fn apply_float<T: DataIndex<f64>>(&mut self, _: &T) -> bool { false }
}

/// `MapFn` function for matching all boolean values of a field against a predicate.
pub struct MatchesAllFnBoolean<F: Fn(&bool) -> bool> {
    f: F,
}
impl<F: Fn(&bool) -> bool> MapFn for MatchesAllFnBoolean<F> {
    type Output = bool;
    fn apply_unsigned<T: DataIndex<u64>>(&mut self, _: &T) -> bool { false }
    fn apply_signed<T: DataIndex<i64>>(&mut self, _: &T) -> bool { false }
    fn apply_text<T: DataIndex<String>>(&mut self, _: &T) -> bool { false }
    fn apply_boolean<T: DataIndex<bool>>(&mut self, field: &T) -> bool {
        (0..field.len()).all(|idx| match field.get_data(idx).unwrap() {
            MaybeNa::Exists(&ref val) => (self.f)(val),
            MaybeNa::Na => false
        })
    }
    fn apply_float<T: DataIndex<f64>>(&mut self, _: &T) -> bool { false }
}

/// `MapFn` function for matching all floating-point values of a field against a predicate.
pub struct MatchesAllFnFloat<F: Fn(&f64) -> bool> {
    f: F,
}
impl<F: Fn(&f64) -> bool> MapFn for MatchesAllFnFloat<F> {
    type Output = bool;
    fn apply_unsigned<T: DataIndex<u64>>(&mut self, _: &T) -> bool { false }
    fn apply_signed<T: DataIndex<i64>>(&mut self, _: &T) -> bool { false }
    fn apply_text<T: DataIndex<String>>(&mut self, _: &T) -> bool { false }
    fn apply_boolean<T: DataIndex<bool>>(&mut self, _: &T) -> bool { false }
    fn apply_float<T: DataIndex<f64>>(&mut self, field: &T) -> bool {
        (0..field.len()).all(|idx| match field.get_data(idx).unwrap() {
            MaybeNa::Exists(&ref val) => (self.f)(val),
            MaybeNa::Na => false
        })
    }
}*/

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





// impl<S: Selector, T> MatchesAll<S, u64> for T where T: ApplyToElem<S> {
//     fn matches_all<F: Fn(&u64) -> bool>(&self, select: S, pred: F) -> Result<bool> {
//         self.apply_to_field(
//             MatchesAllFnUnsigned { f: pred },
//             select
//         )
//     }
// }
// impl<S: Selector, T> MatchesAll<S, i64> for T where T: ApplyToField<S> {
//     fn matches_all<F: Fn(&i64) -> bool>(&self, select: S, pred: F) -> Result<bool> {
//         self.apply_to_field(
//             MatchesAllFnSigned { f: pred },
//             select
//         )
//     }
// }
// impl<S: Selector, T> MatchesAll<S, String> for T where T: ApplyToField<S> {
//     fn matches_all<F: Fn(&String) -> bool>(&self, select: S, pred: F) -> Result<bool> {
//         self.apply_to_field(
//             MatchesAllFnText { f: pred },
//             select
//         )
//     }
// }
// impl<S: Selector, T> MatchesAll<S, bool> for T where T: ApplyToField<S> {
//     fn matches_all<F: Fn(&bool) -> bool>(&self, select: S, pred: F) -> Result<bool> {
//         self.apply_to_field(
//             MatchesAllFnBoolean { f: pred },
//             select
//         )
//     }
// }
// impl<S: Selector, T> MatchesAll<S, f64> for T where T: ApplyToField<S> {
//     fn matches_all<F: Fn(&f64) -> bool>(&self, select: S, pred: F) -> Result<bool> {
//         self.apply_to_field(
//             MatchesAllFnFloat { f: pred },
//             select
//         )
//     }
// }
