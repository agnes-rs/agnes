use masked::MaybeNa;
use apply::{ElemFn, Selector, FieldFn, ApplyToElem, DataIndex, ApplyToField};
use error::Result;

/// `ElemFn` function for matching unsigned integer values.
pub struct MatchesFnUnsigned<'a> {
    value: MaybeNa<&'a u64>
}
impl<'a> ElemFn for MatchesFnUnsigned<'a> {
    type Output = bool;
    fn apply_unsigned(&mut self, value: MaybeNa<&u64>) -> bool { self.value == value }
    fn apply_signed(&mut self, _: MaybeNa<&i64>) -> bool { false }
    fn apply_text(&mut self, _: MaybeNa<&String>) -> bool { false }
    fn apply_boolean(&mut self, _: MaybeNa<&bool>) -> bool { false }
    fn apply_float(&mut self, _: MaybeNa<&f64>) -> bool { false }
}

/// `ElemFn` function for matching signed integer values.
pub struct MatchesFnSigned<'a> {
    value: MaybeNa<&'a i64>
}
impl<'a> ElemFn for MatchesFnSigned<'a> {
    type Output = bool;
    fn apply_unsigned(&mut self, _: MaybeNa<&u64>) -> bool { false }
    fn apply_signed(&mut self, value: MaybeNa<&i64>) -> bool { self.value == value }
    fn apply_text(&mut self, _: MaybeNa<&String>) -> bool { false }
    fn apply_boolean(&mut self, _: MaybeNa<&bool>) -> bool { false }
    fn apply_float(&mut self, _: MaybeNa<&f64>) -> bool { false }
}

/// `ElemFn` function for matching text values.
pub struct MatchesFnText<'a> {
    value: MaybeNa<&'a String>
}
impl<'a> ElemFn for MatchesFnText<'a> {
    type Output = bool;
    fn apply_unsigned(&mut self, _: MaybeNa<&u64>) -> bool { false }
    fn apply_signed(&mut self, _: MaybeNa<&i64>) -> bool { false }
    fn apply_text(&mut self, value: MaybeNa<&String>) -> bool { self.value == value }
    fn apply_boolean(&mut self, _: MaybeNa<&bool>) -> bool { false }
    fn apply_float(&mut self, _: MaybeNa<&f64>) -> bool { false }
}

/// `ElemFn` function for matching boolean values.
pub struct MatchesFnBoolean<'a> {
    value: MaybeNa<&'a bool>
}
impl<'a> ElemFn for MatchesFnBoolean<'a> {
    type Output = bool;
    fn apply_unsigned(&mut self, _: MaybeNa<&u64>) -> bool { false }
    fn apply_signed(&mut self, _: MaybeNa<&i64>) -> bool { false }
    fn apply_text(&mut self, _: MaybeNa<&String>) -> bool { false }
    fn apply_boolean(&mut self, value: MaybeNa<&bool>) -> bool { self.value == value }
    fn apply_float(&mut self, _: MaybeNa<&f64>) -> bool { false }
}

/// `ElemFn` function for matching floating-point values.
pub struct MatchesFnFloat<'a> {
    value: MaybeNa<&'a f64>
}
impl<'a> ElemFn for MatchesFnFloat<'a> {
    type Output = bool;
    fn apply_unsigned(&mut self, _: MaybeNa<&u64>) -> bool { false }
    fn apply_signed(&mut self, _: MaybeNa<&i64>) -> bool { false }
    fn apply_text(&mut self, _: MaybeNa<&String>) -> bool { false }
    fn apply_boolean(&mut self, _: MaybeNa<&bool>) -> bool { false }
    fn apply_float(&mut self, value: MaybeNa<&f64>) -> bool { self.value == value }
}

/// Helper trait / implementations for matching a value. Returns `true` if the selected element
/// matches the provided target value.
pub trait Matches<S: Selector, T> {
    /// Returns `true` if the element specified with the `Selector` matches the provided target
    /// value.
    fn matches(&self, select: S, target: T) -> Result<bool>;
}
impl<S: Selector, T> Matches<S, u64> for T where T: ApplyToElem<S> {
    fn matches(&self, select: S, target: u64) -> Result<bool> {
        self.apply_to_elem(
            MatchesFnUnsigned { value: MaybeNa::Exists(&target) },
            select
        )
    }
}
impl<S: Selector, T> Matches<S, i64> for T where T: ApplyToElem<S> {
    fn matches(&self, select: S, target: i64) -> Result<bool> {
        self.apply_to_elem(
            MatchesFnSigned { value: MaybeNa::Exists(&target) },
            select
        )
    }
}
impl<S: Selector, T> Matches<S, String> for T where T: ApplyToElem<S> {
    fn matches(&self, select: S, target: String) -> Result<bool> {
        self.apply_to_elem(
            MatchesFnText { value: MaybeNa::Exists(&target) },
            select
        )
    }
}
impl<S: Selector, T> Matches<S, bool> for T where T: ApplyToElem<S> {
    fn matches(&self, select: S, target: bool) -> Result<bool> {
        self.apply_to_elem(
            MatchesFnBoolean { value: MaybeNa::Exists(&target) },
            select
        )
    }
}
impl<S: Selector, T> Matches<S, f64> for T where T: ApplyToElem<S> {
    fn matches(&self, select: S, target: f64) -> Result<bool> {
        self.apply_to_elem(
            MatchesFnFloat { value: MaybeNa::Exists(&target) },
            select
        )
    }
}

/// `FieldFn` function for finding an index set of unsigned integer values of a field that match
/// a predicate.
pub struct FilterFnUnsigned<F: Fn(&u64) -> bool> {
    f: F,
}
impl<F: Fn(&u64) -> bool> FieldFn for FilterFnUnsigned<F> {
    type Output = Vec<usize>;
    fn apply_unsigned<T: DataIndex<u64>>(&mut self, field: &T) -> Vec<usize> {
        (0..field.len()).filter(|&idx| {
            match field.get_data(idx).unwrap() {
                MaybeNa::Exists(&ref val) => (self.f)(val),
                MaybeNa::Na => false,
            }
        }).collect()
    }
    fn apply_signed<T: DataIndex<i64>>(&mut self, _: &T) -> Vec<usize> { vec![] }
    fn apply_text<T: DataIndex<String>>(&mut self, _: &T) -> Vec<usize> { vec![] }
    fn apply_boolean<T: DataIndex<bool>>(&mut self, _: &T) -> Vec<usize> { vec![] }
    fn apply_float<T: DataIndex<f64>>(&mut self, _: &T) -> Vec<usize> { vec![] }
}
/// `FieldFn` function for finding an index set of signed integer values of a field that match
/// a predicate.
pub struct FilterFnSigned<F: Fn(&i64) -> bool> {
    f: F,
}
impl<F: Fn(&i64) -> bool> FieldFn for FilterFnSigned<F> {
    type Output = Vec<usize>;
    fn apply_unsigned<T: DataIndex<u64>>(&mut self, _: &T) -> Vec<usize> { vec![] }
    fn apply_signed<T: DataIndex<i64>>(&mut self, field: &T) -> Vec<usize> {
        (0..field.len()).filter(|&idx| {
            match field.get_data(idx).unwrap() {
                MaybeNa::Exists(&ref val) => (self.f)(val),
                MaybeNa::Na => false,
            }
        }).collect()
    }
    fn apply_text<T: DataIndex<String>>(&mut self, _: &T) -> Vec<usize> { vec![] }
    fn apply_boolean<T: DataIndex<bool>>(&mut self, _: &T) -> Vec<usize> { vec![] }
    fn apply_float<T: DataIndex<f64>>(&mut self, _: &T) -> Vec<usize> { vec![] }
}
/// `FieldFn` function for finding an index set of text values of a field that match
/// a predicate.
pub struct FilterFnText<F: Fn(&String) -> bool> {
    f: F,
}
impl<F: Fn(&String) -> bool> FieldFn for FilterFnText<F> {
    type Output = Vec<usize>;
    fn apply_unsigned<T: DataIndex<u64>>(&mut self, _: &T) -> Vec<usize> { vec![] }
    fn apply_signed<T: DataIndex<i64>>(&mut self, _: &T) -> Vec<usize> { vec![] }
    fn apply_text<T: DataIndex<String>>(&mut self, field: &T) -> Vec<usize> {
        (0..field.len()).filter(|&idx| {
            match field.get_data(idx).unwrap() {
                MaybeNa::Exists(&ref val) => (self.f)(val),
                MaybeNa::Na => false,
            }
        }).collect()
    }
    fn apply_boolean<T: DataIndex<bool>>(&mut self, _: &T) -> Vec<usize> { vec![] }
    fn apply_float<T: DataIndex<f64>>(&mut self, _: &T) -> Vec<usize> { vec![] }
}
/// `FieldFn` function for finding an index set of boolean values of a field that match
/// a predicate.
pub struct FilterFnBoolean<F: Fn(&bool) -> bool> {
    f: F,
}
impl<F: Fn(&bool) -> bool> FieldFn for FilterFnBoolean<F> {
    type Output = Vec<usize>;
    fn apply_unsigned<T: DataIndex<u64>>(&mut self, _: &T) -> Vec<usize> { vec![] }
    fn apply_signed<T: DataIndex<i64>>(&mut self, _: &T) -> Vec<usize> { vec![] }
    fn apply_text<T: DataIndex<String>>(&mut self, _: &T) -> Vec<usize> { vec![] }
    fn apply_boolean<T: DataIndex<bool>>(&mut self, field: &T) -> Vec<usize> {
        (0..field.len()).filter(|&idx| {
            match field.get_data(idx).unwrap() {
                MaybeNa::Exists(&ref val) => (self.f)(val),
                MaybeNa::Na => false,
            }
        }).collect()
    }
    fn apply_float<T: DataIndex<f64>>(&mut self, _: &T) -> Vec<usize> { vec![] }
}
/// `FieldFn` function for finding an index set of floating-point values of a field that match
/// a predicate.
pub struct FilterFnFloat<F: Fn(&f64) -> bool> {
    f: F,
}
impl<F: Fn(&f64) -> bool> FieldFn for FilterFnFloat<F> {
    type Output = Vec<usize>;
    fn apply_unsigned<T: DataIndex<u64>>(&mut self, _: &T) -> Vec<usize> { vec![] }
    fn apply_signed<T: DataIndex<i64>>(&mut self, _: &T) -> Vec<usize> { vec![] }
    fn apply_text<T: DataIndex<String>>(&mut self, _: &T) -> Vec<usize> { vec![] }
    fn apply_boolean<T: DataIndex<bool>>(&mut self, _: &T) -> Vec<usize> { vec![] }
    fn apply_float<T: DataIndex<f64>>(&mut self, field: &T) -> Vec<usize> {
        (0..field.len()).filter(|&idx| {
            match field.get_data(idx).unwrap() {
                MaybeNa::Exists(&ref val) => (self.f)(val),
                MaybeNa::Na => false,
            }
        }).collect()
    }
}

/// Helper trait / implementations for finding an index set of values in a field that match a
/// predicate. Returns a vector of indices of all elements in the field that pass the predicate.
pub trait GetFilter<S: Selector, T> {
    /// Returns vector of indices of all elements in the field specified with the `Selector` that
    /// pass the predicate.
    fn get_filter<F: Fn(&T) -> bool>(&self, select: S, pred: F) -> Result<Vec<usize>>;
}
impl<S: Selector, T> GetFilter<S, u64> for T where T: ApplyToField<S> {
    fn get_filter<F: Fn(&u64) -> bool>(&self, select: S, pred: F) -> Result<Vec<usize>> {
        self.apply_to_field(
            FilterFnUnsigned { f: pred },
            select
        )
    }
}
impl<S: Selector, T> GetFilter<S, i64> for T where T: ApplyToField<S> {
    fn get_filter<F: Fn(&i64) -> bool>(&self, select: S, pred: F) -> Result<Vec<usize>> {
        self.apply_to_field(
            FilterFnSigned { f: pred },
            select
        )
    }
}
impl<S: Selector, T> GetFilter<S, String> for T where T: ApplyToField<S> {
    fn get_filter<F: Fn(&String) -> bool>(&self, select: S, pred: F) -> Result<Vec<usize>> {
        self.apply_to_field(
            FilterFnText { f: pred },
            select
        )
    }
}
impl<S: Selector, T> GetFilter<S, bool> for T where T: ApplyToField<S> {
    fn get_filter<F: Fn(&bool) -> bool>(&self, select: S, pred: F) -> Result<Vec<usize>> {
        self.apply_to_field(
            FilterFnBoolean { f: pred },
            select
        )
    }
}
impl<S: Selector, T> GetFilter<S, f64> for T where T: ApplyToField<S> {
    fn get_filter<F: Fn(&f64) -> bool>(&self, select: S, pred: F) -> Result<Vec<usize>> {
        self.apply_to_field(
            FilterFnFloat { f: pred },
            select
        )
    }
}


/// `FieldFn` function for matching all unsigned integer values of a field against a predicate.
pub struct MatchesAllFnUnsigned<F: Fn(&u64) -> bool> {
    f: F,
}
impl<F: Fn(&u64) -> bool> FieldFn for MatchesAllFnUnsigned<F> {
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

/// `FieldFn` function for matching all signed integer values of a field against a predicate.
pub struct MatchesAllFnSigned<F: Fn(&i64) -> bool> {
    f: F,
}
impl<F: Fn(&i64) -> bool> FieldFn for MatchesAllFnSigned<F> {
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

/// `FieldFn` function for matching all text values of a field against a predicate.
pub struct MatchesAllFnText<F: Fn(&String) -> bool> {
    f: F,
}
impl<F: Fn(&String) -> bool> FieldFn for MatchesAllFnText<F> {
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

/// `FieldFn` function for matching all boolean values of a field against a predicate.
pub struct MatchesAllFnBoolean<F: Fn(&bool) -> bool> {
    f: F,
}
impl<F: Fn(&bool) -> bool> FieldFn for MatchesAllFnBoolean<F> {
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

/// `FieldFn` function for matching all floating-point values of a field against a predicate.
pub struct MatchesAllFnFloat<F: Fn(&f64) -> bool> {
    f: F,
}
impl<F: Fn(&f64) -> bool> FieldFn for MatchesAllFnFloat<F> {
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
}

/// Helper trait / implementations for matching a predicate to a field. Returns `true` if the
/// provided predicate returns true for all elements in the field.
pub trait MatchesAll<S: Selector, T> {
    /// Returns `true` if the all elements in the field specified with the `Selector` pass the
    /// predicate.
    fn matches_all<F: Fn(&T) -> bool>(&self, select: S, pred: F) -> Result<bool>;
}
impl<S: Selector, T> MatchesAll<S, u64> for T where T: ApplyToField<S> {
    fn matches_all<F: Fn(&u64) -> bool>(&self, select: S, pred: F) -> Result<bool> {
        self.apply_to_field(
            MatchesAllFnUnsigned { f: pred },
            select
        )
    }
}
impl<S: Selector, T> MatchesAll<S, i64> for T where T: ApplyToField<S> {
    fn matches_all<F: Fn(&i64) -> bool>(&self, select: S, pred: F) -> Result<bool> {
        self.apply_to_field(
            MatchesAllFnSigned { f: pred },
            select
        )
    }
}
impl<S: Selector, T> MatchesAll<S, String> for T where T: ApplyToField<S> {
    fn matches_all<F: Fn(&String) -> bool>(&self, select: S, pred: F) -> Result<bool> {
        self.apply_to_field(
            MatchesAllFnText { f: pred },
            select
        )
    }
}
impl<S: Selector, T> MatchesAll<S, bool> for T where T: ApplyToField<S> {
    fn matches_all<F: Fn(&bool) -> bool>(&self, select: S, pred: F) -> Result<bool> {
        self.apply_to_field(
            MatchesAllFnBoolean { f: pred },
            select
        )
    }
}
impl<S: Selector, T> MatchesAll<S, f64> for T where T: ApplyToField<S> {
    fn matches_all<F: Fn(&f64) -> bool>(&self, select: S, pred: F) -> Result<bool> {
        self.apply_to_field(
            MatchesAllFnFloat { f: pred },
            select
        )
    }
}
