use masked::MaybeNa;
use apply::{ElemFn, Selector, FieldFn, ApplyToElem, DataIndex, ApplyToField};

/// `ElemFn` function for assertions of unsigned integer values.
pub struct AssertFnUnsigned<'a> {
    value: MaybeNa<&'a u64>
}
impl<'a> ElemFn for AssertFnUnsigned<'a> {
    type Output = bool;
    fn apply_unsigned(&mut self, value: MaybeNa<&u64>) -> bool { self.value == value }
    fn apply_signed(&mut self, _: MaybeNa<&i64>) -> bool { false }
    fn apply_text(&mut self, _: MaybeNa<&String>) -> bool { false }
    fn apply_boolean(&mut self, _: MaybeNa<&bool>) -> bool { false }
    fn apply_float(&mut self, _: MaybeNa<&f64>) -> bool { false }
}

/// `ElemFn` function for assertions of signed integer values.
pub struct AssertFnSigned<'a> {
    value: MaybeNa<&'a i64>
}
impl<'a> ElemFn for AssertFnSigned<'a> {
    type Output = bool;
    fn apply_unsigned(&mut self, _: MaybeNa<&u64>) -> bool { false }
    fn apply_signed(&mut self, value: MaybeNa<&i64>) -> bool { self.value == value }
    fn apply_text(&mut self, _: MaybeNa<&String>) -> bool { false }
    fn apply_boolean(&mut self, _: MaybeNa<&bool>) -> bool { false }
    fn apply_float(&mut self, _: MaybeNa<&f64>) -> bool { false }
}

/// `ElemFn` function for assertions of text values.
pub struct AssertFnText<'a> {
    value: MaybeNa<&'a String>
}
impl<'a> ElemFn for AssertFnText<'a> {
    type Output = bool;
    fn apply_unsigned(&mut self, _: MaybeNa<&u64>) -> bool { false }
    fn apply_signed(&mut self, _: MaybeNa<&i64>) -> bool { false }
    fn apply_text(&mut self, value: MaybeNa<&String>) -> bool { self.value == value }
    fn apply_boolean(&mut self, _: MaybeNa<&bool>) -> bool { false }
    fn apply_float(&mut self, _: MaybeNa<&f64>) -> bool { false }
}

/// `ElemFn` function for assertions of boolean values.
pub struct AssertFnBoolean<'a> {
    value: MaybeNa<&'a bool>
}
impl<'a> ElemFn for AssertFnBoolean<'a> {
    type Output = bool;
    fn apply_unsigned(&mut self, _: MaybeNa<&u64>) -> bool { false }
    fn apply_signed(&mut self, _: MaybeNa<&i64>) -> bool { false }
    fn apply_text(&mut self, _: MaybeNa<&String>) -> bool { false }
    fn apply_boolean(&mut self, value: MaybeNa<&bool>) -> bool { self.value == value }
    fn apply_float(&mut self, _: MaybeNa<&f64>) -> bool { false }
}

/// `ElemFn` function for assertions of floating-point values.
pub struct AssertFnFloat<'a> {
    value: MaybeNa<&'a f64>
}
impl<'a> ElemFn for AssertFnFloat<'a> {
    type Output = bool;
    fn apply_unsigned(&mut self, _: MaybeNa<&u64>) -> bool { false }
    fn apply_signed(&mut self, _: MaybeNa<&i64>) -> bool { false }
    fn apply_text(&mut self, _: MaybeNa<&String>) -> bool { false }
    fn apply_boolean(&mut self, _: MaybeNa<&bool>) -> bool { false }
    fn apply_float(&mut self, value: MaybeNa<&f64>) -> bool { self.value == value }
}

/// Helper trait / implementations for assertions (often used for test purposes). Returns `true`
/// if the selected element matches the provided target value.
pub trait Assert<Selector, T> {
    /// Returns `true` if the element specified with the `Selector` matches the provided target
    /// value.
    fn assert(&self, select: Selector, target: T) -> Option<bool>;
}
impl<S: Selector, T> Assert<S, u64> for T where T: ApplyToElem<S> {
    fn assert(&self, select: S, target: u64) -> Option<bool> {
        self.apply_to_elem(
            AssertFnUnsigned { value: MaybeNa::Exists(&target) },
            select
        )
    }
}
impl<S: Selector, T> Assert<S, i64> for T where T: ApplyToElem<S> {
    fn assert(&self, select: S, target: i64) -> Option<bool> {
        self.apply_to_elem(
            AssertFnSigned { value: MaybeNa::Exists(&target) },
            select
        )
    }
}
impl<S: Selector, T> Assert<S, String> for T where T: ApplyToElem<S> {
    fn assert(&self, select: S, target: String) -> Option<bool> {
        self.apply_to_elem(
            AssertFnText { value: MaybeNa::Exists(&target) },
            select
        )
    }
}
impl<S: Selector, T> Assert<S, bool> for T where T: ApplyToElem<S> {
    fn assert(&self, select: S, target: bool) -> Option<bool> {
        self.apply_to_elem(
            AssertFnBoolean { value: MaybeNa::Exists(&target) },
            select
        )
    }
}
impl<S: Selector, T> Assert<S, f64> for T where T: ApplyToElem<S> {
    fn assert(&self, select: S, target: f64) -> Option<bool> {
        self.apply_to_elem(
            AssertFnFloat { value: MaybeNa::Exists(&target) },
            select
        )
    }
}

/// `ElemFn` function for assertions of unsigned integer values against a predicate.
pub struct AssertPredFnUnsigned<F: Fn(&u64) -> bool> {
    f: F,
}
impl<F: Fn(&u64) -> bool> FieldFn for AssertPredFnUnsigned<F> {
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

/// `ElemFn` function for assertions of signed integer values against a predicate.
pub struct AssertPredFnSigned<F: Fn(&i64) -> bool> {
    f: F,
}
impl<F: Fn(&i64) -> bool> FieldFn for AssertPredFnSigned<F> {
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

/// `ElemFn` function for assertions of text values against a predicate.
pub struct AssertPredFnText<F: Fn(&String) -> bool> {
    f: F,
}
impl<F: Fn(&String) -> bool> FieldFn for AssertPredFnText<F> {
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

/// `ElemFn` function for assertions of boolean values against a predicate.
pub struct AssertPredFnBoolean<F: Fn(&bool) -> bool> {
    f: F,
}
impl<F: Fn(&bool) -> bool> FieldFn for AssertPredFnBoolean<F> {
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

/// `ElemFn` function for assertions of floating-point values against a predicate.
pub struct AssertPredFnFloat<F: Fn(&f64) -> bool> {
    f: F,
}
impl<F: Fn(&f64) -> bool> FieldFn for AssertPredFnFloat<F> {
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

/// Helper trait / implementations for assertions (often used for test purposes). Returns `true`
/// if the provided predicate returns true for the selected element.
pub trait AssertPred<S: Selector, T> {
    /// Returns `true` if the element specified with the `Selector` passes the predicate.
    fn assert_pred<F: Fn(&T) -> bool>(&self, select: S, pred: F) -> Option<bool>;
}
impl<S: Selector, T> AssertPred<S, u64> for T where T: ApplyToField<S> {
    fn assert_pred<F: Fn(&u64) -> bool>(&self, select: S, pred: F) -> Option<bool> {
        self.apply_to_field(
            AssertPredFnUnsigned { f: pred },
            select
        )
    }
}
impl<S: Selector, T> AssertPred<S, i64> for T where T: ApplyToField<S> {
    fn assert_pred<F: Fn(&i64) -> bool>(&self, select: S, pred: F) -> Option<bool> {
        self.apply_to_field(
            AssertPredFnSigned { f: pred },
            select
        )
    }
}
impl<S: Selector, T> AssertPred<S, String> for T where T: ApplyToField<S> {
    fn assert_pred<F: Fn(&String) -> bool>(&self, select: S, pred: F) -> Option<bool> {
        self.apply_to_field(
            AssertPredFnText { f: pred },
            select
        )
    }
}
impl<S: Selector, T> AssertPred<S, bool> for T where T: ApplyToField<S> {
    fn assert_pred<F: Fn(&bool) -> bool>(&self, select: S, pred: F) -> Option<bool> {
        self.apply_to_field(
            AssertPredFnBoolean { f: pred },
            select
        )
    }
}
impl<S: Selector, T> AssertPred<S, f64> for T where T: ApplyToField<S> {
    fn assert_pred<F: Fn(&f64) -> bool>(&self, select: S, pred: F) -> Option<bool> {
        self.apply_to_field(
            AssertPredFnFloat { f: pred },
            select
        )
    }
}
