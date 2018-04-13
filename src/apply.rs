use std::cmp::Ordering;
use field::FieldIdent;
use masked::MaybeNa;

pub trait Selector {
    type IndexType;
    fn index(&self) -> Self::IndexType;
}
pub struct IndexSelector(pub usize);
impl Selector for IndexSelector {
    type IndexType = usize;
    fn index(&self) -> usize { self.0 }
}
pub struct FieldIndexSelector<'a>(pub &'a FieldIdent, pub usize);
impl<'a> Selector for FieldIndexSelector<'a> {
    type IndexType = (&'a FieldIdent, usize);
    fn index(&self) -> (&'a FieldIdent, usize) { (self.0, self.1) }
}
pub struct FieldSelector<'a>(pub &'a FieldIdent);
impl<'a> Selector for FieldSelector<'a> {
    type IndexType = (&'a FieldIdent);
    fn index(&self) -> (&'a FieldIdent) { (self.0) }
}
pub struct NilSelector;
impl Selector for NilSelector {
    type IndexType = ();
    fn index(&self) -> () {}
}

pub trait ApplyToElem<Selector> {
    fn apply_to_elem<T: ElemFn>(&self, f: T, select: Selector) -> Option<T::Output>;
}
// pub trait ApplyToAllFieldElems {
//     fn apply_to_all_field_elems<T: ElemFn>(&self, f: T, ident: &FieldIdent) -> Option<T::Output>;
// }
// pub trait ApplyToFieldElem {
//     fn apply_to_field_elem<T: ElemFn>(&self, f: T, ident: &FieldIdent, idx: usize)
//         -> Option<T::Output>;
// }

pub trait ElemFn {
    type Output;
    fn apply_unsigned(&mut self, value: MaybeNa<&u64>) -> Self::Output;
    fn apply_signed(&mut self, value: MaybeNa<&i64>) -> Self::Output;
    fn apply_text(&mut self, value: MaybeNa<&String>) -> Self::Output;
    fn apply_boolean(&mut self, value: MaybeNa<&bool>) -> Self::Output;
    fn apply_float(&mut self, value: MaybeNa<&f64>) -> Self::Output;
}

pub trait ApplyToField<Selector> {
    fn apply_to_field<T: FieldFn>(&self, f: T, select: Selector) -> Option<T::Output>;
}
pub trait ApplyToField2<Selector> {
    fn apply_to_field2<T: Field2Fn>(&self, f: T, select: (Selector, Selector)) -> Option<T::Output>;
}

pub trait DataIndex<T: PartialOrd> {
    fn get_data(&self, idx: usize) -> Option<MaybeNa<&T>>;
    fn len(&self) -> usize;
}
pub trait FieldFn {
    type Output;
    fn apply_unsigned<T: DataIndex<u64>>(&mut self, field: &T) -> Self::Output;
    fn apply_signed<T: DataIndex<i64>>(&mut self, field: &T) -> Self::Output;
    fn apply_text<T: DataIndex<String>>(&mut self, field: &T) -> Self::Output;
    fn apply_boolean<T: DataIndex<bool>>(&mut self, field: &T) -> Self::Output;
    fn apply_float<T: DataIndex<f64>>(&mut self, field: &T) -> Self::Output;
}
pub trait Field2Fn {
    type Output;
    fn apply_unsigned<T: DataIndex<u64>>(&mut self, field: &(&T, &T)) -> Self::Output;
    fn apply_signed<T: DataIndex<i64>>(&mut self, field: &(&T, &T)) -> Self::Output;
    fn apply_text<T: DataIndex<String>>(&mut self, field: &(&T, &T)) -> Self::Output;
    fn apply_boolean<T: DataIndex<bool>>(&mut self, field: &(&T, &T)) -> Self::Output;
    fn apply_float<T: DataIndex<f64>>(&mut self, field: &(&T, &T)) -> Self::Output;
}

pub(crate) type SortedOrder = Vec<usize>;
pub trait SortOrderBy<Selector> {
    fn sort_order_by(&self, select: Selector) -> Option<SortedOrder>;
}
impl<Selector, T> SortOrderBy<Selector> for T where T: ApplyToField<Selector> {
    fn sort_order_by(&self, select: Selector) -> Option<SortedOrder> {
        self.apply_to_field(SortOrderFn {}, select)
    }
}

pub struct SortOrderFn {}
macro_rules! impl_sort_order_fn {
    ($name:tt; $ty:ty) => {
        // ordering is (arbitrarily) going to be:
        // NA values, followed by everything else ascending
        fn $name<'a, T: DataIndex<$ty>>(&mut self, field: &T) -> SortedOrder {
            let mut order = (0..field.len()).collect::<Vec<_>>();
            order.sort_unstable_by(|&a, &b| {
                // a, b are always in range, so unwraps are safe
                field.get_data(a).unwrap().cmp(&field.get_data(b).unwrap())
            });
            println!("{} order: {:?}", stringify!($name), order);
            order
        }
    }
}
impl FieldFn for SortOrderFn {
    type Output = SortedOrder;
    impl_sort_order_fn!(apply_unsigned; u64);
    impl_sort_order_fn!(apply_signed;   i64);
    impl_sort_order_fn!(apply_text;     String);
    impl_sort_order_fn!(apply_boolean;  bool);

    fn apply_float<'a, T: DataIndex<f64>>(&mut self, field: &T) -> SortedOrder {
        let mut order = (0..field.len()).collect::<Vec<_>>();
        order.sort_unstable_by(|&a, &b| {
            // a, b are always in range, so unwraps are safe
            let (vala, valb) = (field.get_data(a).unwrap(), field.get_data(b).unwrap());
            vala.partial_cmp(&valb).unwrap_or_else(|| {
                // partial_cmp doesn't fail for MaybeNa::NA, unwraps safe
                let (vala, valb) = (vala.unwrap(), valb.unwrap());
                if vala.is_nan() && !valb.is_nan() {
                    Ordering::Less
                } else {
                    // since partial_cmp only fails for NAN, then !vala.is_nan() && valb.is_nan()
                    Ordering::Greater
                }
            })
        });
        order
    }
}

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

pub trait Assert<Selector, T> {
    fn assert(&self, select: Selector, target: T) -> Option<bool>;
}
impl<Selector, T> Assert<Selector, u64> for T where T: ApplyToElem<Selector> {
    fn assert(&self, select: Selector, target: u64) -> Option<bool> {
        self.apply_to_elem(
            AssertFnUnsigned { value: MaybeNa::Exists(&target) },
            select
        )
    }
}
impl<Selector, T> Assert<Selector, i64> for T where T: ApplyToElem<Selector> {
    fn assert(&self, select: Selector, target: i64) -> Option<bool> {
        self.apply_to_elem(
            AssertFnSigned { value: MaybeNa::Exists(&target) },
            select
        )
    }
}
impl<Selector, T> Assert<Selector, String> for T where T: ApplyToElem<Selector> {
    fn assert(&self, select: Selector, target: String) -> Option<bool> {
        self.apply_to_elem(
            AssertFnText { value: MaybeNa::Exists(&target) },
            select
        )
    }
}
impl<Selector, T> Assert<Selector, bool> for T where T: ApplyToElem<Selector> {
    fn assert(&self, select: Selector, target: bool) -> Option<bool> {
        self.apply_to_elem(
            AssertFnBoolean { value: MaybeNa::Exists(&target) },
            select
        )
    }
}
impl<Selector, T> Assert<Selector, f64> for T where T: ApplyToElem<Selector> {
    fn assert(&self, select: Selector, target: f64) -> Option<bool> {
        self.apply_to_elem(
            AssertFnFloat { value: MaybeNa::Exists(&target) },
            select
        )
    }
}

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

pub trait AssertPred<Selector, T> {
    fn assert_pred<F: Fn(&T) -> bool>(&self, select: Selector, pred: F) -> Option<bool>;
}
impl<Selector, T> AssertPred<Selector, u64> for T where T: ApplyToField<Selector> {
    fn assert_pred<F: Fn(&u64) -> bool>(&self, select: Selector, pred: F) -> Option<bool> {
        self.apply_to_field(
            AssertPredFnUnsigned { f: pred },
            select
        )
    }
}
impl<Selector, T> AssertPred<Selector, i64> for T where T: ApplyToField<Selector> {
    fn assert_pred<F: Fn(&i64) -> bool>(&self, select: Selector, pred: F) -> Option<bool> {
        self.apply_to_field(
            AssertPredFnSigned { f: pred },
            select
        )
    }
}
impl<Selector, T> AssertPred<Selector, String> for T where T: ApplyToField<Selector> {
    fn assert_pred<F: Fn(&String) -> bool>(&self, select: Selector, pred: F) -> Option<bool> {
        self.apply_to_field(
            AssertPredFnText { f: pred },
            select
        )
    }
}
impl<Selector, T> AssertPred<Selector, bool> for T where T: ApplyToField<Selector> {
    fn assert_pred<F: Fn(&bool) -> bool>(&self, select: Selector, pred: F) -> Option<bool> {
        self.apply_to_field(
            AssertPredFnBoolean { f: pred },
            select
        )
    }
}
impl<Selector, T> AssertPred<Selector, f64> for T where T: ApplyToField<Selector> {
    fn assert_pred<F: Fn(&f64) -> bool>(&self, select: Selector, pred: F) -> Option<bool> {
        self.apply_to_field(
            AssertPredFnFloat { f: pred },
            select
        )
    }
}
