use std::collections::VecDeque;
use std::rc::Rc;
use std::ops::{BitAnd, BitOr, Sub, Add};
use std::marker::PhantomData;

use typenum::{
    type_operators::{IsEqual},
    marker_traits::{Bit},
    operator_aliases::{And, Or, Sub1, Add1},
    bit::{B1, B0},
    uint::{Unsigned, UInt, UTerm}
};

use cons::{Cons, Nil};

/// A label for a value in an `LVCons`. Backed by a type-level natural number `Idx`.
#[derive(Debug, Clone)]
pub struct Label<Idx, Name>
{
    index: Idx,
    _name: PhantomData<Name>
}

pub trait LabelName
{
    fn name() -> &'static str;
}
impl<Idx, Name> LabelName for Label<Idx, Name>
    where Name: LabelName
{
    fn name() -> &'static str { Name::name() }
}

pub trait LabelIndex
{
    type Idx;
}
impl<Idx, Name> LabelIndex for Label<Idx, Name>
{
    type Idx = Idx;
}
impl<Label, Value, Tail> LabelIndex for LVCons<Label, Value, Tail>
    where Label: LabelIndex
{
    type Idx = Label::Idx;
}

// pub type NextLabelIndex<T> = Add1<<T as LabelIndex>::Idx>;

//TODO: figure out how to have this return an array
pub trait StrLabels
{
    fn labels<'a>() -> VecDeque<&'a str>;
}
impl StrLabels for Nil
{
    fn labels<'a>() -> VecDeque<&'a str> { VecDeque::new() }
}
impl<L, V, T> StrLabels for LVCons<L, V, T>
    where L: LabelName,
          T: StrLabels
{
    fn labels<'a>() -> VecDeque<&'a str>
    {
        let mut previous = T::labels();
        previous.push_front(L::name());
        previous
    }
}


#[derive(Debug, Clone)]
pub struct Labeled<L, V>
{
    _label: PhantomData<L>,
    value: V
}
impl<L, V> From<V> for Labeled<L, V>
{
    fn from(orig: V) -> Labeled<L, V>
    {
        Labeled
        {
            _label: PhantomData,
            value: orig
        }
    }
}

#[derive(Debug, Clone)]
pub struct TypedValue<D, V>
{
    _dtype: PhantomData<D>,
    value: V
}
impl<D, V> From<V> for TypedValue<D, V>
{
    fn from(orig: V) -> TypedValue<D, V>
    {
        TypedValue
        {
            _dtype: PhantomData,
            value: orig
        }
    }
}

pub trait Typed
{
    type DType;
}
impl<D, V> Typed for TypedValue<D, V>
{
    type DType = D;
}
impl<L, D, V> Typed for Labeled<L, TypedValue<D, V>>
{
    type DType = D;
}

pub type TypeOf<T> = <T as Typed>::DType;

impl<T> Typed for ::field::FieldData<T>
{
    type DType = T;
}
impl<T> Typed for ::frame::Framed<T>
{
    type DType = T;
}
impl<T> Typed for ::std::rc::Rc<T>
    where T: Typed
{
    type DType = T::DType;
}

/// Marker trait for an object that can be held in a Label<...> or TypedValue<...> container.
pub trait SelfValued {}

macro_rules! impl_selfvalued {
    ($($dtype:ty)*) => {$(
        impl SelfValued for $dtype {}
    )*}
}
impl_selfvalued![
    f32 f64
    i8 i16 i32 i64 i128 isize
    u8 u16 u32 u64 u128 usize
    bool char str String
];
impl<T> SelfValued for ::field::FieldData<T> {}
impl<T> SelfValued for ::frame::Framed<T> {}
impl<T> SelfValued for Rc<T> {}

pub trait Valued
{
    type Value;
    fn value_ref(&self) -> &Self::Value;
    fn value(self) -> Self::Value;
}
impl<T> Valued for T where  T: SelfValued {
    type Value = Self;
    fn value_ref(&self) -> &Self { self }
    fn value(self) -> Self::Value { self }
}
impl<D, V> Valued for TypedValue<D, V>
    where V: Valued
{
    type Value = V::Value;
    fn value_ref(&self) -> &Self::Value { &self.value.value_ref() }
    fn value(self) -> Self::Value { self.value.value() }
}
impl<L, V> Valued for Labeled<L, V>
    where V: Valued
{
    type Value = V::Value;
    fn value_ref(&self) -> &V::Value { self.value.value_ref() }
    fn value(self) -> V::Value { self.value.value() }
}

/// Alias for retrieving the Value of a Valued object
pub type ValueOf<T> = <T as Valued>::Value;

pub trait Marked
{
    type Marker;
}
impl<L, M> Marked for Labeled<L, PhantomData<M>>
{
    type Marker = M;
}
impl<L, D, M> Marked for Labeled<L, TypedValue<D, PhantomData<M>>>
{
    type Marker = M;
}
pub type MarkerOf<T> = <T as Marked>::Marker;

/// Label-value cons-list
pub type LVCons<L, V, T> = Cons<Labeled<L, V>, T>;
/// Label-only cons-list
pub type LCons<L, T> = LVCons<L, (), T>;
pub type LabelCons<L, T> = LCons<L, T>;
/// Label-marker cons-list
pub type LMCons<L, M, T> = LVCons<L, PhantomData<M>, T>;
/// Label-DType-value cons-list
pub type LDVCons<L, D, V, T> = LVCons<L, TypedValue<D, V>, T>;

/// Label-level equality. Leverages `typenum`'s `IsEqual` trait for type-level-number equality,
/// but doesn't use `IsEqual`'s `is_equal` method (since no results of this equality check are
/// intended to be instantiated).
pub trait LabelEq<Other> {
    type Eq: Bit;
}

pub type True = B1;
pub type False = B0;

impl<T, U> LabelEq<U> for T
    where T: IsEqual<U>
{
    type Eq = <T as IsEqual<U>>::Output;
}

/// Type-level equality, extended for Labels. The `Name` type parameter doesn't matter.
impl<T, U, TName, UName> LabelEq<Label<U, UName>> for Label<T, TName>
    where T: LabelEq<U>
{
    type Eq = <T as LabelEq<U>>::Eq;
}

/// Equate Labels to backing naturals
impl<T, Name> LabelEq<UTerm> for Label<T, Name>
    where T: LabelEq<UTerm>
{
    type Eq = <T as LabelEq<UTerm>>::Eq;
}
impl<T, U, Name> LabelEq<UInt<U, B0>> for Label<T, Name>
    where T: LabelEq<UInt<U, B0>>
{
    type Eq = <T as LabelEq<UInt<U, B0>>>::Eq;
}
impl<T, U, Name> LabelEq<UInt<U, B1>> for Label<T, Name>
    where T: LabelEq<UInt<U, B1>>
{
    type Eq = <T as LabelEq<UInt<U, B1>>>::Eq;
}

impl<U, Name> LabelEq<Label<U, Name>> for UTerm
    where UTerm: LabelEq<U>
{
    type Eq = <UTerm as LabelEq<U>>::Eq;
}
impl<U, Name> LabelEq<Label<U, Name>> for UInt<U, B0>
    where UInt<U, B0>: LabelEq<U>
{
    type Eq = <UInt<U, B0> as LabelEq<U>>::Eq;
}
impl<U, Name> LabelEq<Label<U, Name>> for UInt<U, B1>
    where UInt<U, B1>: LabelEq<U>
{
    type Eq = <UInt<U, B1> as LabelEq<U>>::Eq;
}

/// A label for a value in an `LVCons` within a specific namespace `NS`. Backed by a type-level
/// natural number `N`.
#[derive(Debug, Clone)]
pub struct NsLabel<N, NS> {
    nat: N,
    namespace: NS
}
/// Type-level equality implementation for `NsLabel`s. Result will be `True` if both namespace and
/// the type-level natural number backing this label match.
impl<T, U, TNS, UNS> LabelEq<NsLabel<U, UNS>> for NsLabel<T, TNS>
    where T: LabelEq<U>,
          TNS: LabelEq<UNS>,
          <T as LabelEq<U>>::Eq: BitAnd<<TNS as LabelEq<UNS>>::Eq>,
          <<T as LabelEq<U>>::Eq as BitAnd<<TNS as LabelEq<UNS>>::Eq>>::Output: Bit,
{
    type Eq = And<<T as LabelEq<U>>::Eq, <TNS as LabelEq<UNS>>::Eq>;
}

/// `LabelEq`-based membership test
pub trait Member<E> {
    type IsMember: Bit;
}

impl<E> Member<E> for Nil {
    type IsMember = False;
}
impl<E, L, V, T> Member<E> for LVCons<L, V, T>
    where L: LabelEq<E>,
          T: Member<E>,
          <L as LabelEq<E>>::Eq: BitOr<<T as Member<E>>::IsMember>,
          <<L as LabelEq<E>>::Eq as BitOr<<T as Member<E>>::IsMember>>::Output: Bit,
{
    type IsMember = Or<<L as LabelEq<E>>::Eq, <T as Member<E>>::IsMember>;
}

/// Trait to ensure that all labels in `LabeList` are found in cons-list `Self`.
pub trait HasLabels<LabelList>
{}
impl<T> HasLabels<Nil> for T
{}
impl<NeedleLbl, NeedleTail, Haystack> HasLabels<LabelCons<NeedleLbl, NeedleTail>>
    for Haystack
    where Haystack: Member<NeedleLbl, IsMember=True>,
          Haystack: HasLabels<NeedleTail>,
{}
impl<NeedleLblIdx, NeedleLbl, Haystack> HasLabels<Label<NeedleLblIdx, NeedleLbl>>
    for Haystack
    where Haystack: Member<Label<NeedleLblIdx, NeedleLbl>, IsMember=True>,
{}

/// Trait to find the subset of cons-list `Self` which are labeled with labels in `LabelList`.
///
/// Any labels in `LabelList` not found in `Self` will be ignored (see `HasLabels` for a trait
/// that requires all members of `LabelList` to be found).
pub trait Filter<LabelList>
{
    type Filtered;

    /// Filters `Self`, constructing new cons-list of type `Filtered`.
    fn filter(self) -> Self::Filtered;
}

// End-point. No more list elements to search. We don't care if anything remains or not in
// `LabelList`.
impl<LabelList> Filter<LabelList> for Nil
{
    type Filtered = Nil;

    fn filter(self) -> Nil { Nil }
}

// Implementation for `LVCons` cons-lists.
impl<LabelList, L, V, T>
    Filter<LabelList>
    for LVCons<L, V, T>
    where
        LabelList: Member<L>,
        LVCons<L, V, T>: FilterPred<LabelList, <LabelList as Member<L>>::IsMember>
{
    type Filtered =
        <LVCons<L, V, T> as FilterPred<LabelList, <LabelList as Member<L>>::IsMember>>::Filtered;

    fn filter(self) -> Self::Filtered
    {
        self.filter_pred()
    }
}

/// Helper filter trait. Used by `Filter` for computing the subset of `Self` cons-list which
/// contains the labels in `LabelList`.
///
/// `IsMember` specifies whether or not the label of the head value of `Self` is a member of
/// `LabelList`.
pub trait FilterPred<LabelList, IsMember>
{
    type Filtered;

    fn filter_pred(self) -> Self::Filtered;
}

// `FilterPred` implementation for a cons-list where the head is in `LabelList`.
impl<LabelList, H, T>
    FilterPred<LabelList, True>
    for Cons<H, T>
    where
        T: Filter<LabelList>,
{
    // head is in list, so we include it and check the tail
    type Filtered = Cons<H, <T as Filter<LabelList>>::Filtered>;

    fn filter_pred(self) -> Self::Filtered
    {
        Cons
        {
            head: self.head,
            tail: self.tail.filter()
        }
    }
}
// `FilterPred` implementation for a cons-list where the head isn't in `LabelList`.
impl<LabelList, H, T>
    FilterPred<LabelList, False>
    for Cons<H, T>
    where
        T: Filter<LabelList>,
{
    // head isn't in list, so we check the tail
    type Filtered = <T as Filter<LabelList>>::Filtered;

    fn filter_pred(self) -> Self::Filtered
    {
        self.tail.filter()
    }
}


/// Trait to find the subset of cons-list `Self` which are labeled with labels in `LabelList`,
/// providing a method to clone a copy of that list.
///
/// Any labels in `LabelList` not found in `Self` will be ignored (see `HasLabels` for a trait
/// that requires all members of `LabelList` to be found).
pub trait FilterClone<LabelList>
{
    type Filtered;

    /// Filters `Self` and clones into new cons-list of type `Filtered`.
    fn filter_clone(&self) -> Self::Filtered;
}

impl<LabelList> FilterClone<LabelList> for Nil
{
    type Filtered = Nil;

    fn filter_clone(&self) -> Nil { Nil }
}

// Implementation for `LVCons` cons-lists.
impl<LabelList, L, V, T>
    FilterClone<LabelList>
    for LVCons<L, V, T>
    where
        LabelList: Member<L>,
        LVCons<L, V, T>: FilterPredClone<LabelList, <LabelList as Member<L>>::IsMember>
{
    type Filtered =
        <LVCons<L, V, T> as FilterPredClone<LabelList, <LabelList as Member<L>>::IsMember>>
            ::Filtered;

    fn filter_clone(&self) -> Self::Filtered
    {
        self.filter_pred_clone()
    }
}

/// Helper filter trait. Used by `Filter` for computing the subset of `Self` cons-list which
/// contains the labels in `LabelList`, and cloning a copy of that subset.
///
/// `IsMember` specifies whether or not the label of the head value of `Self` is a member of
/// `LabelList`.
pub trait FilterPredClone<LabelList, IsMember>
{
    type Filtered;

    fn filter_pred_clone(&self) -> Self::Filtered;
}

// `FilterPredClone` implementation for a cons-list where the head is in `LabelList`.
impl<LabelList, H, T>
    FilterPredClone<LabelList, True>
    for Cons<H, T>
    where
        T: FilterClone<LabelList>,
        H: Clone
{
    // head is in list, so we include it and check the tail
    type Filtered = Cons<H, <T as FilterClone<LabelList>>::Filtered>;

    fn filter_pred_clone(&self) -> Self::Filtered
    {
        Cons
        {
            head: self.head.clone(),
            tail: self.tail.filter_clone()
        }
    }
}
// `FilterPred` implementation for a cons-list where the head isn't in `LabelList`.
impl<LabelList, H, T>
    FilterPredClone<LabelList, False>
    for Cons<H, T>
    where
        T: FilterClone<LabelList>,
{
    // head isn't in list, so we check the tail
    type Filtered = <T as FilterClone<LabelList>>::Filtered;

    fn filter_pred_clone(&self) -> Self::Filtered
    {
        self.tail.filter_clone()
    }
}

/// Lookup into a `Cons`-list by `typenum` natural number.
pub trait LookupElemByNat<N>
{
    type Elem;
    fn elem(&self) -> &Self::Elem;
}

impl<H, T> LookupElemByNat<UTerm> for Cons<H, T>
{
    type Elem = H;
    fn elem(&self) -> &Self::Elem { &self.head }
}

impl<H, T> LookupElemByNat<UInt<UTerm, B1>> for Cons<H, T>
    where T: LookupElemByNat<UTerm>
{
    type Elem = <T as LookupElemByNat<UTerm>>::Elem;
    fn elem(&self) -> &Self::Elem { self.tail.elem() }
}

impl<H, T, N> LookupElemByNat<UInt<N, B0>> for Cons<H, T>
    where N: Sub<B1>,
          T: LookupElemByNat<UInt<Sub1<N>, B1>>
{
    type Elem = <T as LookupElemByNat<UInt<Sub1<N>, B1>>>::Elem;
    fn elem(&self) -> &Self::Elem { self.tail.elem() }
}

impl<H, T, N, B> LookupElemByNat<UInt<UInt<N, B>, B1>> for Cons<H, T>
    where T: LookupElemByNat<UInt<UInt<N, B>, B0>>
{
    type Elem = <T as LookupElemByNat<UInt<UInt<N, B>, B0>>>::Elem;
    fn elem(&self) -> &Self::Elem { self.tail.elem() }
}

pub trait LookupNatByLabel<L>
{
    type Nat: Unsigned;
    fn nat(&self) -> usize { Self::Nat::to_usize() }
}
impl<TargetL, L, V, T> LookupNatByLabel<TargetL>
    for LVCons<L, V, T>
    where TargetL: LabelEq<L>,
          LVCons<L, V, T>:
            LookupNatByLabelMatch<TargetL, <TargetL as LabelEq<L>>::Eq>,
{
    type Nat = <LVCons<L, V, T> as
        LookupNatByLabelMatch<TargetL, <TargetL as LabelEq<L>>::Eq>>::Nat;
}

pub trait LookupNatByLabelMatch<TargetL, B>
{
    type Nat: Unsigned;
}
impl<TargetL, L, V, T> LookupNatByLabelMatch<TargetL, True>
    for LVCons<L, V, T>
{
    type Nat = UTerm;
}
impl<TargetL, L, V, T> LookupNatByLabelMatch<TargetL, False>
    for LVCons<L, V, T>
    where T: LookupNatByLabel<TargetL>,
          <T as LookupNatByLabel<TargetL>>::Nat: Add<B1>,
          <<T as LookupNatByLabel<TargetL>>::Nat as Add<B1>>::Output: Unsigned
{
    type Nat = Add1<<T as LookupNatByLabel<TargetL>>::Nat>;
}

pub trait LookupElemByLabel<L>
{
    type Elem;
    fn elem(&self) -> &Self::Elem;
}
impl<L, T> LookupElemByLabel<L> for T
    where T: LookupNatByLabel<L>,
          T: LookupElemByNat<<T as LookupNatByLabel<L>>::Nat>
{
    type Elem = <Self as LookupElemByNat<<Self as LookupNatByLabel<L>>::Nat>>::Elem;
    fn elem(&self) -> &Self::Elem {
        LookupElemByNat::<_>::elem(self)
    }
}
pub type ElemOf<T, Label> = <T as LookupElemByLabel<Label>>::Elem;

pub trait LookupValuedElemByLabel<L>: LookupElemByLabel<L>
{
    type Elem: Valued;
    fn elem(&self) -> &<Self as LookupValuedElemByLabel<L>>::Elem;
}
impl<T, L> LookupValuedElemByLabel<L>
    for T
    where
        T: LookupElemByLabel<L>,
        ElemOf<Self, L>: Valued
{
    type Elem = <Self as LookupElemByLabel<L>>::Elem;
    fn elem(&self) -> &<Self as LookupElemByLabel<L>>::Elem
    {
        <Self as LookupElemByLabel<L>>::elem(self)
    }
}
pub type ValuedElemOf<T, Label> = <T as LookupValuedElemByLabel<Label>>::Elem;
pub type ValueOfElemOf<T, Label> = <<T as LookupValuedElemByLabel<Label>>::Elem as Valued>::Value;

pub trait LookupMarkedElemByLabel<L>: LookupElemByLabel<L>
{
    type Elem: Marked;
}
impl<T, L> LookupMarkedElemByLabel<L>
    for T
    where
        T: LookupElemByLabel<L>,
        ElemOf<Self, L>: Marked
{
    type Elem = <Self as LookupElemByLabel<L>>::Elem;
}
pub type MarkedElemOf<T, Label> = <T as LookupMarkedElemByLabel<Label>>::Elem;
pub type MarkerOfElemOf<T, Label> = <<T as LookupMarkedElemByLabel<Label>>::Elem as Marked>::Marker;

pub trait LookupTypedElemByLabel<L>: LookupElemByLabel<L>
{
    type Elem: Typed;
}
impl<T, L> LookupTypedElemByLabel<L>
    for T
    where
        T: LookupElemByLabel<L>,
        ElemOf<Self, L>: Typed,
{
    type Elem = <Self as LookupElemByLabel<L>>::Elem;
}
pub type TypedElemOf<T, Label> = <T as LookupTypedElemByLabel<Label>>::Elem;
pub type TypeOfElemOf<T, Label> = <<T as LookupTypedElemByLabel<Label>>::Elem as Typed>::DType;

pub trait AssocLabels {
    type Labels;
}
impl<Label, Value, Tail> AssocLabels for LVCons<Label, Value, Tail>
    where Tail: AssocLabels,
{
    type Labels = LabelCons<Label, Tail::Labels>;
}
impl AssocLabels for Nil {
    type Labels = Nil;
}



#[macro_export]
macro_rules! label {
    ($name:ident, $nat:ty) => {
        #[allow(non_snake_case)]
        pub mod $name {
            #![allow(dead_code)]
            #[allow(unused_imports)]
            use typenum::consts::*;
            pub const NAME: &'static str = stringify![$name];

            pub type Natural = $nat;

            #[derive(Debug, Clone)]
            pub struct Name;
            impl $crate::label::LabelName for Name
            {
                fn name() -> &'static str { NAME }
            }
            pub type Label = $crate::label::Label<$nat, Name>;
        }
    }
}

#[macro_export]
macro_rules! Labels {
    (@labels()) => { Nil };
    (@labels($label:ident, $($rest:ident,)*)) =>
    {
        LCons<$label::Label, Labels![@labels($($rest,)*)]>
    };
    ($($label:ident),*$(,)*) =>
    {
        Labels![@labels($($label,)*)]
    }
}

#[macro_export]
macro_rules! Fields {
    (@fields()) => { Nil };
    (@fields($label:ident: $dtype:ident, $($rest_label:ident: $rest_dtype:ident,)*)) =>
    {
        FieldCons<$label::Label, $dtype, Fields![@fields($($rest_label: $rest_dtype,)*)]>
    };
    ($($label:ident: $dtype:ident),*$(,)*) =>
    {
        Fields![@fields($($label: $dtype,)*)]
    };
    ($existing:ident .. $($label:ident: $dtype:ident),*$(,)*) =>
    {
        <$existing as $crate::cons::Append<Fields![@fields($($label: $dtype,)*)]>>::Appended
    };
    ($($label:ident: $dtype:ident),*$(,)* .. $existing:ident) =>
    {
        <Fields![@fields($($label: $dtype,)*)] as $crate::cons::Append<$existing>>::Appended
    }
}


#[cfg(test)]
mod tests
{
    use cons::*;
    use super::*;
    use typenum::{
        Bit,
        consts::{U0, U1, U2, U3, U4}
    };

    #[test]
    fn type_eq()
    {
        assert!(<U1 as LabelEq<U1>>::Eq::to_bool());
        assert!(!<U1 as LabelEq<U4>>::Eq::to_bool());

        label![ImALabel, U0];
        label![ImAnotherLabel, U1];
        // type ImALabel = Label<U0>;
        // type ImAnotherLabel = Label<U1>;

        assert!(<ImALabel::Label as LabelEq<ImALabel::Label>>::Eq::to_bool());
        assert!(!<ImALabel::Label as LabelEq<ImAnotherLabel::Label>>::Eq::to_bool());
    }

    #[test]
    fn lookup()
    {
        label![F0, U0];
        label![F1, U1];
        label![F2, U2];
        label![F3, U3];
        label![F4, U4];

        let list = LVCons {
            head: Labeled::<F0::Label, _>::from(6u64),
            tail: LVCons {
                head: Labeled::<F1::Label, _>::from(5.3f64),
                tail: LVCons {
                    head: Labeled::<F2::Label, _>::from(-3i64),
                    tail: LVCons {
                        head: Labeled::<F3::Label, _>::from("Hello".to_string()),
                        tail: LVCons {
                            head: Labeled::<F4::Label, _>::from(3.2f32),
                            tail: Nil
                        },
                    },
                },
            },
        };

        assert_eq!(LookupElemByNat::<U0>::elem(&list).value, 6u64);
        assert_eq!(LookupElemByNat::<U1>::elem(&list).value, 5.3);
        assert_eq!(LookupElemByNat::<U2>::elem(&list).value, -3i64);
        assert_eq!(LookupElemByNat::<U3>::elem(&list).value, "Hello".to_string());
        assert_eq!(LookupElemByNat::<U4>::elem(&list).value, 3.2f32);

        assert_eq!(LookupNatByLabel::<F0::Label>::nat(&list), 0);
        assert_eq!(LookupNatByLabel::<F1::Label>::nat(&list), 1);
        assert_eq!(LookupNatByLabel::<F2::Label>::nat(&list), 2);
        assert_eq!(LookupNatByLabel::<F3::Label>::nat(&list), 3);
        assert_eq!(LookupNatByLabel::<F4::Label>::nat(&list), 4);

        assert_eq!(LookupElemByLabel::<F0::Label>::elem(&list).value, 6u64);
        assert_eq!(LookupElemByLabel::<F1::Label>::elem(&list).value, 5.3);
        assert_eq!(LookupElemByLabel::<F2::Label>::elem(&list).value, -3i64);
        assert_eq!(LookupElemByLabel::<F3::Label>::elem(&list).value, "Hello".to_string());
        assert_eq!(LookupElemByLabel::<F4::Label>::elem(&list).value, 3.2f32);

        // type F5 = Label<U5>;
        label![F5, U5];
        let list = LVCons {
            head: Labeled::<F5::Label, _>::from(3u32),
            tail: list,
        };

        assert_eq!(LookupNatByLabel::<F0::Label>::nat(&list), 1);
        assert_eq!(LookupNatByLabel::<F1::Label>::nat(&list), 2);
        assert_eq!(LookupNatByLabel::<F2::Label>::nat(&list), 3);
        assert_eq!(LookupNatByLabel::<F3::Label>::nat(&list), 4);
        assert_eq!(LookupNatByLabel::<F4::Label>::nat(&list), 5);
        assert_eq!(LookupNatByLabel::<F5::Label>::nat(&list), 0);

        assert_eq!(LookupElemByLabel::<F0::Label>::elem(&list).value, 6u64);
        assert_eq!(LookupElemByLabel::<F1::Label>::elem(&list).value, 5.3);
        assert_eq!(LookupElemByLabel::<F2::Label>::elem(&list).value, -3i64);
        assert_eq!(LookupElemByLabel::<F3::Label>::elem(&list).value, "Hello".to_string());
        assert_eq!(LookupElemByLabel::<F4::Label>::elem(&list).value, 3.2f32);
        assert_eq!(LookupElemByLabel::<F5::Label>::elem(&list).value, 3u32);
    }

    #[test]
    fn filter()
    {
        label![F0, U0];
        label![F1, U1];
        label![F2, U2];
        label![F3, U3];
        label![F4, U4];

        type SampleLabels =
            LVCons<
                F0::Label, u64,
                LVCons<
                    F1::Label, f64,
                    LVCons<
                        F2::Label, i64,
                        LVCons<
                            F3::Label, String,
                            LVCons<
                                F4::Label, f32,
                                Nil
                            >
                        >
                    >
                >
            >;

        {
            // null case
            type Filtered = <SampleLabels as Filter<Labels![]>>::Filtered;
            // empty filter, length should be 0
            assert_eq!(Filtered::LEN, 0);
        }
        {
            // other null case
            type Filtered = <Nil as Filter<Labels![F1, F3]>>::Filtered;
            // empty cons-list, so filtered length should be 0
            assert_eq!(Filtered::LEN, 0);
        }
        {
            type Filtered = <SampleLabels as Filter<Labels![F3]>>::Filtered;
            // we only filtered 1 label, so length should be 1
            assert_eq!(Filtered::LEN, 1);
        }
        {
            type Filtered = <SampleLabels as Filter<Labels![F1, F2, F4]>>::Filtered;
            // we only filtered 3 labels, so length should be 3
            assert_eq!(Filtered::LEN, 3);

            {
                type Refiltered = <Filtered as Filter<Labels![F1, F2, F4]>>::Filtered;
                // filtered same labels, so length should stay at 3
                assert_eq!(Refiltered::LEN, 3);
            }
            {
                type Refiltered = <Filtered as Filter<Labels![F1, F2]>>::Filtered;
                // filtered 2 labels that should exist `Filtered`, so length should be 2
                assert_eq!(Refiltered::LEN, 2);
            }
            {
                type Refiltered = <Filtered as Filter<Labels![F3, F0]>>::Filtered;
                // filtered 2 labels that should not exist `Filtered`, so length should be 0
                assert_eq!(Refiltered::LEN, 0);
            }
            {
                type Refiltered = <Filtered as Filter<Labels![F0, F1, F2, F3, F4]>>::Filtered;
                // `F0 and `F3` don't exist in `Filtered`, so length should be 3
                assert_eq!(Refiltered::LEN, 3);
            }
        }
        {
            label![F5, U5];
            type Filtered = <SampleLabels as Filter<Labels![F1, F2, F4, F5]>>::Filtered;
            // F5 doesn't exist in SampleLabels, so we still should only have 3
            assert_eq!(Filtered::LEN, 3);
        }
        {
            label![F5, U5];
            label![F6, U6];
            label![F7, U7];
            type Filtered = <SampleLabels as Filter<Labels![F5, F6, F7]>>::Filtered;
            // None of these labels exist in SampleLabels, so we should have 0
            assert_eq!(Filtered::LEN, 0);
        }
        {
            // check for problems cause by duplicated in label list
            type Filtered = <SampleLabels as Filter<Labels![F2, F2, F2]>>::Filtered;
            // we only filtered 1 label (even if it was duplicated), so length should be 1
            assert_eq!(Filtered::LEN, 1);
        }
        {
            // check for problems cause by duplicated in label list
            type Filtered = <SampleLabels as Filter<Labels![F2, F2, F3]>>::Filtered;
            // we only filtered 2 label (albeit with some duplication), so length should be 2
            assert_eq!(Filtered::LEN, 2);
        }
    }
}
