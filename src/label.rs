use std::collections::VecDeque;
use std::marker::PhantomData;
use std::ops::{Add, BitAnd, BitOr, Not, Sub};
use std::rc::Rc;

use typenum::{
    bit::{B0, B1},
    marker_traits::Bit,
    operator_aliases::{Add1, And, Or, Sub1},
    type_operators::IsEqual,
    uint::{UInt, UTerm, Unsigned},
};

use cons::{Cons, Nil};
use store::DataRef;

pub trait Label: Identifier {
    const NAME: &'static str;
    const TYPE: &'static str;
}

/// A label for a value in an `LVCons` within a specific namespace `NS`. Backed by a type-level
/// natural number `N`.
#[derive(Debug, Clone)]
pub struct Ident<Ns, Nat> {
    _marker: PhantomData<(Ns, Nat)>,
}

pub trait Identifier {
    type Ident: Identifier; // = Ident<Self::Namespace, Self::Natural>;
    type Namespace;
    type Natural;
}
impl<Ns, Nat> Identifier for Ident<Ns, Nat> {
    type Ident = Self;
    type Namespace = Ns;
    type Natural = Nat;
}
pub type NsOf<T> = <T as Identifier>::Namespace;
pub type NatOf<T> = <T as Identifier>::Natural;

impl Identifier for UTerm {
    type Ident = Ident<Self::Namespace, Self::Natural>;
    type Namespace = LocalNamespace;
    type Natural = Self;
}
impl<U, B> Identifier for UInt<U, B> {
    type Ident = Ident<Self::Namespace, Self::Natural>;
    type Namespace = LocalNamespace;
    type Natural = Self;
}

pub trait LabelName {
    fn name() -> &'static str;
    fn str_type() -> &'static str;
}
impl<T> LabelName for T
where
    T: Label,
{
    fn name() -> &'static str {
        T::NAME
    }
    fn str_type() -> &'static str {
        T::TYPE
    }
}

/// Ident-level equality. Leverages `typenum`'s `IsEqual` trait for type-level-number equality,
/// but doesn't use `IsEqual`'s `is_equal` method (since no results of this equality check are
/// intended to be instantiated).
pub trait IdentEq<Other> {
    type Eq: Bit;
}

pub type True = B1;
pub type False = B0;

/// Fallback to IsEqual
impl<T, U> IdentEq<U> for T
where
    T: IsEqual<U>,
{
    type Eq = <T as IsEqual<U>>::Output;
}

/// Type-level equality implementation for `Ident`s. Result will be `True` if both namespace and
/// the type-level natural number backing this label match.
impl<TNs, TNat, UNs, UNat> IdentEq<Ident<UNs, UNat>> for Ident<TNs, TNat>
where
    TNs: IsEqual<UNs>,
    TNat: IsEqual<UNat>,
    <TNs as IsEqual<UNs>>::Output: BitAnd<<TNat as IsEqual<UNat>>::Output>,
    <<TNs as IsEqual<UNs>>::Output as BitAnd<<TNat as IsEqual<UNat>>::Output>>::Output: Bit,
{
    type Eq = And<<TNs as IsEqual<UNs>>::Output, <TNat as IsEqual<UNat>>::Output>;
}

/// Common namespace for local-only lookups (e.g. looking up the frame number in a view from a
/// field label)
pub struct LocalNamespace;
impl IsEqual<LocalNamespace> for LocalNamespace {
    type Output = True;
    fn is_equal(self, _rhs: LocalNamespace) -> True {
        B1
    }
}

pub trait LabelEq<U> {
    type Eq;
}
impl<T, U> LabelEq<U> for T
where
    T: Identifier,
    U: Identifier,
    T::Ident: IdentEq<U::Ident>,
{
    type Eq = <T::Ident as IdentEq<U::Ident>>::Eq;
}

#[derive(Debug, Clone)]
pub struct Labeled<L, V> {
    _label: PhantomData<L>,
    pub value: V,
}
impl<L, V> From<V> for Labeled<L, V> {
    fn from(orig: V) -> Labeled<L, V> {
        Labeled {
            _label: PhantomData,
            value: orig,
        }
    }
}

#[derive(Debug, Clone)]
pub struct TypedValue<D, V> {
    _dtype: PhantomData<D>,
    value: V,
}
impl<D, V> From<V> for TypedValue<D, V> {
    fn from(orig: V) -> TypedValue<D, V> {
        TypedValue {
            _dtype: PhantomData,
            value: orig,
        }
    }
}

pub trait Typed {
    type DType;
}
impl<D, V> Typed for TypedValue<D, V> {
    type DType = D;
}
impl<L, D, V> Typed for Labeled<L, TypedValue<D, V>> {
    type DType = D;
}

pub type TypeOf<T> = <T as Typed>::DType;

impl<T> Typed for ::field::FieldData<T> {
    type DType = T;
}
impl<T> Typed for ::frame::Framed<T> {
    type DType = T;
}
impl<T> Typed for ::store::DataRef<T>
where
    T: Typed,
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
impl<T> SelfValued for DataRef<T> {}

pub trait Valued {
    type Value;
    fn value_ref(&self) -> &Self::Value;
    fn value_mut(&mut self) -> &mut Self::Value;
    fn value(self) -> Self::Value;
}
impl<T> Valued for T
where
    T: SelfValued,
{
    type Value = Self;
    fn value_ref(&self) -> &Self {
        self
    }
    fn value_mut(&mut self) -> &mut Self {
        self
    }
    fn value(self) -> Self::Value {
        self
    }
}
impl<D, V> Valued for TypedValue<D, V>
where
    V: Valued,
{
    type Value = V::Value;
    fn value_ref(&self) -> &Self::Value {
        &self.value.value_ref()
    }
    fn value_mut(&mut self) -> &mut Self::Value {
        self.value.value_mut()
    }
    fn value(self) -> Self::Value {
        self.value.value()
    }
}
impl<L, V> Valued for Labeled<L, V>
where
    V: Valued,
{
    type Value = V::Value;
    fn value_ref(&self) -> &V::Value {
        self.value.value_ref()
    }
    fn value_mut(&mut self) -> &mut V::Value {
        self.value.value_mut()
    }
    fn value(self) -> V::Value {
        self.value.value()
    }
}

/// Alias for retrieving the Value of a Valued object
pub type ValueOf<T> = <T as Valued>::Value;

pub trait Marked {
    type Marker;
}
impl<L, M> Marked for Labeled<L, PhantomData<M>> {
    type Marker = M;
}
impl<L, D, M> Marked for Labeled<L, TypedValue<D, PhantomData<M>>> {
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

/// `LabelEq`-based membership test
pub trait Member<E> {
    type IsMember: Bit;
}

impl<E> Member<E> for Nil {
    type IsMember = False;
}
impl<E, L, V, T> Member<E> for LVCons<L, V, T>
where
    L: LabelEq<E>,
    T: Member<E>,
    <L as LabelEq<E>>::Eq: BitOr<<T as Member<E>>::IsMember>,
    <<L as LabelEq<E>>::Eq as BitOr<<T as Member<E>>::IsMember>>::Output: Bit,
{
    type IsMember = Or<<L as LabelEq<E>>::Eq, <T as Member<E>>::IsMember>;
}

/// Trait to ensure that all labels in `LabeList` are found in cons-list `Self`.
pub trait HasLabels<LabelList> {}
// Everything as the empty label list
impl<T> HasLabels<Nil> for T {}
// make sure the first label is in the haystack, then move on the to rest of the needles
impl<NeedleLbl, NeedleTail, Haystack> HasLabels<LabelCons<NeedleLbl, NeedleTail>> for Haystack
where
    Haystack: Member<NeedleLbl, IsMember = True>,
    Haystack: HasLabels<NeedleTail>,
{
}
/// Convenience implementation for the case where only a single label is provided
impl<Needle, Haystack> HasLabels<Needle> for Haystack
where
    Needle: Label,
    Haystack: Member<Needle, IsMember = True>,
{
}

/// Marker trait for ensuring that the labels of a cons-list constitute a set (no label cardinality
/// greater than 1).
pub trait IsLabelSet {
    type IsSet;
}
// Empty set
impl IsLabelSet for Nil {
    type IsSet = True;
}
// Cons-list is a label set if head label isn't found in tail, and tail is a label set
impl<L, V, T> IsLabelSet for LVCons<L, V, T>
where
    T: Member<L>,
    <T as Member<L>>::IsMember: Not,
    <<T as Member<L>>::IsMember as Not>::Output: BitAnd<<T as IsLabelSet>::IsSet>,
    T: IsLabelSet,
{
    type IsSet = And<<<T as Member<L>>::IsMember as Not>::Output, <T as IsLabelSet>::IsSet>;
}

/// Lookup into a `Cons`-list by `typenum` natural number.
pub trait LookupElemByNat<N> {
    type Elem;
    fn elem(&self) -> &Self::Elem;
}

impl<H, T> LookupElemByNat<UTerm> for Cons<H, T> {
    type Elem = H;
    fn elem(&self) -> &Self::Elem {
        &self.head
    }
}

impl<H, T> LookupElemByNat<UInt<UTerm, B1>> for Cons<H, T>
where
    T: LookupElemByNat<UTerm>,
{
    type Elem = <T as LookupElemByNat<UTerm>>::Elem;
    fn elem(&self) -> &Self::Elem {
        self.tail.elem()
    }
}

impl<H, T, N> LookupElemByNat<UInt<N, B0>> for Cons<H, T>
where
    N: Sub<B1>,
    T: LookupElemByNat<UInt<Sub1<N>, B1>>,
{
    type Elem = <T as LookupElemByNat<UInt<Sub1<N>, B1>>>::Elem;
    fn elem(&self) -> &Self::Elem {
        self.tail.elem()
    }
}

impl<H, T, N, B> LookupElemByNat<UInt<UInt<N, B>, B1>> for Cons<H, T>
where
    T: LookupElemByNat<UInt<UInt<N, B>, B0>>,
{
    type Elem = <T as LookupElemByNat<UInt<UInt<N, B>, B0>>>::Elem;
    fn elem(&self) -> &Self::Elem {
        self.tail.elem()
    }
}

pub trait LookupNatByLabel<L> {
    type Nat: Unsigned;
    fn nat(&self) -> usize {
        Self::Nat::to_usize()
    }
}
impl<TargetL, L, V, T> LookupNatByLabel<TargetL> for LVCons<L, V, T>
where
    TargetL: LabelEq<L>,
    LVCons<L, V, T>: LookupNatByLabelMatch<TargetL, <TargetL as LabelEq<L>>::Eq>,
{
    type Nat =
        <LVCons<L, V, T> as LookupNatByLabelMatch<TargetL, <TargetL as LabelEq<L>>::Eq>>::Nat;
}

pub trait LookupNatByLabelMatch<TargetL, B> {
    type Nat: Unsigned;
}
impl<TargetL, L, V, T> LookupNatByLabelMatch<TargetL, True> for LVCons<L, V, T> {
    type Nat = UTerm;
}
impl<TargetL, L, V, T> LookupNatByLabelMatch<TargetL, False> for LVCons<L, V, T>
where
    T: LookupNatByLabel<TargetL>,
    <T as LookupNatByLabel<TargetL>>::Nat: Add<B1>,
    <<T as LookupNatByLabel<TargetL>>::Nat as Add<B1>>::Output: Unsigned,
{
    type Nat = Add1<<T as LookupNatByLabel<TargetL>>::Nat>;
}

pub trait LookupElemByLabel<L> {
    type Elem;
    fn elem(&self) -> &Self::Elem;
}
impl<L, T> LookupElemByLabel<L> for T
where
    T: LookupNatByLabel<L>,
    T: LookupElemByNat<<T as LookupNatByLabel<L>>::Nat>,
{
    type Elem = <Self as LookupElemByNat<<Self as LookupNatByLabel<L>>::Nat>>::Elem;
    fn elem(&self) -> &Self::Elem {
        LookupElemByNat::<_>::elem(self)
    }
}
pub type ElemOf<T, Label> = <T as LookupElemByLabel<Label>>::Elem;

pub trait LookupValuedElemByLabel<L>: LookupElemByLabel<L> {
    type Elem: Valued;
    fn elem(&self) -> &<Self as LookupValuedElemByLabel<L>>::Elem;
}
impl<T, L> LookupValuedElemByLabel<L> for T
where
    T: LookupElemByLabel<L>,
    ElemOf<Self, L>: Valued,
{
    type Elem = <Self as LookupElemByLabel<L>>::Elem;
    fn elem(&self) -> &<Self as LookupElemByLabel<L>>::Elem {
        <Self as LookupElemByLabel<L>>::elem(self)
    }
}
pub type ValuedElemOf<T, Label> = <T as LookupValuedElemByLabel<Label>>::Elem;
pub type ValueOfElemOf<T, Label> = <<T as LookupValuedElemByLabel<Label>>::Elem as Valued>::Value;

pub trait LookupMarkedElemByLabel<L>: LookupElemByLabel<L> {
    type Elem: Marked;
}
impl<T, L> LookupMarkedElemByLabel<L> for T
where
    T: LookupElemByLabel<L>,
    ElemOf<Self, L>: Marked,
{
    type Elem = <Self as LookupElemByLabel<L>>::Elem;
}
pub type MarkedElemOf<T, Label> = <T as LookupMarkedElemByLabel<Label>>::Elem;
pub type MarkerOfElemOf<T, Label> = <<T as LookupMarkedElemByLabel<Label>>::Elem as Marked>::Marker;

pub trait LookupTypedElemByLabel<L>: LookupElemByLabel<L> {
    type Elem: Typed;
}
impl<T, L> LookupTypedElemByLabel<L> for T
where
    T: LookupElemByLabel<L>,
    ElemOf<Self, L>: Typed,
{
    type Elem = <Self as LookupElemByLabel<L>>::Elem;
}
pub type TypedElemOf<T, Label> = <T as LookupTypedElemByLabel<Label>>::Elem;
pub type TypeOfElemOf<T, Label> = <<T as LookupTypedElemByLabel<Label>>::Elem as Typed>::DType;

/// Trait to find the subset of cons-list `Self` which are labeled with labels in `LabelList`.
///
/// Any labels in `LabelList` not found in `Self` will be ignored (see `HasLabels` for a trait
/// that requires all members of `LabelList` to be found).
pub trait LabelFilter<LabelList> {
    type Output;
}

// End-point. No more list elements to search. We don't care if anything remains or not in
// `LabelList`.
impl<LabelList> LabelFilter<LabelList> for Nil {
    type Output = Nil;
}

// Implementation for `LVCons` cons-lists.
impl<LabelList, L, V, T> LabelFilter<LabelList> for LVCons<L, V, T>
where
    LabelList: Member<L>,
    LVCons<L, V, T>: LabelFilterPred<LabelList, <LabelList as Member<L>>::IsMember>,
{
    type Output = <LVCons<L, V, T> as LabelFilterPred<
        LabelList,
        <LabelList as Member<L>>::IsMember,
    >>::Output;
}

/// Helper filter trait. Used by `Filter` for computing the subset of `Self` cons-list which
/// contains the labels in `LabelList`.
///
/// `IsMember` specifies whether or not the label of the head value of `Self` is a member of
/// `LabelList`.
pub trait LabelFilterPred<LabelList, IsMember> {
    type Output;
}

// `LabelFilterPred` implementation for a cons-list where the head is in `LabelList`.
impl<LabelList, H, T> LabelFilterPred<LabelList, True> for Cons<H, T>
where
    T: LabelFilter<LabelList>,
{
    // head is in list, so we include it and check the tail
    type Output = Cons<H, <T as LabelFilter<LabelList>>::Output>;
}
// `LabelFilterPred` implementation for a cons-list where the head isn't in `LabelList`.
impl<LabelList, H, T> LabelFilterPred<LabelList, False> for Cons<H, T>
where
    T: LabelFilter<LabelList>,
{
    // head isn't in list, so we check the tail
    type Output = <T as LabelFilter<LabelList>>::Output;
}

/// Trait to find the subset of cons-list `Self` which are labeled with labels in `LabelList`,
/// and applying a method to each element of that list.
///
/// Any labels in `LabelList` not found in `Self` will be ignored (see `HasLabels` for a trait
/// that requires all members of `LabelList` to be found).
pub trait FilterApply<LabelList, FArgs, FOut> {
    type Output;

    fn filter_apply<F>(&self, f: F) -> Self::Output where F: Clone + FnOnce(&FArgs) -> FOut;
}

// Base-case (Nil) implementation
impl<LabelList, FArgs, FOut> FilterApply<LabelList, FArgs, FOut> for Nil {
    type Output = Nil;

    fn filter_apply<F>(&self, _f: F) -> Nil where F: Clone + FnOnce(&FArgs) -> FOut {
        Nil
    }
}

// Implementation for `LVCons` cons-lists.
impl<LabelList, FArgs, FOut, L, V, T> FilterApply<LabelList, FArgs, FOut> for LVCons<L, V, T>
where
    LabelList: Member<L>,
    LVCons<L, V, T>: FilterApplyPred<LabelList, FArgs, FOut, <LabelList as Member<L>>::IsMember>,
{
    type Output = <LVCons<L, V, T> as FilterApplyPred<
        LabelList,
        FArgs,
        FOut,
        <LabelList as Member<L>>::IsMember,
    >>::Output;

    fn filter_apply<F>(&self, f: F) -> Self::Output where F: Clone + FnOnce(&FArgs) -> FOut {
        self.filter_apply_pred(f)
    }
}

/// Helper filter trait. Used by `Filter` for computing the subset of `Self` cons-list which
/// contains the labels in `LabelList`, and cloning a copy of that subset.
///
/// `IsMember` specifies whether or not the label of the head value of `Self` is a member of
/// `LabelList`.
pub trait FilterApplyPred<LabelList, FArgs, FOut, IsMember> {
    type Output;

    fn filter_apply_pred<F>(&self, f: F) -> Self::Output where F: Clone + FnOnce(&FArgs) -> FOut;
}

// `FilterApplyPred` implementation for a cons-list where the head is in `LabelList`.
impl<LabelList, FOut, H, T> FilterApplyPred<LabelList, H, FOut, True> for Cons<H, T>
where
    T: FilterApply<LabelList, H, FOut>,
{
    // head is in list, so we include it and check the tail
    type Output = Cons<FOut, <T as FilterApply<LabelList, H, FOut>>::Output>;

    fn filter_apply_pred<F>(&self, f: F) -> Self::Output where F: Clone + FnOnce(&H) -> FOut {
        Cons {
            head: f.clone()(&self.head),
            tail: self.tail.filter_apply(f),
        }
    }
}
// `FilterPred` implementation for a cons-list where the head isn't in `LabelList`.
impl<LabelList, FOut, H, T> FilterApplyPred<LabelList, H, FOut, False> for Cons<H, T>
where
    T: FilterApply<LabelList, H, FOut>,
{
    // head isn't in list, so we check the tail
    type Output = <T as FilterApply<LabelList, H, FOut>>::Output;

    fn filter_apply_pred<F>(&self, f: F) -> Self::Output where F: Clone + FnOnce(&H) -> FOut {
        self.tail.filter_apply(f)
    }
}

/// Convenience trait for cloning the values of a cons-list which match a specified `LabelList`.
///
/// Any labels in `LabelList` not found in `Self` will be ignored (see `HasLabels` for a trait
/// that requires all members of `LabelList` to be found).
pub trait FilterClone<LabelList> {
    type Output;

    /// Filters `Self` and clones into new cons-list of associated type `Output`.
    fn filter_clone(&self) -> Self::Output;
}

impl<LabelList> FilterClone<LabelList> for Nil
{
    type Output = Nil;
    fn filter_clone(&self) -> Nil {
        Nil
    }
}

impl<LabelList, L, V, T> FilterClone<LabelList> for LVCons<L, V, T>
where
    LabelList: Member<L>,
    Labeled<L, V>: Clone,
    Self: FilterApply<LabelList, Labeled<L, V>, Labeled<L, V>>
{
    type Output = <Self as FilterApply<LabelList, Labeled<L, V>, Labeled<L, V>>>::Output;

    fn filter_clone(&self) -> Self::Output {
        self.filter_apply(|&ref h| h.clone())
    }
}







pub trait AssocLabels {
    type Labels;
}
impl<Label, Value, Tail> AssocLabels for LVCons<Label, Value, Tail>
where
    Tail: AssocLabels,
{
    type Labels = LabelCons<Label, Tail::Labels>;
}
impl AssocLabels for Nil {
    type Labels = Nil;
}

//TODO: figure out how to have this return an array
pub trait StrLabels {
    fn labels<'a>() -> VecDeque<&'a str>;
}
impl StrLabels for Nil {
    fn labels<'a>() -> VecDeque<&'a str> {
        VecDeque::new()
    }
}
impl<L, V, T> StrLabels for LVCons<L, V, T>
where
    L: LabelName,
    T: StrLabels,
{
    fn labels<'a>() -> VecDeque<&'a str> {
        let mut previous = T::labels();
        previous.push_front(L::name());
        previous
    }
}

pub trait StrTypes {
    fn str_types<'a>() -> VecDeque<&'a str>;
}
impl StrTypes for Nil {
    fn str_types<'a>() -> VecDeque<&'a str> {
        VecDeque::new()
    }
}
impl<L, V, T> StrTypes for LVCons<L, V, T>
where
    L: LabelName,
    T: StrTypes,
{
    fn str_types<'a>() -> VecDeque<&'a str> {
        let mut previous = T::str_types();
        previous.push_front(L::str_type());
        previous
    }
}

#[macro_export]
macro_rules! namespace {
    (@fields() -> ($($out:tt)*)) => {
        declare_fields![Namespace; $($out)*];
        pub type Fields = Fields![$($out)*];
    };

    (@fields
        (field $field_name:ident: $field_ty:ident = $str_name:expr; $($rest:tt)*)
        ->
        ($($out:tt)*)
    ) => {
        namespace![@fields
            ($($rest)*)
            ->
            ($($out)* $field_name: $field_ty = $str_name,)
        ];
    };
    (@fields
        (field $field_name:ident: $field_ty:ident; $($rest:tt)*)
        ->
        ($($out:tt)*)
    ) => {
        namespace![@fields
            ($($rest)*)
            ->
            ($($out)* $field_name: $field_ty = stringify![$field_name],)
        ];
    };

    (@body($($body:tt)*)) => {
        namespace![@fields($($body)*) -> ()];
    };

    ($vis:vis namespace $ns_name:ident: $prev_ns:ident {
        $($body:tt)*
    }) => {
        $vis mod $ns_name
        {
            #![allow(dead_code)]
            use super::$prev_ns;
            pub type Namespace = typenum::Add1<$prev_ns::Namespace>;
            pub type Store = $crate::store::DataStore<Fields>;
            pub type DataStore = Store;
            pub type View = <Store as $crate::store::IntoView>::Output;
            pub type DataView = View;

            namespace![@body($($body)*)];
        }
    };
    ($vis:vis namespace $ns_name:ident {
        $($body:tt)*
    }) => {
        $vis mod $ns_name
        {
            #![allow(dead_code)]
            pub type Namespace = typenum::U0;
            pub type Store = $crate::store::DataStore<Fields>;
            pub type DataStore = Store;
            pub type View = <Store as $crate::store::IntoView>::Output;
            pub type DataView = View;

            namespace![@body($($body)*)];
        }
    };
}

macro_rules! nat_label {
    ($label:ident, $ns:ty, $nat:ty, $dtype:ty, $name:expr) => {
        #[derive(Debug, Clone)]
        pub struct $label;

        impl $crate::label::Identifier for $label {
            type Ident = $crate::label::Ident<$ns, $nat>;
            type Namespace = $ns;
            type Natural = $nat;
        }
        impl $crate::label::Label for $label {
            const NAME: &'static str = $name;
            const TYPE: &'static str = stringify![$dtype];
        }
        impl $crate::label::Typed for $label {
            type DType = $dtype;
        }
    };
}

#[macro_export]
macro_rules! first_label {
    ($label:ident, $ns:ty, $dtype:ty) => {
        first_label![$label, $ns, $dtype, stringify![$label]];
    };
    ($label:ident, $ns:ty, $dtype:ty, $name:expr) => {
        nat_label![$label, $ns, typenum::consts::U0, $dtype, $name];
    };
}

#[macro_export]
macro_rules! next_label {
    ($label:ident, $prev:ident, $dtype:ty) => {
        next_label![$label, $prev, $dtype, stringify![$label]];
    };
    ($label:ident, $prev:ident, $dtype:ty, $name:expr) => {
        nat_label![
            $label,
            $crate::label::NsOf<$prev>,
            typenum::Add1<$crate::label::NatOf<$prev>>,
            $dtype,
            $name
        ];
    };
}

#[macro_export]
macro_rules! Labels {
    (@labels()) => { Nil };
    (@labels($label:ident, $($rest:ident,)*)) =>
    {
        LCons<$label, Labels![@labels($($rest,)*)]>
    };
    ($($label:ident),*$(,)*) =>
    {
        Labels![@labels($($label,)*)]
    }
}

#[macro_export]
macro_rules! declare_fields
{
    // end case
    (@step($ns:ty)($prev_label:ident)()) => {};

    // non-initial label
    (@step
        ($ns:ty)
        ($prev_label:ident)
        ($label:ident: $dtype:ident = $name:expr, $($rest:tt)*)
    )
        =>
    {
        next_label![$label, $prev_label, $dtype, $name];
        declare_fields![@step
            ($ns)
            ($label)
            ($($rest)*)
        ];
    };
    // handle non-trailing comma
    (@step($ns:ty)($prev_label:ident)($label:ident: $dtype:ident = $name:expr))
        =>
    {
        declare_fields![@step($ns)($prev_label)($label: $dtype,)]
    };

    // initial label
    (@start
        ($ns:ty)
        ($label:ident: $dtype:ident = $name:expr, $($rest:tt)*)
    )
        =>
    {
        first_label![$label, $ns, $dtype, $name];
        declare_fields![@step
            ($ns)
            ($label)
            ($($rest)*)
        ];
    };
    // handle non-trailing comma
    (@start($ns:ty)($label:ident: $dtype:ident = $name:expr))
        =>
    {
        declare_fields![@step($ns)($label: $dtype = $name,)]
    };

    // entry point
    ($ns:ty; $($fields:tt)*) => {
        declare_fields![@start($ns)($($fields)*)];
    };
}

#[macro_export]
macro_rules! Fields {
    (@fields()) => { $crate::cons::Nil };
    (@fields(
        $label:ident: $dtype:ident $(= $name:expr)*,
        $($rest_label:ident: $rest_dtype:ident $(= $rest_name:expr)*,)*)
    )
        =>
    {
        $crate::fieldlist::FieldCons<
            $label,
            $dtype,
            Fields![@fields($($rest_label: $rest_dtype,)*)]
        >
    };
    ($($label:ident: $dtype:ident $(= $name:expr)*),*$(,)*) =>
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
mod tests {
    use super::*;
    use cons::*;
    use typenum::{
        consts::{U0, U1, U2, U3, U4},
        Bit,
    };

    pub type SampleNamespace = U0;
    first_label![ImALabel, U0, u64];
    next_label![ImAnotherLabel, ImALabel, u64];

    #[test]
    fn type_eq() {
        assert!(<U1 as IdentEq<U1>>::Eq::to_bool());
        assert!(!<U1 as IdentEq<U4>>::Eq::to_bool());
        assert!(<ImALabel as LabelEq<ImALabel>>::Eq::to_bool());
        assert!(!<ImALabel as LabelEq<ImAnotherLabel>>::Eq::to_bool());
    }

    pub type NumberNamespace = Add1<SampleNamespace>;
    first_label![F0, NumberNamespace, u64];
    next_label![F1, F0, f64];
    next_label![F2, F1, i64];
    next_label![F3, F2, String];
    next_label![F4, F3, f32];
    next_label![F5, F4, f32];
    next_label![F6, F5, f32];
    next_label![F7, F6, f32];

    #[test]
    fn lookup() {
        let list = LVCons {
            head: Labeled::<F0, _>::from(6u64),
            tail: LVCons {
                head: Labeled::<F1, _>::from(5.3f64),
                tail: LVCons {
                    head: Labeled::<F2, _>::from(-3i64),
                    tail: LVCons {
                        head: Labeled::<F3, _>::from("Hello".to_string()),
                        tail: LVCons {
                            head: Labeled::<F4, _>::from(3.2f32),
                            tail: Nil,
                        },
                    },
                },
            },
        };

        assert_eq!(LookupElemByNat::<U0>::elem(&list).value, 6u64);
        assert_eq!(LookupElemByNat::<U1>::elem(&list).value, 5.3);
        assert_eq!(LookupElemByNat::<U2>::elem(&list).value, -3i64);
        assert_eq!(
            LookupElemByNat::<U3>::elem(&list).value,
            "Hello".to_string()
        );
        assert_eq!(LookupElemByNat::<U4>::elem(&list).value, 3.2f32);

        assert_eq!(LookupNatByLabel::<F0>::nat(&list), 0);
        assert_eq!(LookupNatByLabel::<F1>::nat(&list), 1);
        assert_eq!(LookupNatByLabel::<F2>::nat(&list), 2);
        assert_eq!(LookupNatByLabel::<F3>::nat(&list), 3);
        assert_eq!(LookupNatByLabel::<F4>::nat(&list), 4);

        assert_eq!(LookupElemByLabel::<F0>::elem(&list).value, 6u64);
        assert_eq!(LookupElemByLabel::<F1>::elem(&list).value, 5.3);
        assert_eq!(LookupElemByLabel::<F2>::elem(&list).value, -3i64);
        assert_eq!(
            LookupElemByLabel::<F3>::elem(&list).value,
            "Hello".to_string()
        );
        assert_eq!(LookupElemByLabel::<F4>::elem(&list).value, 3.2f32);

        let list = LVCons {
            head: Labeled::<F5, _>::from(3u32),
            tail: list,
        };

        assert_eq!(LookupNatByLabel::<F0>::nat(&list), 1);
        assert_eq!(LookupNatByLabel::<F1>::nat(&list), 2);
        assert_eq!(LookupNatByLabel::<F2>::nat(&list), 3);
        assert_eq!(LookupNatByLabel::<F3>::nat(&list), 4);
        assert_eq!(LookupNatByLabel::<F4>::nat(&list), 5);
        assert_eq!(LookupNatByLabel::<F5>::nat(&list), 0);

        assert_eq!(LookupElemByLabel::<F0>::elem(&list).value, 6u64);
        assert_eq!(LookupElemByLabel::<F1>::elem(&list).value, 5.3);
        assert_eq!(LookupElemByLabel::<F2>::elem(&list).value, -3i64);
        assert_eq!(
            LookupElemByLabel::<F3>::elem(&list).value,
            "Hello".to_string()
        );
        assert_eq!(LookupElemByLabel::<F4>::elem(&list).value, 3.2f32);
        assert_eq!(LookupElemByLabel::<F5>::elem(&list).value, 3u32);
    }

    #[test]
    fn filter() {
        type SampleLabels = LVCons<
            F0,
            u64,
            LVCons<F1, f64, LVCons<F2, i64, LVCons<F3, String, LVCons<F4, f32, Nil>>>>,
        >;

        {
            // null case
            type Filtered = <SampleLabels as LabelFilter<Labels![]>>::Output;
            // empty filter, length should be 0
            assert_eq!(length![Filtered], 0);
        }
        {
            // other null case
            type Filtered = <Nil as LabelFilter<Labels![F1, F3]>>::Output;
            // empty cons-list, so filtered length should be 0
            assert_eq!(length![Filtered], 0);
        }
        {
            type Filtered = <SampleLabels as LabelFilter<Labels![F3]>>::Output;
            // we only filtered 1 label, so length should be 1
            assert_eq!(length![Filtered], 1);
        }
        {
            type Filtered = <SampleLabels as LabelFilter<Labels![F1, F2, F4]>>::Output;
            // we only filtered 3 labels, so length should be 3
            assert_eq!(length![Filtered], 3);

            {
                type Refiltered = <Filtered as LabelFilter<Labels![F1, F2, F4]>>::Output;
                // filtered same labels, so length should stay at 3
                assert_eq!(length![Refiltered], 3);
            }
            {
                type Refiltered = <Filtered as LabelFilter<Labels![F1, F2]>>::Output;
                // filtered 2 labels that should exist `Filtered`, so length should be 2
                assert_eq!(length![Refiltered], 2);
            }
            {
                type Refiltered = <Filtered as LabelFilter<Labels![F3, F0]>>::Output;
                // filtered 2 labels that should not exist `Filtered`, so length should be 0
                assert_eq!(length![Refiltered], 0);
            }
            {
                type Refiltered = <Filtered as LabelFilter<Labels![F0, F1, F2, F3, F4]>>::Output;
                // `F0 and `F3` don't exist in `Filtered`, so length should be 3
                assert_eq!(length![Refiltered], 3);
            }
        }
        {
            type Filtered = <SampleLabels as LabelFilter<Labels![F1, F2, F4, F5]>>::Output;
            // F5 doesn't exist in SampleLabels, so we still should only have 3
            assert_eq!(length![Filtered], 3);
        }
        {
            type Filtered = <SampleLabels as LabelFilter<Labels![F5, F6, F7]>>::Output;
            // None of these labels exist in SampleLabels, so we should have 0
            assert_eq!(length![Filtered], 0);
        }
        {
            // check for problems cause by duplicated in label list
            type Filtered = <SampleLabels as LabelFilter<Labels![F2, F2, F2]>>::Output;
            // we only filtered 1 label (even if it was duplicated), so length should be 1
            assert_eq!(length![Filtered], 1);
        }
        {
            // check for problems cause by duplicated in label list
            type Filtered = <SampleLabels as LabelFilter<Labels![F2, F2, F3]>>::Output;
            // we only filtered 2 label (albeit with some duplication), so length should be 2
            assert_eq!(length![Filtered], 2);
        }
    }
}
