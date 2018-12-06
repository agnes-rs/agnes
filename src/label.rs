use std::collections::VecDeque;
use std::rc::Rc;
use std::ops::{BitAnd, BitOr, Not, Sub, Add};
use std::marker::PhantomData;

use typenum::{
    type_operators::{IsEqual},
    marker_traits::{Bit},
    operator_aliases::{And, Or, Sub1, Add1},
    bit::{B1, B0},
    uint::{Unsigned, UInt, UTerm}
};

use cons::{Cons, Nil};

// /// A label for a value in an `LVCons`. Backed by a type-level natural number `Idx`.
// #[derive(Debug, Clone)]
// pub struct Label<Idx, Name>
// {
//     index: Idx,
//     _name: PhantomData<Name>
// }

// pub trait Id
// {

//     type NsNat; // = NsNatural<Namespace, Natural>;
// }

pub trait Label: Identifier
{
    // type Ident: Identifier; // = Ident<Self::Namespace, Self::Natural>;
    const NAME: &'static str;
}
// pub type LblNsOf<Lbl> = NsOf<<Lbl as Label>::Ident>;
// pub type LblNatOf<Lbl> = NatOf<<Lbl as Label>::Ident>;



/// A label for a value in an `LVCons` within a specific namespace `NS`. Backed by a type-level
/// natural number `N`.
#[derive(Debug, Clone)]
pub struct Ident<Ns, Nat>
{
    _marker: PhantomData<(Ns, Nat)>,
}

pub trait Identifier
{
    type Ident: Identifier; // = Ident<Self::Namespace, Self::Natural>;
    type Namespace;
    type Natural;
}
impl<Ns, Nat> Identifier
    for Ident<Ns, Nat>
{
    type Ident = Self;
    type Namespace = Ns;
    type Natural = Nat;
}
pub type NsOf<T> = <T as Identifier>::Namespace;
pub type NatOf<T> = <T as Identifier>::Natural;


impl Identifier for UTerm
{
    type Ident = Ident<Self::Namespace, Self::Natural>;
    type Namespace = LocalNamespace;
    type Natural = Self;
}
impl<U, B> Identifier for UInt<U, B>
{
    type Ident = Ident<Self::Namespace, Self::Natural>;
    type Namespace = LocalNamespace;
    type Natural = Self;
}

pub trait LabelName
{
    fn name() -> &'static str;
}
impl<T> LabelName
    for T
    where
        T: Label
{
    fn name() -> &'static str { T::NAME }
}

// impl<Idx, Name> LabelName for Label<Idx, Name>
//     where Name: LabelName
// {
//     fn name() -> &'static str { Name::name() }
// }

// pub trait LabelIndex
// {
//     type Idx;
// }
// impl<T> LabelIndex
//     for T
//     where
//         T: Label
// {
//     type Idx = <T as Identifier>::Natural;
// }
// // impl<Idx, Name> LabelIndex for Label<Idx, Name>
// // {
// //     type Idx = Idx;
// // }
// impl<Label, Value, Tail> LabelIndex for LVCons<Label, Value, Tail>
//     where Label: LabelIndex
// {
//     type Idx = Label::Idx;
// }

// pub type NextLabelIndex<T> = Add1<<T as LabelIndex>::Idx>;








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
    where T: IsEqual<U>
{
    type Eq = <T as IsEqual<U>>::Output;
}

/// Type-level equality implementation for `Ident`s. Result will be `True` if both namespace and
/// the type-level natural number backing this label match.
impl<TNs, TNat, UNs, UNat> IdentEq<Ident<UNs, UNat>>
    for Ident<TNs, TNat>
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
impl IsEqual<LocalNamespace>
    for LocalNamespace
{
    type Output = True;
    fn is_equal(self, _rhs: LocalNamespace) -> True { B1 }
}

pub trait LabelEq<U>
{
    type Eq;
}
impl<T, U> LabelEq<U>
    for T
    where
        T: Identifier,
        U: Identifier,
        T::Ident: IdentEq<U::Ident>
{
    type Eq = <T::Ident as IdentEq<U::Ident>>::Eq;
}

// /// Equate Labels to backing naturals
// impl<T> LabelEq<UTerm>
//     for T
//     where
//         T: Label,
//         NatOf<T>: LabelEq<UTerm>
// {
//     type Eq = <NatOf<T> as LabelEq<UTerm>>::Eq;
// }
// impl<T, U> LabelEq<UInt<U, B0>>
//     for T
//     where
//         T: Label,
//         NatOf<T>: LabelEq<UInt<U, B0>>
// {
//     type Eq = <NatOf<T> as LabelEq<UInt<U, B0>>>::Eq;
// }
// impl<T, U> LabelEq<UInt<U, B1>>
//     for T
//     where
//         T: Label,
//         NatOf<T>: LabelEq<UInt<U, B1>>
// {
//     type Eq = <NatOf<T> as LabelEq<UInt<U, B1>>>::Eq;
// }


// impl<U> LabelEq<U>
//     for UTerm
//     where
//         U: Label,
//         UTerm: LabelEq<NatOf<U>>
// {
//     type Eq = <UTerm as LabelEq<NatOf<U>>>::Eq;
// }
// impl<T, U> LabelEq<U>
//     for UInt<T, B0>
//     where
//         U: Label,
//         UInt<T, B0>: LabelEq<NatOf<U>>
// {
//     type Eq = <UInt<T, B0> as LabelEq<NatOf<U>>>::Eq;
// }
// impl<T, U> LabelEq<U>
//     for UInt<T, B1>
//     where
//         U: Label,
//         UInt<T, B1>: LabelEq<NatOf<U>>
// {
//     type Eq = <UInt<T, B1> as LabelEq<NatOf<U>>>::Eq;
// }


// /// Two labels are identical if their identifier are identical
// pub trait LabelEq<Other>
// {
//     type Eq: Bit;
// }

// impl<TLbl, ULbl> LabelEq<ULbl>
//     for TLbl
//     where
//         TLbl: Identifier,
//         ULbl: Identifier,
//         TLbl: IdentEq<ULbl>,
// {
//     type Eq = <TLbl as IdentEq<ULbl>>::Eq;
// }




#[derive(Debug, Clone)]
pub struct Labeled<L, V>
{
    _label: PhantomData<L>,
    pub value: V
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
// Everything as the empty label list
impl<T> HasLabels<Nil> for T
{}
// make sure the first label is in the haystack, then move on the to rest of the needles
impl<NeedleLbl, NeedleTail, Haystack> HasLabels<LabelCons<NeedleLbl, NeedleTail>>
    for Haystack
    where Haystack: Member<NeedleLbl, IsMember=True>,
          Haystack: HasLabels<NeedleTail>,
{}
/// Convenience implementation for the case where only a single label is provided
impl<Needle, Haystack> HasLabels<Needle>
    for Haystack
    where
        Needle: Label,
        Haystack: Member<Needle, IsMember=True>,
{}

/// Marker trait for ensuring that the labels of a cons-list constitute a set (no label cardinality
/// greater than 1).
pub trait IsLabelSet
{
    type IsSet;
}
// Empty set
impl IsLabelSet
    for Nil
{
    type IsSet = True;
}
// Cons-list is a label set if head label isn't found in tail, and tail is a label set
impl<L, V, T> IsLabelSet
    for LVCons<L, V, T>
    where
        T: Member<L>,
        <T as Member<L>>::IsMember: Not,
        <<T as Member<L>>::IsMember as Not>::Output: BitAnd<<T as IsLabelSet>::IsSet>,
        T: IsLabelSet,
{
    type IsSet = And<
        <<T as Member<L>>::IsMember as Not>::Output,
        <T as IsLabelSet>::IsSet,
    >;
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




// #[macro_export]
// macro_rules! first_ns {
//     ($name:ident) => {
//         type $name = typenum::U0;
//     }
// }

// #[macro_export]
// macro_rules! ns {
//     ($name:ident) => {
//         first_ns![$name];
//     };
//     ($name:ident, $prev:ident) => {
//         type $name = typenum::Add1<$prev>;
//     }
// }

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
        // $(field $field_name:ident: $field_ty:ident;)*
        $($body:tt)*
    }) => {
        $vis mod $ns_name
        {
            #![allow(dead_code)]
            use super::$prev_ns;
            pub type Namespace = typenum::Add1<$prev_ns::Namespace>;
            pub type Store = $crate::store::DataStore<Fields>;
            pub type DataStore = Store;

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

            namespace![@body($($body)*)];
        }
    };
}


macro_rules! nat_label
{
    ($label:ident, $ns:ty, $nat:ty, $dtype:ty, $name:expr) => {
        #[derive(Debug, Clone)]
        pub struct $label;

        impl $crate::label::Identifier for $label
        {
            type Ident = $crate::label::Ident<$ns, $nat>;
            type Namespace = $ns;
            type Natural = $nat;
        }
        impl $crate::label::Label for $label
        {
            const NAME: &'static str = $name;
        }
        impl $crate::label::Typed for $label
        {
            type DType = $dtype;
        }
    }
}


#[macro_export]
macro_rules! first_label {
    ($label:ident, $ns:ty, $dtype:ty) => {
        first_label![$label, $ns, $dtype, stringify![$label]];
    };
    ($label:ident, $ns:ty, $dtype:ty, $name:expr) => {
        nat_label![$label, $ns, typenum::consts::U0, $dtype, $name];
    }
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
    }
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

// #[macro_export]
// macro_rules! declare_fields {
//     (@step($ns:ty)($prev_label:ident)()) => {
//     };
//     (@step
//         ($ns:ty)
//         ($prev_label:ident)
//         ($label:ident: $dtype:ident, $($rest_label:ident: $rest_dtype:ident,)*)
//     )
//         =>
//     {
//         next_label![$label, $prev_label, $dtype, $name];
//         declare_fields![@step
//             ($ns)
//             ($label)
//             ($($rest_label: $rest_dtype,)*)
//         ];
//     };

//     (@start
//         ($ns:ty)
//         ($label:ident: $dtype:ident, $($rest_label:ident: $rest_dtype:ident,)*)
//     )
//         =>
//     {
//         first_label![$label, $ns, $dtype, $name];
//         declare_fields![@step
//             ($ns)
//             ($label)
//             ($($rest_label: $rest_dtype,)*)
//         ];
//     };

//     ($ns:ty; $($label:ident: $dtype:ident),*$(,)*) =>
//     {
//         declare_fields![$ns; $($label: $dtype = stringify![$label],)*];
//     };
//     ($ns:ty; $($label:ident: $dtype:ident = $name:expr),*$(,)*) =>
//     {
//         declare_fields![@start($ns)($($label: $dtype,)*)];
//     };
// }

#[macro_export]
macro_rules! declare_fields
{
    // end case
    (@step($ns:ty)($prev_label:ident)()) => {};

    // non-initial label with name string
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
    // // non-initial label without name string
    // (@step
    //     ($ns:ty)
    //     ($prev_label:ident)
    //     ($label:ident: $dtype:ident, $($rest:tt)*)
    // )
    //     =>
    // {
    //     next_label![$label, $prev_label, $dtype, stringify![$label]];
    //     declare_fields![@step
    //         ($ns)
    //         ($label)
    //         ($($rest)*)
    //     ];
    // };
    // // handle non-trailing comma
    // (@step($ns:ty)($prev_label:ident)($label:ident: $dtype:ident))
    //     =>
    // {
    //     declare_fields![@step($ns)($prev_label)($label: $dtype,)];
    // };


    // initial label with name string
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
    // // initial label, no name string
    // (@start
    //     ($ns:ty)
    //     ($label:ident: $dtype:ident, $($rest:tt)*)
    // )
    //     =>
    // {
    //     first_label![$label, $ns, $dtype, stringify![$label]];
    //     declare_fields![@step
    //         ($ns)
    //         ($label)
    //         ($($rest)*)
    //     ];
    // };
    // // handle non-trailing comma
    // (@start($ns:ty)($label:ident: $dtype:ident))
    //     =>
    // {
    //     declare_fields![@step($ns)($label: $dtype,)]
    // };

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
mod tests
{
    use cons::*;
    use super::*;
    use typenum::{
        Bit,
        consts::{U0, U1, U2, U3, U4}
    };

    pub type SampleNamespace = U0;
    first_label![ImALabel, U0, u64];
    next_label![ImAnotherLabel, ImALabel, u64];

    // #[allow(non_snake_case)]
    // pub mod ImALabel
    // {
    //     use typenum::U0;
    //     // use super::SampleNamespace;

    //     #[derive(Debug, Clone)]
    //     pub struct Name;
    //     pub const NAME: &'static str = stringify![ImALabel];
    //     impl ::label::LabelName for Name
    //     {
    //         fn name() -> &'static str { NAME }
    //     }

    //     pub type Natural = U0;
    //     pub type Label = ::label::Label<Natural, Name>;
    //     pub type Namespace = super::SampleNamespace;
    // }

    #[test]
    fn type_eq()
    {
        use typenum::U0;
        assert!(<U1 as IdentEq<U1>>::Eq::to_bool());
        assert!(!<U1 as IdentEq<U4>>::Eq::to_bool());

        // first_label![ImALabel, SampleNamespace];
        // next_label![ImAnotherLabel, ImALabel];
        // type ImALabel = Label<U0>;
        // type ImAnotherLabel = Label<U1>;

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
    fn lookup()
    {
        // label![F0, U0];
        // label![F1, U1];
        // label![F2, U2];
        // label![F3, U3];
        // label![F4, U4];

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

        assert_eq!(LookupNatByLabel::<F0>::nat(&list), 0);
        assert_eq!(LookupNatByLabel::<F1>::nat(&list), 1);
        assert_eq!(LookupNatByLabel::<F2>::nat(&list), 2);
        assert_eq!(LookupNatByLabel::<F3>::nat(&list), 3);
        assert_eq!(LookupNatByLabel::<F4>::nat(&list), 4);

        assert_eq!(LookupElemByLabel::<F0>::elem(&list).value, 6u64);
        assert_eq!(LookupElemByLabel::<F1>::elem(&list).value, 5.3);
        assert_eq!(LookupElemByLabel::<F2>::elem(&list).value, -3i64);
        assert_eq!(LookupElemByLabel::<F3>::elem(&list).value, "Hello".to_string());
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
        assert_eq!(LookupElemByLabel::<F3>::elem(&list).value, "Hello".to_string());
        assert_eq!(LookupElemByLabel::<F4>::elem(&list).value, 3.2f32);
        assert_eq!(LookupElemByLabel::<F5>::elem(&list).value, 3u32);
    }

    #[test]
    fn filter()
    {
        // label![F0, U0];
        // label![F1, U1];
        // label![F2, U2];
        // label![F3, U3];
        // label![F4, U4];

        type SampleLabels =
            LVCons<
                F0, u64,
                LVCons<
                    F1, f64,
                    LVCons<
                        F2, i64,
                        LVCons<
                            F3, String,
                            LVCons<
                                F4, f32,
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
            assert_eq!(length![Filtered], 0);
        }
        {
            // other null case
            type Filtered = <Nil as Filter<Labels![F1, F3]>>::Filtered;
            // empty cons-list, so filtered length should be 0
            assert_eq!(length![Filtered], 0);
        }
        {
            type Filtered = <SampleLabels as Filter<Labels![F3]>>::Filtered;
            // we only filtered 1 label, so length should be 1
            assert_eq!(length![Filtered], 1);
        }
        {
            type Filtered = <SampleLabels as Filter<Labels![F1, F2, F4]>>::Filtered;
            // we only filtered 3 labels, so length should be 3
            assert_eq!(length![Filtered], 3);

            {
                type Refiltered = <Filtered as Filter<Labels![F1, F2, F4]>>::Filtered;
                // filtered same labels, so length should stay at 3
                assert_eq!(length![Refiltered], 3);
            }
            {
                type Refiltered = <Filtered as Filter<Labels![F1, F2]>>::Filtered;
                // filtered 2 labels that should exist `Filtered`, so length should be 2
                assert_eq!(length![Refiltered], 2);
            }
            {
                type Refiltered = <Filtered as Filter<Labels![F3, F0]>>::Filtered;
                // filtered 2 labels that should not exist `Filtered`, so length should be 0
                assert_eq!(length![Refiltered], 0);
            }
            {
                type Refiltered = <Filtered as Filter<Labels![F0, F1, F2, F3, F4]>>::Filtered;
                // `F0 and `F3` don't exist in `Filtered`, so length should be 3
                assert_eq!(length![Refiltered], 3);
            }
        }
        {
            // label![F5, U5];
            type Filtered = <SampleLabels as Filter<Labels![F1, F2, F4, F5]>>::Filtered;
            // F5 doesn't exist in SampleLabels, so we still should only have 3
            assert_eq!(length![Filtered], 3);
        }
        {
            // label![F5, U5];
            // label![F6, U6];
            // label![F7, U7];
            type Filtered = <SampleLabels as Filter<Labels![F5, F6, F7]>>::Filtered;
            // None of these labels exist in SampleLabels, so we should have 0
            assert_eq!(length![Filtered], 0);
        }
        {
            // check for problems cause by duplicated in label list
            type Filtered = <SampleLabels as Filter<Labels![F2, F2, F2]>>::Filtered;
            // we only filtered 1 label (even if it was duplicated), so length should be 1
            assert_eq!(length![Filtered], 1);
        }
        {
            // check for problems cause by duplicated in label list
            type Filtered = <SampleLabels as Filter<Labels![F2, F2, F3]>>::Filtered;
            // we only filtered 2 label (albeit with some duplication), so length should be 2
            assert_eq!(length![Filtered], 2);
        }
    }
}
