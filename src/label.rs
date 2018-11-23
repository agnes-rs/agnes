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

// #[derive(Debug, Clone)]
// pub struct MarkerValue<M, V>
// {
//     _marker: PhantomData<M>,
//     value: V
// }
// impl<D, V> From<V> for MarkerValue<D, V>
// {
//     fn from(orig: V) -> MarkerValue<D, V>
//     {
//         MarkerValue
//         {
//             _marker: PhantomData,
//             value: orig
//         }
//     }
// }

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
// impl<'a, T> SelfValued for &'a ::field::FieldData<T> {}
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
// impl<M, V> Valued for MarkerValue<M, V>
//     where V: Valued
// {
//     type Value = V::Value;
//     fn value_ref(&self) -> &Self::Value { &self.value.value_ref() }
// }
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

/// Label-only cons-list
pub type LCons<L, T> = Cons<PhantomData<L>, T>;
pub type LabelCons<L, T> = LCons<L, T>;
/// Label-value cons-list
pub type LVCons<L, V, T> = Cons<Labeled<L, V>, T>;
/// Label-marker cons-list
pub type LMCons<L, M, T> = LVCons<L, PhantomData<M>, T>;
/// Label-DType-value cons-list
pub type LDVCons<L, D, V, T> = LVCons<L, TypedValue<D, V>, T>;
/// Label-DType-marker cons-list
pub type LDMCons<L, D, M, T> = LDVCons<L, D, PhantomData<M>, T>;
// /// Label-DType-value-marker cons-list
// pub type LDMVCons<L, D, M, V, T> = LDVCons<L, D, MarkerValue<M, V>, T>;

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

pub type NextLabelIndex<T> = Add1<<T as LabelIndex>::Idx>;

pub trait StrLabels
{
    fn labels<'a>() -> VecDeque<&'a str>;
}
impl StrLabels for Nil
{
    fn labels<'a>() -> VecDeque<&'a str> { VecDeque::new() }
}
impl<L, T> StrLabels for LCons<L, T>
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
// impl<L, T> LCons<L, T>
// {
//     pub fn str_labels<'a>() -> Vec<&'a str>
//         where Self: StrLabels
//     {
//         <Self as StrLabels>::labels(vec![])
//     }
// }

/// Type-level equality. Leverages `typenum`'s `IsEqual` trait for type-level-number equality,
/// but doesn't use `IsEqual`'s `is_equal` method (since no results of this equality check are
/// intended to be instantiated).
pub trait TypeEq<Other> {
    type Eq: Bit;
}

pub type True = B1;
pub type False = B0;

impl<T, U> TypeEq<U> for T
    where T: IsEqual<U>
{
    type Eq = <T as IsEqual<U>>::Output;
}

/// Type-level equality, extended for Labels. The `Name` type parameter doesn't matter.
impl<T, U, TName, UName> TypeEq<Label<U, UName>> for Label<T, TName>
    where T: TypeEq<U>
{
    type Eq = <T as TypeEq<U>>::Eq;
}

/// Equate Labels to backing naturals
impl<T, Name> TypeEq<UTerm> for Label<T, Name>
    where T: TypeEq<UTerm>
{
    type Eq = <T as TypeEq<UTerm>>::Eq;
}
impl<T, U, Name> TypeEq<UInt<U, B0>> for Label<T, Name>
    where T: TypeEq<UInt<U, B0>>
{
    type Eq = <T as TypeEq<UInt<U, B0>>>::Eq;
}
impl<T, U, Name> TypeEq<UInt<U, B1>> for Label<T, Name>
    where T: TypeEq<UInt<U, B1>>
{
    type Eq = <T as TypeEq<UInt<U, B1>>>::Eq;
}

impl<U, Name> TypeEq<Label<U, Name>> for UTerm
    where UTerm: TypeEq<U>
{
    type Eq = <UTerm as TypeEq<U>>::Eq;
}
impl<U, Name> TypeEq<Label<U, Name>> for UInt<U, B0>
    where UInt<U, B0>: TypeEq<U>
{
    type Eq = <UInt<U, B0> as TypeEq<U>>::Eq;
}
impl<U, Name> TypeEq<Label<U, Name>> for UInt<U, B1>
    where UInt<U, B1>: TypeEq<U>
{
    type Eq = <UInt<U, B1> as TypeEq<U>>::Eq;
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
impl<T, U, TNS, UNS> TypeEq<NsLabel<U, UNS>> for NsLabel<T, TNS>
    where T: TypeEq<U>,
          TNS: TypeEq<UNS>,
          <T as TypeEq<U>>::Eq: BitAnd<<TNS as TypeEq<UNS>>::Eq>,
          <<T as TypeEq<U>>::Eq as BitAnd<<TNS as TypeEq<UNS>>::Eq>>::Output: Bit,
{
    type Eq = And<<T as TypeEq<U>>::Eq, <TNS as TypeEq<UNS>>::Eq>;
}

/// `TypeEq`-based membership test
pub trait Member<E> {
    type IsMember: Bit;
}

impl<E> Member<E> for Nil {
    type IsMember = False;
}
impl<E, L, T> Member<E> for LCons<L, T>
    where L: TypeEq<E>,
          T: Member<E>,
          <L as TypeEq<E>>::Eq: BitOr<<T as Member<E>>::IsMember>,
          <<L as TypeEq<E>>::Eq as BitOr<<T as Member<E>>::IsMember>>::Output: Bit,
{
    type IsMember = Or<<L as TypeEq<E>>::Eq, <T as Member<E>>::IsMember>;
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
    where TargetL: TypeEq<L>,
          LVCons<L, V, T>:
            LookupNatByLabelMatch<TargetL, <TargetL as TypeEq<L>>::Eq>,
{
    type Nat = <LVCons<L, V, T> as
        LookupNatByLabelMatch<TargetL, <TargetL as TypeEq<L>>::Eq>>::Nat;
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
pub type TypeOfElemOf<T, Label> = <<T as LookupElemByLabel<Label>>::Elem as Typed>::DType;
pub type ValueOfElemOf<T, Label> = <<T as LookupElemByLabel<Label>>::Elem as Valued>::Value;
pub type MarkerOfElemOf<T, Label> = <<T as LookupElemByLabel<Label>>::Elem as Marked>::Marker;

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
        mod $name {
            #![allow(dead_code)]
            #[allow(unused_imports)]
            use typenum::consts::*;
            const NAME: &'static str = stringify![$name];

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

#[cfg(test)]
mod tests
{
    use cons::Nil;
    use super::*;
    use typenum::{
        Bit,
        consts::{U0, U1, U2, U3, U4}
    };

    #[test]
    fn type_eq()
    {
        assert!(<U1 as TypeEq<U1>>::Eq::to_bool());
        assert!(!<U1 as TypeEq<U4>>::Eq::to_bool());

        label![ImALabel, U0];
        label![ImAnotherLabel, U1];
        // type ImALabel = Label<U0>;
        // type ImAnotherLabel = Label<U1>;

        assert!(<ImALabel::Label as TypeEq<ImALabel::Label>>::Eq::to_bool());
        assert!(!<ImALabel::Label as TypeEq<ImAnotherLabel::Label>>::Eq::to_bool());
    }

    #[test]
    fn lookup()
    {
        label![F0, U0];
        label![F1, U1];
        label![F2, U2];
        label![F3, U3];
        label![F4, U4];

        let foo = LVCons {
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

        assert_eq!(LookupElemByNat::<U0>::elem(&foo).value, 6u64);
        assert_eq!(LookupElemByNat::<U1>::elem(&foo).value, 5.3);
        assert_eq!(LookupElemByNat::<U2>::elem(&foo).value, -3i64);
        assert_eq!(LookupElemByNat::<U3>::elem(&foo).value, "Hello".to_string());
        assert_eq!(LookupElemByNat::<U4>::elem(&foo).value, 3.2f32);

        assert_eq!(LookupNatByLabel::<F0::Label>::nat(&foo), 0);
        assert_eq!(LookupNatByLabel::<F1::Label>::nat(&foo), 1);
        assert_eq!(LookupNatByLabel::<F2::Label>::nat(&foo), 2);
        assert_eq!(LookupNatByLabel::<F3::Label>::nat(&foo), 3);
        assert_eq!(LookupNatByLabel::<F4::Label>::nat(&foo), 4);

        assert_eq!(LookupElemByLabel::<F0::Label>::elem(&foo).value, 6u64);
        assert_eq!(LookupElemByLabel::<F1::Label>::elem(&foo).value, 5.3);
        assert_eq!(LookupElemByLabel::<F2::Label>::elem(&foo).value, -3i64);
        assert_eq!(LookupElemByLabel::<F3::Label>::elem(&foo).value, "Hello".to_string());
        assert_eq!(LookupElemByLabel::<F4::Label>::elem(&foo).value, 3.2f32);

        // type F5 = Label<U5>;
        label![F5, U5];
        let foo = LVCons {
            head: Labeled::<F5::Label, _>::from(3u32),
            tail: foo,
        };

        assert_eq!(LookupNatByLabel::<F0::Label>::nat(&foo), 1);
        assert_eq!(LookupNatByLabel::<F1::Label>::nat(&foo), 2);
        assert_eq!(LookupNatByLabel::<F2::Label>::nat(&foo), 3);
        assert_eq!(LookupNatByLabel::<F3::Label>::nat(&foo), 4);
        assert_eq!(LookupNatByLabel::<F4::Label>::nat(&foo), 5);
        assert_eq!(LookupNatByLabel::<F5::Label>::nat(&foo), 0);

        assert_eq!(LookupElemByLabel::<F0::Label>::elem(&foo).value, 6u64);
        assert_eq!(LookupElemByLabel::<F1::Label>::elem(&foo).value, 5.3);
        assert_eq!(LookupElemByLabel::<F2::Label>::elem(&foo).value, -3i64);
        assert_eq!(LookupElemByLabel::<F3::Label>::elem(&foo).value, "Hello".to_string());
        assert_eq!(LookupElemByLabel::<F4::Label>::elem(&foo).value, 3.2f32);
        assert_eq!(LookupElemByLabel::<F5::Label>::elem(&foo).value, 3u32);
    }
}