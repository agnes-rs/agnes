/*!
Traits, structs, and type aliases for handling labels and associated logic.
*/
use std::collections::VecDeque;
use std::marker::PhantomData;
use std::ops::{Add, BitAnd, BitOr, Not, Sub};

use typenum::{
    bit::{B0, B1},
    marker_traits::Bit,
    operator_aliases::{Add1, And, Or, Sub1},
    type_operators::IsEqual,
    uint::{UInt, UTerm, Unsigned},
};

use cons::{Cons, Nil};
use store::DataRef;

/// Trait to provide associated types (table and backing natural) for a field identifier.
///
/// All identifiers in `agnes` exist in a specific table (a marker struct which represents that
/// table). Within the table, identifiers are backed by a type-level natural number (using the
/// `typenum` crate for type-level numbers).
pub trait Identifier {
    /// The [Ident](struct.Ident.html) struct (which should always be
    /// Ident<Self::Table, Self::Natural) for this identifier.
    type Ident: Identifier; // = Ident<Self::Table, Self::Natural>;
    /// The table for this identifier.
    type Table;
    /// The `typenum`-based backing natural number corresponding to this identifier.
    type Natural;
}

/// A label, which is simply an [Identifier](trait.Identifier.html) along with an associated
/// `const` name and type description.
pub trait Label: Identifier {
    /// The label name.
    const NAME: &'static str;
    /// The type description for the data referred to by this label.
    const TYPE: &'static str;
}

/// An basic identifier struct for an identifier within the table `Tbl`, backed by the type-level
/// natural number `Nat`.
#[derive(Debug, Clone)]
pub struct Ident<Tbl, Nat> {
    _marker: PhantomData<(Tbl, Nat)>,
}

impl<Tbl, Nat> Identifier for Ident<Tbl, Nat> {
    type Ident = Self;
    type Table = Tbl;
    type Natural = Nat;
}
/// Helpful type alias to refer to the table in which an identifier exists.
pub type TblOf<T> = <T as Identifier>::Table;
/// Helpful type alias to refer to the backing natural number for an identifier.
pub type NatOf<T> = <T as Identifier>::Natural;

impl Identifier for UTerm {
    type Ident = Ident<Self::Table, Self::Natural>;
    type Table = Local;
    type Natural = Self;
}
impl<U, B> Identifier for UInt<U, B> {
    type Ident = Ident<Self::Table, Self::Natural>;
    type Table = Local;
    type Natural = Self;
}

/// Trait to access name and type description for a label.
pub trait LabelName {
    /// Returns the label name.
    fn name() -> &'static str;
    /// Returns a string specified the type of this data referred to by this label.
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
    /// Whether or not these identifiers are equal.
    type Eq: Bit;
}

/// Type alias for the 'true' bit.
pub type True = B1;
/// Type alias for the 'false' bit.
pub type False = B0;

/// Fallback to IsEqual
impl<T, U> IdentEq<U> for T
where
    T: IsEqual<U>,
{
    type Eq = <T as IsEqual<U>>::Output;
}

/// Type-level equality implementation for `Ident`s. Result will be `True` if both table and
/// the type-level natural number backing this label match.
impl<TTable, TNat, UTbl, UNat> IdentEq<Ident<UTbl, UNat>> for Ident<TTable, TNat>
where
    TTable: IsEqual<UTbl>,
    TNat: IsEqual<UNat>,
    <TTable as IsEqual<UTbl>>::Output: BitAnd<<TNat as IsEqual<UNat>>::Output>,
    <<TTable as IsEqual<UTbl>>::Output as BitAnd<<TNat as IsEqual<UNat>>::Output>>::Output: Bit,
{
    type Eq = And<<TTable as IsEqual<UTbl>>::Output, <TNat as IsEqual<UNat>>::Output>;
}

/// Common dummy table for 'local' lookups -- lookups that are not related to the concept of tables
/// (in particular, used for looking up the frame index in a view from a field label)
pub struct Local;
impl IsEqual<Local> for Local {
    type Output = True;
    fn is_equal(self, _rhs: Local) -> True {
        B1
    }
}

/// Trait for determining whether or not the `Self` and `U` labels refer to the same field.
pub trait LabelEq<U> {
    /// Whether or not the two labels refer to the same field.
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

/// Container for a value of type `V` labeled with `L`.
#[derive(Debug, Clone)]
pub struct Labeled<L, V> {
    _label: PhantomData<L>,
    /// The contained value corresponding to this label.
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

/// Container for storing the underlying data type `D` (of a field, for example) for a value of
/// type `V`.
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

/// Trait for associating an underlying data type with a type.
pub trait Typed {
    /// Associated data type with this type.
    type DType;
}
impl<D, V> Typed for TypedValue<D, V> {
    type DType = D;
}
impl<L, D, V> Typed for Labeled<L, TypedValue<D, V>> {
    type DType = D;
}

/// Type alias for the associated data type.
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

/// Marker trait for an object that can be held in a Labeled<...> or TypedValue<...> container.
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

/// Trait for extracting the an associated value of a value-holding container (e.g.
/// [TypedValue](struct.TypedValue.html), [Labeled](struct.Labeled.html)).
pub trait Valued {
    /// The associated value.
    type Value;
    /// Read-only reference to the value.
    fn value_ref(&self) -> &Self::Value;
    /// Mutable reference to the value.
    fn value_mut(&mut self) -> &mut Self::Value;
    /// Take ownership of the value.
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

/// Type alias for retrieving the Value of a [Valued](trait.Valued.html) object.
pub type ValueOf<T> = <T as Valued>::Value;

/// Trait for finding the associated marker (non-instantiated type) for a container
/// (e.g. [Labeled](struct.Labeled.html)).
pub trait Marked {
    /// Associated marker.
    type Marker;
}
impl<L, M> Marked for Labeled<L, PhantomData<M>> {
    type Marker = M;
}
impl<L, D, M> Marked for Labeled<L, TypedValue<D, PhantomData<M>>> {
    type Marker = M;
}

/// Type alias for retrieving the marker of a [Marked](trait.Marked.html) object.
pub type MarkerOf<T> = <T as Marked>::Marker;

/// Label-value cons-list
pub type LVCons<L, V, T> = Cons<Labeled<L, V>, T>;
/// Label-only cons-list
pub type LCons<L, T> = LVCons<L, (), T>;
/// Type alias for a label-only cons-list.
pub type LabelCons<L, T> = LCons<L, T>;
/// Label-marker cons-list
pub type LMCons<L, M, T> = LVCons<L, PhantomData<M>, T>;
/// Label-DType-value cons-list
pub type LDVCons<L, D, V, T> = LVCons<L, TypedValue<D, V>, T>;

/// `LabelEq`-based membership test for cons-lists. Specifies whether `E` is a member (based on
/// labels) of Self.
pub trait Member<E> {
    /// [True](type.True.html) or [False](type.False.html).
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
    /// [True](type.True.html) or [False](type.False.html).
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

/// Look up an element from a cons-list by `typenum` natural number.
pub trait LookupElemByNat<N> {
    /// Type of looked-up element.
    type Elem;
    /// Look up the element from this cons-list.
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

/// Lookup a type-level natural number backing label `L`.
pub trait LookupNatByLabel<L> {
    /// The backing type-level natural number for `L`.
    type Nat: Unsigned;
    /// A run-time accessor for `Nat`.
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

/// Helper lookup trait for [LookupNatByLabel](trait.LookupNatByLabel.html). Used by
/// `LookupNatByLabel` for computing the backing type-level natural number for label `TargetL`.
///
/// `B` specifies whether or not `TargetL` matches the head value of `Self`.
pub trait LookupNatByLabelMatch<TargetL, B> {
    /// Backing type-level natural number for `TargetL`.
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

/// Look up an element from a cons-list by label `L`.
pub trait LookupElemByLabel<L> {
    /// Type of lookup-up element.
    type Elem;
    /// Look up the element from this cons-list.
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

/// Type alias for an element (as looked up by `Label`) from cons-list `T`.
pub type ElemOf<T, Label> = <T as LookupElemByLabel<Label>>::Elem;

/// Sepcialization of [LookupElemByLabel](trait.LookupElemByLabel.html) where the looked-up element
/// implements [Valued](trait.Valued.html).
pub trait LookupValuedElemByLabel<L>: LookupElemByLabel<L> {
    /// Type of looked-up element.
    type Elem: Valued;

    /// Look up the element from this cons-list.
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

/// Type alias for an element implementing [Valued](trait.Valued.html) (as looked up by `Label`)
/// from cons-list `T`.
pub type ValuedElemOf<T, Label> = <T as LookupValuedElemByLabel<Label>>::Elem;
/// Type alias for the associated `Value` of an element implementing [Valued](trait.Valued.html)
/// (as looked up by `Label`) from cons-list `T`.
pub type ValueOfElemOf<T, Label> = <<T as LookupValuedElemByLabel<Label>>::Elem as Valued>::Value;

/// Sepcialization of [LookupElemByLabel](trait.LookupElemByLabel.html) where the looked-up element
/// implements [Marked](trait.Marked.html).
pub trait LookupMarkedElemByLabel<L>: LookupElemByLabel<L> {
    /// Marker type of looked-up element.
    type Elem: Marked;
}
impl<T, L> LookupMarkedElemByLabel<L> for T
where
    T: LookupElemByLabel<L>,
    ElemOf<Self, L>: Marked,
{
    type Elem = <Self as LookupElemByLabel<L>>::Elem;
}
/// Type alias for an element implementing [Marked](trait.Marked.html) (as looked up by `Label`)
/// from cons-list `T`.
pub type MarkedElemOf<T, Label> = <T as LookupMarkedElemByLabel<Label>>::Elem;
/// Type alias for the associated `Marker` of an element implementing [Marked](trait.Marked.html)
/// (as looked up by `Label`) from cons-list `T`.
pub type MarkerOfElemOf<T, Label> = <<T as LookupMarkedElemByLabel<Label>>::Elem as Marked>::Marker;

/// Sepcialization of [LookupElemByLabel](trait.LookupElemByLabel.html) where the looked-up element
/// implements [Typed](trait.Typed.html).
pub trait LookupTypedElemByLabel<L>: LookupElemByLabel<L> {
    /// Associated data type of looked-up element.
    type Elem: Typed;
}
impl<T, L> LookupTypedElemByLabel<L> for T
where
    T: LookupElemByLabel<L>,
    ElemOf<Self, L>: Typed,
{
    type Elem = <Self as LookupElemByLabel<L>>::Elem;
}
/// Type alias for an element implementing [Typed](trait.Typed.html) (as looked up by `Label`)
/// from cons-list `T`.
pub type TypedElemOf<T, Label> = <T as LookupTypedElemByLabel<Label>>::Elem;
/// Type alias for the associated `DType` of an element implementing [Typed](trait.Typed.html)
/// (as looked up by `Label`) from cons-list `T`.
pub type TypeOfElemOf<T, Label> = <<T as LookupTypedElemByLabel<Label>>::Elem as Typed>::DType;

/// Trait to find the subset of cons-list `Self` which are labeled with labels in `LabelList`.
///
/// Any labels in `LabelList` not found in `Self` will be ignored (see `HasLabels` for a trait
/// that requires all members of `LabelList` to be found).
pub trait LabelSubset<LabelList> {
    /// Subset of `Self` that are labeled with labels in `LabelList`.
    type Output;
}

// End-point. No more list elements to search. We don't care if anything remains or not in
// `LabelList`.
impl<LabelList> LabelSubset<LabelList> for Nil {
    type Output = Nil;
}

// Implementation for `LVCons` cons-lists.
impl<LabelList, L, V, T> LabelSubset<LabelList> for LVCons<L, V, T>
where
    LabelList: Member<L>,
    LVCons<L, V, T>: LabelSubsetPred<LabelList, <LabelList as Member<L>>::IsMember>,
{
    type Output =
        <LVCons<L, V, T> as LabelSubsetPred<LabelList, <LabelList as Member<L>>::IsMember>>::Output;
}

/// Helper filter trait. Used by `Filter` for computing the subset of `Self` cons-list which
/// contains the labels in `LabelList`.
///
/// `IsMember` specifies whether or not the label of the head value of `Self` is a member of
/// `LabelList`.
pub trait LabelSubsetPred<LabelList, IsMember> {
    /// Subset of `Self` that are labeled with labels in `LabelList`.
    type Output;
}

// `LabelSubsetPred` implementation for a cons-list where the head is in `LabelList`.
impl<LabelList, H, T> LabelSubsetPred<LabelList, True> for Cons<H, T>
where
    T: LabelSubset<LabelList>,
{
    // head is in list, so we include it and check the tail
    type Output = Cons<H, <T as LabelSubset<LabelList>>::Output>;
}
// `LabelSubsetPred` implementation for a cons-list where the head isn't in `LabelList`.
impl<LabelList, H, T> LabelSubsetPred<LabelList, False> for Cons<H, T>
where
    T: LabelSubset<LabelList>,
{
    // head isn't in list, so we check the tail
    type Output = <T as LabelSubset<LabelList>>::Output;
}

macro_rules! subset_apply {
    (
        $req_trait:tt $req_fn:tt ($($req_fn_output:tt)*)
        $trait_name:tt $fn_name:tt
        $trait_name_pred:tt $fn_name_pred:tt
    ) => {

        /// Trait for calling function `$req_fn` of trait `$req_trait` for all values of a cons-list
        /// which match a specified `LabelList`.
        ///
        /// Any labels in `LabelList` not found in `Self` will be ignored (see `HasLabels` for a
        /// trait that requires all members of `LabelList` to be found).
        pub trait $trait_name<LabelList> {
            /// Output of applying `$req_fn` to values in this cons-list which match labels in
            /// `LabelList`.
            type Output;

            /// Apply `$req_fn` to value in the head of this cons-list if label of head matches
            /// labels of `LabelList`.
            fn $fn_name(&self) -> Self::Output;
        }

        // Base-case (Nil) implementation
        impl<LabelList> $trait_name<LabelList> for Nil {
            type Output = Nil;

            fn $fn_name(&self) -> Nil {
                Nil
            }
        }

        // Implementation for `LVCons` cons-lists.
        impl<LabelList, L, V, T> $trait_name<LabelList> for LVCons<L, V, T>
        where
            LabelList: Member<L>,
            Self: $trait_name_pred<LabelList, <LabelList as Member<L>>::IsMember>
        {
            type Output = <LVCons<L, V, T> as $trait_name_pred<
                LabelList,
                <LabelList as Member<L>>::IsMember,
            >>::Output;

            fn $fn_name(&self) -> Self::Output {
                self.$fn_name_pred()
            }
        }

        /// Helper trait. Used by `$trait_name` for computing the subset of `Self` cons-list which
        /// contains the labels in `LabelList`, and applying `$req_fn` to that subset.
        ///
        /// `IsMember` specifies whether or not the label of the head value of `Self` is a member of
        /// `LabelList`.
        pub trait $trait_name_pred<LabelList, IsMember> {
            /// Output of applying `$req_fn` to values in this cons-list if `IsMember` is `True`.
            type Output;

            /// Apply `$req_fn` to value in the head of this cons-list if `IsMember` is `True`.
            fn $fn_name_pred(&self) -> Self::Output;
        }

        // `$trait_name_pred` implementation for a cons-list where the head is in `LabelList`.
        impl<LabelList, H, T> $trait_name_pred<LabelList, True> for Cons<H, T>
        where
            T: $trait_name<LabelList>,
            H: $req_trait,
        {
            type Output = Cons<$($req_fn_output)*, <T as $trait_name<LabelList>>::Output>;

            fn $fn_name_pred(&self) -> Self::Output {
                Cons {
                    head: self.head.$req_fn(),
                    tail: self.tail.$fn_name()
                }
            }
        }

        // `$trait_name_pred` implementation for a cons-list where the head isn't in `LabelList`.
        impl<LabelList, H, T> $trait_name_pred<LabelList, False> for Cons<H, T>
        where
            T: $trait_name<LabelList>,
        {
            type Output = <T as $trait_name<LabelList>>::Output;

            fn $fn_name_pred(&self) -> Self::Output {
                self.tail.$fn_name()
            }
        }

    }
}

subset_apply![
    Clone clone (H)
    SubsetClone subset_clone
    SubsetClonePred subset_clone_pred
];

/// Generates a [LabelCons](type.LabelCons.html)-list with the labels associated with this
/// cons-list.
pub trait AssocLabels {
    /// [LabelCons](type.LabelCons.html)-list of labels associated with the `Self` cons-list.
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
/// Trait for generating a collection (`VecDeque`) of string labels for the labels associated with
/// the `Self` cons-list.
pub trait StrLabels {
    /// Returns the labels (as strings) for the labels associated with `Self`.
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

/// Trait for generating a collection (`VecDeque`) of string descriptions for the types associated
/// with the `Self` cons-list.
pub trait StrTypes {
    /// Returns the string descriptions of the types associated with `Self`.
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

//TODO: finish this example
/// Declares a set of data tables that all occupy the same namespace (i.e. can be merged or
/// joined together). This macro should be used at the beginning of any `agnes`-using code, to
/// declare the various source and constructed table field labels.
///
/// # Example
/// namespace![
///     pub table employee {
///         EmpId: u64,
///         DeptId: u64,
///         EmpName: String,
///     }
///     pub table department {
///         DeptId: u64,
///         DeptName: String,
///     }
/// ]
#[macro_export]
macro_rules! namespace {
    (@fields() -> ($($out:tt)*)) => {
        declare_fields![Table; $($out)*];
        /// [FieldCons](../fieldlist/type.FieldCons.html) cons-list of fields in this table.
        pub type Fields = Fields![$($out)*];
    };
    (@fields(,) -> ($($out:tt)*)) => {
        declare_fields![Table; $($out)*];
        /// [FieldCons](../fieldlist/type.FieldCons.html) cons-list of fields in this table.
        pub type Fields = Fields![$($out)*];
    };

    (@fields
        (,$field_name:ident: $field_ty:ident = {$str_name:expr} $($rest:tt)*)
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
        (,$field_name:ident: $field_ty:ident $($rest:tt)*)
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
        namespace![@fields(,$($body)*) -> ()];
    };

    (@construct($vis:vis $tbl_name:ident)($nat:ty)($($body:tt)*)) => {
        $vis mod $tbl_name {
            #![allow(dead_code)]
            /*!
                Type aliases defining what is contained within table $tbl_name.
            */

            /// Type-level backing natural number for this table. This type connects all tables
            /// within a namespace together.
            pub type Table = $nat;

            /// Type alias for a [DataStore](../store/struct.DataStore.html) composed of the fields
            /// referenced in this table definition.
            pub type Store = $crate::store::DataStore<Fields>;
            /// Extra type alias for `Store`.
            pub type DataStore = Store;
            /// Type alias for a [DataView](../view/struct.DataView.html) composed of the fields
            /// referenced in this table definition.
            pub type View = <Store as $crate::store::IntoView>::Output;
            /// Extra type alias for `View`.
            pub type DataView = View;

            namespace![@body($($body)*)];
        }
    };

    // end case
    (@continue($prev_tbl:ty)) => {};

    // non-initial case
    (@continue($prev_tbl:ty)
        $vis:vis table $tbl_name:ident {
            $($body:tt)*
        }
        $($rest:tt)*
    ) => {
        namespace![@construct($vis $tbl_name)($prev_tbl)($($body)*)];
        namespace![@continue(typenum::Add1<$prev_tbl>) $($rest)*];
    };

    // entry point
    (
        $vis:vis table $tbl_name:ident {
            $($body:tt)*
        }
        $($rest:tt)*
    ) => {
        namespace![@construct($vis $tbl_name)(typenum::U0)($($body)*)];
        namespace![@continue(typenum::Add1<typenum::U0>) $($rest)*];
    }
}

#[macro_export]
macro_rules! nat_label {
    ($label:ident, $tbl:ty, $nat:ty, $dtype:ty, $name:expr) => {
        /// Unit struct representing the field $label.
        #[derive(Debug, Clone)]
        pub struct $label;

        impl $crate::label::Identifier for $label {
            type Ident = $crate::label::Ident<$tbl, $nat>;
            type Table = $tbl;
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
    ($label:ident, $tbl:ty, $dtype:ty) => {
        first_label![$label, $tbl, $dtype, stringify![$label]];
    };
    ($label:ident, $tbl:ty, $dtype:ty, $name:expr) => {
        nat_label![$label, $tbl, typenum::consts::U0, $dtype, $name];
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
            $crate::label::TblOf<$prev>,
            typenum::Add1<$crate::label::NatOf<$prev>>,
            $dtype,
            $name
        ];
    };
}

#[macro_export]
macro_rules! Labels {
    (@labels()) => { $crate::cons::Nil };
    (@labels($label:ident, $($rest:tt,)*)) =>
    {
        $crate::label::LCons<$label, Labels![@labels($($rest,)*)]>
    };
    (@labels($label:path, $($rest:tt,)*)) =>
    {
        $crate::label::LCons<$label, Labels![@labels($($rest,)*)]>
    };
    ($($label:ident),*$(,)*) =>
    {
        Labels![@labels($($label,)*)]
    };
    ($($label:path),*$(,)*) =>
    {
        Labels![@labels($($label,)*)]
    }
}

#[macro_export]
macro_rules! declare_fields
{
    // end case
    (@step($tbl:ty)($prev_label:ident)()) => {};

    // non-initial label
    (@step
        ($tbl:ty)
        ($prev_label:ident)
        ($label:ident: $dtype:ident = $name:expr, $($rest:tt)*)
    )
        =>
    {
        next_label![$label, $prev_label, $dtype, $name];
        declare_fields![@step
            ($tbl)
            ($label)
            ($($rest)*)
        ];
    };
    // handle non-trailing comma
    (@step($tbl:ty)($prev_label:ident)($label:ident: $dtype:ident = $name:expr))
        =>
    {
        declare_fields![@step($tbl)($prev_label)($label: $dtype,)]
    };

    // initial label
    (@start
        ($tbl:ty)
        ($label:ident: $dtype:ident = $name:expr, $($rest:tt)*)
    )
        =>
    {
        first_label![$label, $tbl, $dtype, $name];
        declare_fields![@step
            ($tbl)
            ($label)
            ($($rest)*)
        ];
    };
    // handle non-trailing comma
    (@start($tbl:ty)($label:ident: $dtype:ident = $name:expr))
        =>
    {
        declare_fields![@step($tbl)($label: $dtype = $name,)]
    };

    // entry point
    ($tbl:ty; $($fields:tt)*) => {
        declare_fields![@start($tbl)($($fields)*)];
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

    pub type SampleTable = U0;
    first_label![ImALabel, U0, u64];
    next_label![ImAnotherLabel, ImALabel, u64];

    #[test]
    fn type_eq() {
        assert!(<U1 as IdentEq<U1>>::Eq::to_bool());
        assert!(!<U1 as IdentEq<U4>>::Eq::to_bool());
        assert!(<ImALabel as LabelEq<ImALabel>>::Eq::to_bool());
        assert!(!<ImALabel as LabelEq<ImAnotherLabel>>::Eq::to_bool());
    }

    pub type NumberTable = Add1<SampleTable>;
    first_label![F0, NumberTable, u64];
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
            type Filtered = <SampleLabels as LabelSubset<Labels![]>>::Output;
            // empty filter, length should be 0
            assert_eq!(length![Filtered], 0);
        }
        {
            // other null case
            type Filtered = <Nil as LabelSubset<Labels![F1, F3]>>::Output;
            // empty cons-list, so filtered length should be 0
            assert_eq!(length![Filtered], 0);
        }
        {
            type Filtered = <SampleLabels as LabelSubset<Labels![F3]>>::Output;
            // we only filtered 1 label, so length should be 1
            assert_eq!(length![Filtered], 1);
        }
        {
            type Filtered = <SampleLabels as LabelSubset<Labels![F1, F2, F4]>>::Output;
            // we only filtered 3 labels, so length should be 3
            assert_eq!(length![Filtered], 3);

            {
                type Refiltered = <Filtered as LabelSubset<Labels![F1, F2, F4]>>::Output;
                // filtered same labels, so length should stay at 3
                assert_eq!(length![Refiltered], 3);
            }
            {
                type Refiltered = <Filtered as LabelSubset<Labels![F1, F2]>>::Output;
                // filtered 2 labels that should exist `Filtered`, so length should be 2
                assert_eq!(length![Refiltered], 2);
            }
            {
                type Refiltered = <Filtered as LabelSubset<Labels![F3, F0]>>::Output;
                // filtered 2 labels that should not exist `Filtered`, so length should be 0
                assert_eq!(length![Refiltered], 0);
            }
            {
                type Refiltered = <Filtered as LabelSubset<Labels![F0, F1, F2, F3, F4]>>::Output;
                // `F0 and `F3` don't exist in `Filtered`, so length should be 3
                assert_eq!(length![Refiltered], 3);
            }
        }
        {
            type Filtered = <SampleLabels as LabelSubset<Labels![F1, F2, F4, F5]>>::Output;
            // F5 doesn't exist in SampleLabels, so we still should only have 3
            assert_eq!(length![Filtered], 3);
        }
        {
            type Filtered = <SampleLabels as LabelSubset<Labels![F5, F6, F7]>>::Output;
            // None of these labels exist in SampleLabels, so we should have 0
            assert_eq!(length![Filtered], 0);
        }
        {
            // check for problems cause by duplicated in label list
            type Filtered = <SampleLabels as LabelSubset<Labels![F2, F2, F2]>>::Output;
            // we only filtered 1 label (even if it was duplicated), so length should be 1
            assert_eq!(length![Filtered], 1);
        }
        {
            // check for problems cause by duplicated in label list
            type Filtered = <SampleLabels as LabelSubset<Labels![F2, F2, F3]>>::Output;
            // we only filtered 2 label (albeit with some duplication), so length should be 2
            assert_eq!(length![Filtered], 2);
        }
    }
}
