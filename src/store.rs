/*!
Data storage struct and implementation. [DataStore](struct.DataStore.html) represents and stores the
data from a single data source.
*/
use std::fmt::Debug;
use std::ops::Deref;
use std::rc::Rc;

#[cfg(feature = "serialize")]
use serde::ser::{Serialize, Serializer};
use typenum::uint::UTerm;

use access::DataIndex;
use cons::*;
use error;
use field::{FieldData, Value};
use fieldlist::{FieldCons, FieldPayloadCons, FieldSpec};
use frame::DataFrame;
use label::*;
use select::{FieldSelect, SelectFieldByLabel};
use view::{DataView, FrameLookupCons, ViewFrameCons};

/// Local `Rc` wrapper type for [FieldData](../field/struct.FieldData.html) objects.
#[derive(Debug, Hash, PartialEq, Eq)]
pub struct DataRef<DType>(pub Rc<FieldData<DType>>);

impl<DType> DataRef<DType> {
    fn new(field: FieldData<DType>) -> DataRef<DType> {
        DataRef(Rc::new(field))
    }
}

impl<DType> Clone for DataRef<DType> {
    fn clone(&self) -> DataRef<DType> {
        DataRef(Rc::clone(&self.0))
    }
}

impl<T> Deref for DataRef<T> {
    type Target = FieldData<T>;

    fn deref(&self) -> &FieldData<T> {
        &self.0.deref()
    }
}

impl<T> From<FieldData<T>> for DataRef<T> {
    fn from(orig: FieldData<T>) -> DataRef<T> {
        DataRef(Rc::new(orig))
    }
}

impl<T> DataIndex for DataRef<T>
where
    FieldData<T>: DataIndex<DType = T>,
    T: Debug,
{
    type DType = T;

    fn get_datum(&self, idx: usize) -> error::Result<Value<&T>> {
        <FieldData<T> as DataIndex>::get_datum(&self.0, idx)
    }
    fn len(&self) -> usize {
        <FieldData<T> as DataIndex>::len(&self.0)
    }
}

#[cfg(feature = "serialize")]
impl<T> Serialize for DataRef<T>
where
    T: Serialize,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.0.serialize(serializer)
    }
}

/// Type alias for main data store cons-list. Each `head` contains label and data type information
/// along with a [DataRef(struct.DataRef.html)] reference to the data for this field.
pub type StorageCons<Label, DType, Tail> = FieldPayloadCons<Label, DType, DataRef<DType>, Tail>;

/// Primary `agnes` data storage object. `Fields` is a [FieldCons](../fieldlist/type.FieldCons.html)
/// cons-list which implements [AssocStorage](trait.AssocStorage.html); the `DataStore` contains
/// this associated storage structure.
#[derive(Debug)]
pub struct DataStore<Fields: AssocStorage> {
    data: Fields::Storage,
}

/// Provide an associated [StorageCons](type.StorageCons.html) cons-list with `Self`.
pub trait AssocStorage {
    /// Associated [StorageCons](type.StorageCons.html) cons-list.
    type Storage: Debug;
}
impl<Label, DType, Tail> AssocStorage for FieldCons<Label, DType, Tail>
where
    Tail: AssocStorage,
    Label: Debug,
    DType: Debug,
{
    type Storage = StorageCons<Label, DType, Tail::Storage>;
}
impl AssocStorage for Nil {
    type Storage = Nil;
}

impl<Fields> DataStore<Fields>
where
    Fields: AssocStorage,
{
    /// Generate and return an empty data store
    pub fn empty() -> DataStore<Nil> {
        DataStore { data: Nil }
    }
}

/// Trait to provide the number of rows of this data structure.
pub trait NRows {
    /// Return the number of rows in this data structure.
    fn nrows(&self) -> usize;
}
impl NRows for Nil {
    fn nrows(&self) -> usize {
        0
    }
}
impl<Label, DType, Tail> NRows for StorageCons<Label, DType, Tail> {
    fn nrows(&self) -> usize {
        self.head.value_ref().len()
    }
}

impl<Fields> NRows for DataStore<Fields>
where
    Fields: AssocStorage,
    Fields::Storage: NRows,
{
    fn nrows(&self) -> usize {
        self.data.nrows()
    }
}

/// Type alias for a reference to a [FieldData](../field/struct.FieldData.html) along with label
/// and data type annotation.
pub type NewFieldStorage<NewLabel, NewDType> =
    Labeled<NewLabel, TypedValue<NewDType, DataRef<NewDType>>>;

macro_rules! make_add_field {
    (
        $(#[$add_trait_doc:meta])* trait $add_trait:tt;
        $(#[$add_fn_doc:meta])* fn $add_fn:tt;

        $(#[$add_valiter_trait_doc:meta])* trait $add_valiter_trait:tt;
        $(#[$add_valiter_fn_doc:meta])* fn $add_valiter_fn:tt;

        $(#[$add_iter_trait_doc:meta])* trait $add_iter_trait:tt;
        $(#[$add_iter_fn_doc:meta])* fn $add_iter_fn:tt;

        $(#[$add_cloned_valiter_trait_doc:meta])* trait $add_cloned_valiter_trait:tt;
        $(#[$add_cloned_valiter_fn_doc:meta])* fn $add_cloned_valiter_fn:tt;

        $(#[$add_cloned_iter_trait_doc:meta])* trait $add_cloned_iter_trait:tt;
        $(#[$add_cloned_iter_fn_doc:meta])* fn $add_cloned_iter_fn:tt;

        $(#[$add_empty_trait_doc:meta])* trait $add_empty_trait:tt;
        $(#[$add_empty_fn_doc:meta])* fn $add_empty_fn:tt;

        $push_trait:tt $push_fn:tt $pushed_alias:tt
    ) => {
        /// Type alias for the output of applying $push_fn to previous fields.
        pub type $pushed_alias<PrevFields, NewLabel, NewDType> =
            <PrevFields as $push_trait<FieldSpec<NewLabel, NewDType>>>::Output;

        $(#[$add_trait_doc])*
        pub trait $add_trait<NewLabel, NewDType> {
            /// [FieldCons](../fieldlist/type.FieldCons.html) cons-list after adding field.
            type OutputFields: AssocStorage;

            $(#[$add_fn_doc])*
            fn $add_fn(self, data: FieldData<NewDType>) -> DataStore<Self::OutputFields>;
        }

        impl<PrevFields, NewLabel, NewDType> $add_trait<NewLabel, NewDType>
            for DataStore<PrevFields>
        where
            PrevFields: AssocStorage + $push_trait<FieldSpec<NewLabel, NewDType>>,
            $pushed_alias<PrevFields, NewLabel, NewDType>: AssocStorage,
            PrevFields::Storage: $push_trait<
                NewFieldStorage<NewLabel, NewDType>,
                Output = <$pushed_alias<PrevFields, NewLabel, NewDType> as AssocStorage>::Storage,
            >,
            NewLabel: Debug,
            NewDType: Debug,
        {
            type OutputFields = $pushed_alias<PrevFields, NewLabel, NewDType>;

            fn $add_fn(self, data: FieldData<NewDType>) -> DataStore<Self::OutputFields> {
                DataStore {
                    data: self
                        .data
                        .$push_fn(TypedValue::from(DataRef::new(data)).into()),
                }
            }
        }

        $(#[$add_valiter_trait_doc])*
        pub trait $add_valiter_trait<NewLabel, NewDType> {
            /// [FieldCons](../fieldlist/type.FieldCons.html) cons-list after adding field.
            type OutputFields: AssocStorage;

            $(#[$add_valiter_fn_doc])*
            fn $add_valiter_fn<IntoIter, Iter>(
                self,
                iter: IntoIter,
            ) -> DataStore<Self::OutputFields>
            where
                Iter: Iterator<Item = Value<NewDType>>,
                IntoIter: IntoIterator<IntoIter = Iter, Item = Value<NewDType>>;
        }
        impl<PrevFields, NewLabel, NewDType> $add_valiter_trait<NewLabel, NewDType>
            for DataStore<PrevFields>
        where
            PrevFields: AssocStorage + $push_trait<FieldSpec<NewLabel, NewDType>>,
            $pushed_alias<PrevFields, NewLabel, NewDType>: AssocStorage,
            PrevFields::Storage: $push_trait<
                NewFieldStorage<NewLabel, NewDType>,
                Output = <$pushed_alias<PrevFields, NewLabel, NewDType> as AssocStorage>::Storage,
            >,
            NewLabel: Debug,
            NewDType: Default + Debug,
        {
            type OutputFields = $pushed_alias<PrevFields, NewLabel, NewDType>;

            fn $add_valiter_fn<IntoIter, Iter>(
                self,
                iter: IntoIter,
            ) -> DataStore<Self::OutputFields>
            where
                Iter: Iterator<Item = Value<NewDType>>,
                IntoIter: IntoIterator<IntoIter = Iter, Item = Value<NewDType>>,
            {
                DataStore {
                    data: self.data.$push_fn(
                        TypedValue::from(DataRef::new(
                            iter.into_iter().collect::<FieldData<NewDType>>(),
                        ))
                        .into(),
                    ),
                }
            }
        }

        $(#[$add_iter_trait_doc])*
        pub trait $add_iter_trait<NewLabel, NewDType> {
            /// [FieldCons](../fieldlist/type.FieldCons.html) cons-list after adding field.
            type OutputFields: AssocStorage;

            $(#[$add_iter_fn_doc])*
            fn $add_iter_fn<IntoIter, Iter>(self, iter: IntoIter) -> DataStore<Self::OutputFields>
            where
                Iter: Iterator<Item = NewDType>,
                IntoIter: IntoIterator<IntoIter = Iter, Item = NewDType>;
        }
        impl<PrevFields, NewLabel, NewDType> $add_iter_trait<NewLabel, NewDType>
            for DataStore<PrevFields>
        where
            PrevFields: AssocStorage + $push_trait<FieldSpec<NewLabel, NewDType>>,
            $pushed_alias<PrevFields, NewLabel, NewDType>: AssocStorage,
            PrevFields::Storage: $push_trait<
                NewFieldStorage<NewLabel, NewDType>,
                Output = <$pushed_alias<PrevFields, NewLabel, NewDType> as AssocStorage>::Storage,
            >,
            NewLabel: Debug,
            NewDType: Debug,
        {
            type OutputFields = $pushed_alias<PrevFields, NewLabel, NewDType>;

            fn $add_iter_fn<IntoIter, Iter>(self, iter: IntoIter) -> DataStore<Self::OutputFields>
            where
                Iter: Iterator<Item = NewDType>,
                IntoIter: IntoIterator<IntoIter = Iter, Item = NewDType>,
            {
                DataStore {
                    data: self.data.$push_fn(
                        TypedValue::from(DataRef::new(
                            iter.into_iter().collect::<FieldData<NewDType>>(),
                        ))
                        .into(),
                    ),
                }
            }
        }

        $(#[$add_cloned_valiter_trait_doc])*
        pub trait $add_cloned_valiter_trait<NewLabel, NewDType> {
            /// [FieldCons](../fieldlist/type.FieldCons.html) cons-list after adding field.
            type OutputFields: AssocStorage;

            $(#[$add_cloned_valiter_fn_doc])*
            fn $add_cloned_valiter_fn<'a, IntoIter, Iter>(
                self,
                iter: IntoIter,
            ) -> DataStore<Self::OutputFields>
            where
                Iter: Iterator<Item = Value<&'a NewDType>>,
                IntoIter: IntoIterator<IntoIter = Iter, Item = Value<&'a NewDType>>,
                NewDType: 'a;
        }
        impl<PrevFields, NewLabel, NewDType> $add_cloned_valiter_trait<NewLabel, NewDType>
            for DataStore<PrevFields>
        where
            PrevFields: AssocStorage + $push_trait<FieldSpec<NewLabel, NewDType>>,
            $pushed_alias<PrevFields, NewLabel, NewDType>: AssocStorage,
            PrevFields::Storage: $push_trait<
                NewFieldStorage<NewLabel, NewDType>,
                Output = <$pushed_alias<PrevFields, NewLabel, NewDType> as AssocStorage>::Storage,
            >,
            NewLabel: Debug,
            NewDType: Default + Clone + Debug,
        {
            type OutputFields = $pushed_alias<PrevFields, NewLabel, NewDType>;

            fn $add_cloned_valiter_fn<'a, IntoIter, Iter>(
                self,
                iter: IntoIter,
            ) -> DataStore<Self::OutputFields>
            where
                Iter: Iterator<Item = Value<&'a NewDType>>,
                IntoIter: IntoIterator<IntoIter = Iter, Item = Value<&'a NewDType>>,
                NewDType: 'a,
            {
                DataStore {
                    data: self.data.$push_fn(
                        TypedValue::from(DataRef::new(
                            iter.into_iter()
                                .map(|x| x.clone())
                                .collect::<FieldData<NewDType>>(),
                        ))
                        .into(),
                    ),
                }
            }
        }

        $(#[$add_cloned_iter_trait_doc])*
        pub trait $add_cloned_iter_trait<NewLabel, NewDType> {
            /// [FieldCons](../fieldlist/type.FieldCons.html) cons-list after adding field.
            type OutputFields: AssocStorage;

            $(#[$add_cloned_iter_fn_doc])*
            fn $add_cloned_iter_fn<'a, IntoIter, Iter>(
                self,
                iter: IntoIter,
            ) -> DataStore<Self::OutputFields>
            where
                Iter: Iterator<Item = &'a NewDType>,
                IntoIter: IntoIterator<IntoIter = Iter, Item = &'a NewDType>,
                NewDType: 'a;
        }
        impl<PrevFields, NewLabel, NewDType> $add_cloned_iter_trait<NewLabel, NewDType>
            for DataStore<PrevFields>
        where
            PrevFields: AssocStorage + $push_trait<FieldSpec<NewLabel, NewDType>>,
            $pushed_alias<PrevFields, NewLabel, NewDType>: AssocStorage,
            PrevFields::Storage: $push_trait<
                NewFieldStorage<NewLabel, NewDType>,
                Output = <$pushed_alias<PrevFields, NewLabel, NewDType> as AssocStorage>::Storage,
            >,
            NewLabel: Debug,
            NewDType: Clone + Debug,
        {
            type OutputFields = $pushed_alias<PrevFields, NewLabel, NewDType>;

            fn $add_cloned_iter_fn<'a, IntoIter, Iter>(
                self,
                iter: IntoIter,
            ) -> DataStore<Self::OutputFields>
            where
                Iter: Iterator<Item = &'a NewDType>,
                IntoIter: IntoIterator<IntoIter = Iter, Item = &'a NewDType>,
                NewDType: 'a,
            {
                DataStore {
                    data: self.data.$push_fn(
                        TypedValue::from(DataRef::new(
                            iter.into_iter()
                                .map(|x| x.clone())
                                .collect::<FieldData<NewDType>>(),
                        ))
                        .into(),
                    ),
                }
            }
        }

        $(#[$add_empty_trait_doc])*
        pub trait $add_empty_trait<NewLabel, NewDType> {
            /// [FieldCons](../fieldlist/type.FieldCons.html) cons-list after adding field.
            type OutputFields: AssocStorage;

            $(#[$add_empty_fn_doc])*
            fn $add_empty_fn(self) -> DataStore<Self::OutputFields>;
        }
        impl<PrevFields, NewLabel, NewDType> $add_empty_trait<NewLabel, NewDType>
            for DataStore<PrevFields>
        where
            PrevFields: AssocStorage + $push_trait<FieldSpec<NewLabel, NewDType>>,
            $pushed_alias<PrevFields, NewLabel, NewDType>: AssocStorage,
            PrevFields::Storage: $push_trait<
                NewFieldStorage<NewLabel, NewDType>,
                Output = <$pushed_alias<PrevFields, NewLabel, NewDType> as AssocStorage>::Storage,
            >,
            NewLabel: Debug,
            NewDType: Debug,
        {
            type OutputFields = $pushed_alias<PrevFields, NewLabel, NewDType>;

            fn $add_empty_fn(self) -> DataStore<Self::OutputFields> {
                DataStore {
                    data: self
                        .data
                        .$push_fn(TypedValue::from(DataRef::new(FieldData::default())).into()),
                }
            }
        }

        impl<PrevFields> DataStore<PrevFields>
        where
            PrevFields: AssocStorage,
        {
            $(#[$add_fn_doc])*
            pub fn $add_fn<NewLabel, NewDType>(
                self,
                data: FieldData<NewDType>,
            ) -> DataStore<<Self as $add_trait<NewLabel, NewDType>>::OutputFields>
            where
                Self: $add_trait<NewLabel, NewDType>,
            {
                $add_trait::$add_fn(self, data)
            }

            $(#[$add_valiter_fn_doc])*
            pub fn $add_valiter_fn<NewLabel, NewDType, IntoIter, Iter>(
                self,
                iter: IntoIter,
            ) -> DataStore<<Self as $add_valiter_trait<NewLabel, NewDType>>::OutputFields>
            where
                Iter: Iterator<Item = Value<NewDType>>,
                IntoIter: IntoIterator<IntoIter = Iter, Item = Value<NewDType>>,
                Self: $add_valiter_trait<NewLabel, NewDType>,
            {
                $add_valiter_trait::$add_valiter_fn(self, iter)
            }

            $(#[$add_iter_fn_doc])*
            pub fn $add_iter_fn<NewLabel, NewDType, IntoIter, Iter>(
                self,
                iter: IntoIter,
            ) -> DataStore<<Self as $add_iter_trait<NewLabel, NewDType>>::OutputFields>
            where
                Iter: Iterator<Item = NewDType>,
                IntoIter: IntoIterator<IntoIter = Iter, Item = NewDType>,
                Self: $add_iter_trait<NewLabel, NewDType>,
            {
                $add_iter_trait::$add_iter_fn(self, iter)
            }

            $(#[$add_cloned_valiter_fn_doc])*
            pub fn $add_cloned_valiter_fn<'a, NewLabel, NewDType, IntoIter, Iter>(
                self,
                iter: IntoIter,
            ) -> DataStore<<Self as $add_cloned_valiter_trait<NewLabel, NewDType>>::OutputFields>
            where
                Iter: Iterator<Item = Value<&'a NewDType>>,
                IntoIter: IntoIterator<IntoIter = Iter, Item = Value<&'a NewDType>>,
                Self: $add_cloned_valiter_trait<NewLabel, NewDType>,
                NewDType: 'a,
            {
                $add_cloned_valiter_trait::$add_cloned_valiter_fn(self, iter)
            }
            $(#[$add_cloned_iter_fn_doc])*
            pub fn $add_cloned_iter_fn<'a, NewLabel, NewDType, IntoIter, Iter>(
                self,
                iter: IntoIter,
            ) -> DataStore<<Self as $add_cloned_iter_trait<NewLabel, NewDType>>::OutputFields>
            where
                Iter: Iterator<Item = &'a NewDType>,
                IntoIter: IntoIterator<IntoIter = Iter, Item = &'a NewDType>,
                Self: $add_cloned_iter_trait<NewLabel, NewDType>,
                NewDType: 'a,
            {
                $add_cloned_iter_trait::$add_cloned_iter_fn(self, iter)
            }

            $(#[$add_empty_fn_doc])*
            pub fn $add_empty_fn<NewLabel, NewDType>(
                self,
            ) -> DataStore<<Self as $add_empty_trait<NewLabel, NewDType>>::OutputFields>
            where
                Self: $add_empty_trait<NewLabel, NewDType>,
            {
                $add_empty_trait::$add_empty_fn(self)
            }
        }
    };
}

make_add_field![
    /// Trait for pushing a [FieldData](../field/struct.FieldData.html) onto the front of a
    /// [DataStore](struct.DataStore.html)'s fields cons-list.
    trait PushFrontField;
    /// Push a [FieldData](../field/struct.FieldData.html) onto the front of this store's
    /// fields cons-list.
    fn push_front_field;

    /// Trait for pushing a field onto the front of a [DataStore](struct.DataStore.html)'s fields
    /// cons-list using data from an iterator of [Value](../field/enum.Value.html) objects.
    trait PushFrontFromValueIter;
    /// Push a field onto the front of this store's fields cons-list using data from an iterator
    /// of [Value](../field/enum.Value.html) objects.
    fn push_front_from_value_iter;

    /// Trait for pushing a field onto the front of a [DataStore](struct.DataStore.html)'s fields
    /// cons-list using data from an iterator of objects. Field is assumed to have no missing data.
    trait PushFrontFromIter;
    /// Push a field onto the front of this store's fields cons-list using data from an iterator
    /// of objects.
    fn push_front_from_iter;

    /// Trait for pushing a field onto the front of a [DataStore](struct.DataStore.html)'s fields
    /// cons-list cloning data from an iterator of [Value](../field/enum.Value.html) objects.
    trait PushFrontClonedFromValueIter;
    /// Push a field onto the front of this store's fields cons-list cloning data from an iterator
    /// of [Value](../field/enum.Value.html) objects.
    fn push_front_cloned_from_value_iter;

    /// Trait for pushing a field onto the front of a [DataStore](struct.DataStore.html)'s fields
    /// cons-list cloning data from an iterator of objects. Field is assumed to have no missing
    /// data.
    trait PushFrontClonedFromIter;
    /// Push a field onto the front of this store's fields cons-list cloning data from an iterator
    /// of objects.
    fn push_front_cloned_from_iter;

    /// Trait for pushing an empty field onto the front of a [DataStore](struct.DataStore.html)'s
    /// fields cons-list.
    trait PushFrontEmpty;
    /// Push an empty field into the front of this store's fields cons-list.
    fn push_front_empty;

    PushFront push_front PushedFrontField
];

make_add_field![
    /// Trait for pushing a [FieldData](../field/struct.FieldData.html) onto the back of a
    /// [DataStore](struct.DataStore.html)'s fields cons-list.
    trait PushBackField;
    /// Push a [FieldData](../field/struct.FieldData.html) onto the back of this store's
    /// fields cons-list.
    fn push_back_field;

    /// Trait for pushing a field onto the back of a [DataStore](struct.DataStore.html)'s fields
    /// cons-list using data from an iterator of [Value](../field/enum.Value.html) objects.
    trait PushBackFromValueIter;
    /// Push a field onto the back of this store's fields cons-list using data from an iterator
    /// of [Value](../field/enum.Value.html) objects.
    fn push_back_from_value_iter;

    /// Trait for pushing a field onto the back of a [DataStore](struct.DataStore.html)'s fields
    /// cons-list using data from an iterator of objects. Field is assumed to have no missing data.
    trait PushBackFromIter;
    /// Push a field onto the back of this store's fields cons-list using data from an iterator
    /// of objects.
    fn push_back_from_iter;

    /// Trait for pushing a field onto the back of a [DataStore](struct.DataStore.html)'s fields
    /// cons-list cloning data from an iterator of [Value](../field/enum.Value.html) objects.
    trait PushBackClonedFromValueIter;
    /// Push a field onto the back of this store's fields cons-list cloning data from an iterator
    /// of [Value](../field/enum.Value.html) objects.
    fn push_back_cloned_from_value_iter;

    /// Trait for pushing a field onto the back of a [DataStore](struct.DataStore.html)'s fields
    /// cons-list cloning data from an iterator of objects. Field is assumed to have no missing
    /// data.
    trait PushBackClonedFromIter;
    /// Push a field onto the back of this store's fields cons-list cloning data from an iterator
    /// of objects.
    fn push_back_cloned_from_iter;

    /// Trait for pushing an empty field onto the back of a [DataStore](struct.DataStore.html)'s
    /// fields cons-list.
    trait PushBackEmpty;
    /// Push an empty field into the back of this store's fields cons-list.
    fn push_back_empty;

    PushBack push_back PushedBackField
];

impl<Label, Fields> SelectFieldByLabel<Label> for DataStore<Fields>
where
    Fields: AssocStorage,
    Fields::Storage: LookupElemByLabel<Label>,
    ElemOf<Fields::Storage, Label>: Typed,
    ElemOf<Fields::Storage, Label>: Valued<Value = DataRef<TypeOfElemOf<Fields::Storage, Label>>>,
    DataRef<TypeOfElemOf<Fields::Storage, Label>>: DataIndex,
    TypeOfElemOf<Fields::Storage, Label>: Debug,
{
    type Output = DataRef<<<Fields::Storage as LookupElemByLabel<Label>>::Elem as Typed>::DType>;

    fn select_field(&self) -> Self::Output {
        DataRef::clone(LookupElemByLabel::<Label>::elem(&self.data).value_ref())
    }
}
impl<Fields> FieldSelect for DataStore<Fields> where Fields: AssocStorage {}

/// Trait to determine the [FrameLookupCons](../view/type.FrameLookupCons.html) for a field list.
pub trait AssocFrameLookup {
    /// The associated `FrameLookupCons`.
    type Output;
}
impl AssocFrameLookup for Nil {
    type Output = Nil;
}
impl<Label, Value, Tail> AssocFrameLookup for LVCons<Label, Value, Tail>
where
    Tail: AssocFrameLookup,
{
    type Output = FrameLookupCons<Label, UTerm, Label, <Tail as AssocFrameLookup>::Output>;
}

impl<Fields> DataStore<Fields>
where
    Fields: AssocStorage + AssocFrameLookup,
{
    /// Wrap this `DataStore` with a [DataView](../view/struct.DataView.html) object. Utility
    /// function that leverages [IntoView](trait.IntoView.html).
    pub fn into_view(self) -> <Self as IntoView>::Output
    where
        Self: IntoView,
    {
        IntoView::into_view(self)
    }
}

/// Trait that provides a method to convert `Self` into a [DataView](../view/struct.DataView.html)
/// object.
pub trait IntoView {
    /// The `Labels` type parameter for the output `DataView`.
    type Labels;
    /// The `Frames` type parameter for the output `DataView`.
    type Frames;
    /// The output `DataView` (should always be `DataView<Self::Labels, Self::Frames>`).
    type Output; // = DataView<Self::Labels, Self::Frames>
    /// Convert `self` into a [DataView](../view/struct.DataView.html) object.
    fn into_view(self) -> Self::Output;
}
impl<Fields> IntoView for DataStore<Fields>
where
    Fields: AssocStorage + AssocFrameLookup,
{
    type Labels = <Fields as AssocFrameLookup>::Output;
    type Frames = ViewFrameCons<UTerm, Fields, Nil>;
    type Output = DataView<Self::Labels, Self::Frames>;

    fn into_view(self) -> Self::Output {
        DataView::new(ViewFrameCons {
            head: DataFrame::from(self).into(),
            tail: Nil,
        })
    }
}

/// Type alias for a [DataStore](struct.DataStore.html) constructed with a single field.
pub type SingleFieldStore<Label, T> =
    DataStore<<DataStore<Nil> as PushFrontFromValueIter<Label, T>>::OutputFields>;

impl<Label, I, T> IntoView for Labeled<Label, I>
where
    I: Iterator<Item = Value<T>>,
    DataStore<Nil>: PushFrontFromValueIter<Label, T>,
    <DataStore<Nil> as PushFrontFromValueIter<Label, T>>::OutputFields: AssocFrameLookup,
{
    type Labels = <SingleFieldStore<Label, T> as IntoView>::Labels;
    type Frames = <SingleFieldStore<Label, T> as IntoView>::Frames;
    type Output = <SingleFieldStore<Label, T> as IntoView>::Output;

    fn into_view(self) -> Self::Output {
        DataStore::<Nil>::empty()
            .push_front_from_value_iter(self.value)
            .into_view()
    }
}

#[cfg(test)]
mod tests {

    use std::fmt::Debug;
    use std::path::Path;
    use typenum::U0;

    use csv_sniffer::metadata::Metadata;

    use super::{DataStore, NRows};
    use cons::*;
    use field::Value;
    use select::FieldSelect;
    use source::csv::{CsvReader, CsvSource, IntoCsvSrcSpec};

    fn load_csv_file<Spec>(filename: &str, spec: Spec) -> (CsvReader<Spec::CsvSrcSpec>, Metadata)
    where
        Spec: IntoCsvSrcSpec,
        <Spec as IntoCsvSrcSpec>::CsvSrcSpec: Debug,
    {
        let data_filepath = Path::new(file!()) // start as this file
            .parent()
            .unwrap() // navigate up to src directory
            .parent()
            .unwrap() // navigate up to root directory
            .join("tests") // navigate into integration tests directory
            .join("data") // navigate into data directory
            .join(filename); // navigate to target file

        let source = CsvSource::new(data_filepath).unwrap();
        (
            CsvReader::new(&source, spec).unwrap(),
            source.metadata().clone(),
        )
    }

    namespace![
        pub table gdp {
            CountryName: String,
            CountryCode: String,
            Year1983: f64,
        }
    ];

    #[test]
    fn storage_create() {
        let ds = DataStore::<Nil>::empty();

        type TestNamespace = U0;
        first_label![Test, TestNamespace, u64];

        let data = vec![
            Value::Exists(4u64),
            Value::Exists(1),
            Value::Na,
            Value::Exists(3),
            Value::Exists(7),
            Value::Exists(8),
            Value::Na,
        ];
        let expected_nrows = data.len();

        let ds = ds.push_back_from_iter::<Test, _, _, _>(data);
        println!("{:?}", ds);
        assert_eq!(ds.nrows(), expected_nrows);
        assert_eq!(ds.field::<Test>().len(), expected_nrows);

        let gdp_spec = spec![
            fieldname gdp::CountryName = "Country Name";
            fieldname gdp::CountryCode = "Country Code";
            fieldname gdp::Year1983 = "1983";
        ];

        let (mut csv_rdr, _metadata) = load_csv_file("gdp.csv", gdp_spec.clone());
        let ds = csv_rdr.read().unwrap();
        const EXPECTED_GDP_NROWS: usize = 264;
        assert_eq!(ds.nrows(), EXPECTED_GDP_NROWS);
        assert_eq!(ds.field::<gdp::CountryName>().len(), EXPECTED_GDP_NROWS);
    }
}
