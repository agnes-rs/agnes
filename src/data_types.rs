/*!
Structures, traits, and macros for managing the list of data types that `agnes` data structures
support.

This module contains the heterogenous data structures for containing data, along with macros for
automatically creating the structures for a specific data type list.
*/

use std::hash::Hash;
use std::fmt::{Debug, Display};
use std::marker::PhantomData;

use serde::Serializer;

use access::DataIndex;
use field::FieldData;
use frame::Reindexer;
use cons::*;
use error::*;

/// Type alias for a data structure containing all of the
/// [FieldData](../field/struct.FieldData.html) structures for a single data type.
pub type TypeData<DTypes, T> = Vec<FieldData<DTypes, T>>;

/// End of a data type list.
pub type DTypeNil = Nil;
/// Building block of a type list.
pub type DTypeCons<H, T> = Cons<PhantomData<H>, PhantomData<T>>;

/// End of a data storage list;
#[derive(Debug, Clone)]
pub struct StorageNil;

/// Building block of a data storage list.
type StorageCons<DTypes, H, T> = Cons<TypeData<DTypes, H>, T>;

/// Trait providing a method for creating a new empty storage structure
pub trait CreateStorage {
    /// Create a new empty storage structure.
    fn create_storage() -> Self;
}
impl CreateStorage for StorageNil {
    fn create_storage() -> StorageNil { StorageNil }
}
impl<DTypes, H, T> CreateStorage for StorageCons<DTypes, H, T>
    where DTypes: DTypeList,
          H: DataType<DTypes>,
          T: CreateStorage
{
    fn create_storage() -> StorageCons<DTypes, H, T> {
        StorageCons {
            head: TypeData::<DTypes, H>::new(),
            tail: T::create_storage(),
        }
    }
}

/// A trait for selecting all fields of `Target` type from storage. Typically, you would use the
/// `select_type` and `select_type_mut` inherent methods on StorageCons instead.
pub trait TypeSelector<DTypes, Target>
    where DTypes: DTypeList,
          Target: DataType<DTypes>
{
    /// Returns a reference to the [TypeData](type.TypeData.html) structure for type `Target`.
    fn select_type<'a>(&'a self) -> &'a TypeData<DTypes, Target>;
    /// Returns a mutable reference to the [TypeData](type.TypeData.html) structure for type
    /// `Target`.
    fn select_type_mut<'a>(&'a mut self) -> &'a mut TypeData<DTypes, Target>;
}

/// A trait for finding the `DType` for a specified `Target` type. Typically, you would use
/// the `select_type_num` inherent method on StorageCons instead.
pub trait DTypeSelector<DTypes, Target> where DTypes: AssocTypes {
    /// Returns the `DType` enumeration value for the `Target` type.
    fn select_dtype(&self) -> DTypes::DType;
}

/// Trait for adding a data vector to the specified `Target` type.
pub trait AddVec<Target>
{
    /// Add an empty vector data for `Target` type.
    fn add_vec(&mut self) -> Result<usize>;
}

impl<DTypes, H, T> StorageCons<DTypes, H, T>
    where DTypes: DTypeList,
          H: DataType<DTypes>
{
    /// Returns a reference to the [TypeData](type.TypeData.html) structure for type `Target`.
    pub fn select_type<'a, Target>(&'a self) -> &'a TypeData<DTypes, Target>
        where Target: DataType<DTypes>, Self: TypeSelector<DTypes, Target>
    {
        TypeSelector::select_type(self)
    }
    /// Returns a mutable reference to the [TypeData](type.TypeData.html) structure for type
    /// `Target`.
    pub fn select_type_mut<'a, Target>(&'a mut self) -> &'a mut TypeData<DTypes, Target>
        where Target: DataType<DTypes>, Self: TypeSelector<DTypes, Target>
    {
        TypeSelector::select_type_mut(self)
    }
    /// Returns the `DType` enumeration value for the type `Target`.
    pub fn select_dtype<Target>(&self) -> DTypes::DType
        where Self: DTypeSelector<DTypes, Target>
    {
        DTypeSelector::select_dtype(self)
    }
}

/// Trait that provides a method for returning the maximum length of a field in a heterogeneous
/// storage structure.
pub trait MaxLen<DTypes> {
    /// Returns the maximum length of all fields in this structure.
    fn max_len(&self) -> usize;
}
impl<DTypes> MaxLen<DTypes> for StorageNil {
    fn max_len(&self) -> usize { 0 }
}
impl<DTypes, Head: DataType<DTypes>, Tail> MaxLen<DTypes> for StorageCons<DTypes, Head, Tail>
    where DTypes: DTypeList,
          Tail: MaxLen<DTypes>
{
    fn max_len(&self) -> usize {
        self.head.iter().fold(0, |max, field_data| {
            let len = field_data.len();
            if len > max { len } else { max }
        }).max(self.tail.max_len())
    }
}

/// Trait that provides details on the location of a field within a heterogeneous data storage
/// structure.
pub trait FieldLocator<DTypes>
    where DTypes: AssocTypes
{
    /// The data type of this field.
    fn ty(&self) -> DTypes::DType;
    /// The index of this field within the [TypeData](type.TypeData.html) structure.
    fn td_idx(&self) -> usize;
}

/// Field serialization trait. Behaves similar to the `serde::Serialize` trait, but with extra
/// specification of what data, exactly, is to be serialize.
pub trait FieldSerialize<DTypes> where DTypes: DTypeList
{
    /// Serialize the data [located](trait.FieldLocator.html) in a particular field, using
    /// the specified [reindexer](../frame/trait.Reindexer.html) for indexing into the data,
    /// using the specified `serializer`.
    fn serialize<L, R, S>(&self, locator: &L, reindexer: &R, serializer: S)
        -> ::std::result::Result<S::Ok, S::Error>
        where L: FieldLocator<DTypes>,
              S: Serializer,
              R: Reindexer<DTypes>;
}

/// Trait providing a method for applying a [Func](trait.Func.html) to data in data storage
/// structure. Implemented automatically by `data_types` macros where a `Func` is implemented
/// for all data types of a data storage structure.
pub trait Map<DTypes, F, FOut> where DTypes: AssocTypes
{
    /// Apply a [Func](trait.Func.html) to the data specified by a
    /// [FieldLocator](trait.FieldLocator.html).
    ///
    /// Fails if the field is not able to be located in this data structure.
    fn map<L>(&self, locator: &L, f: F) -> Result<FOut>
        where L: FieldLocator<DTypes>;
}
/// Trait providing a method for applying a [Func](trait.Func.html) to data in data storage
/// structure. Implemented automatically by `data_types` macros where a `Func` is implemented
/// for type `T`.
pub trait TMap<DTypes, T, F>
    where DTypes: AssocTypes,
          T: DataType<DTypes>,
          F: Func<DTypes, T>
{
    /// Apply a [Func](trait.Func.html) to the data specified by a
    /// [FieldLocator](trait.FieldLocator.html).
    ///
    /// Fails if the field is not able to be located in this data structure or has a different
    /// type than `T`.
    fn tmap<L>(&self, locator: &L, f: F) -> Result<F::Output>
        where L: FieldLocator<DTypes>;
}
/// Trait providing a method for applying a [FuncExt](trait.FuncExt.html) to data in data storage
/// structure. Implemented automatically by `data_types` macros where a `FuncExt` is implemented
/// for all data types of a data storage structure.
pub trait MapExt<DTypes: AssocTypes, F, FOut>
{
    /// Apply a [FuncExt](trait.Func.html) to the data specified by a
    /// [FieldLocator](trait.FieldLocator.html).
    ///
    /// Fails if the field is not able to be located in this data structure.
    fn map_ext<L>(&self, locator: &L, f: F) -> Result<FOut>
        where L: FieldLocator<DTypes>;
}
/// Trait providing a method for applying a [FuncPartial](trait.FuncPartial.html) to data in data
/// storage structure. Implemented automatically by `data_types` macros.
pub trait MapPartial<DTypes, F>
    where DTypes: DTypeList,
          F: FuncPartial<DTypes>
{
    /// Apply a [FuncPartial](trait.Func.html) to the data specified by a
    /// [FieldLocator](trait.FieldLocator.html).
    ///
    /// Fails if the field is not able to be located in this data structure.
    fn map_partial<L, R>(&self, locator: &L, reindexer: &R, f: F)
        -> Option<F::Output>
        where L: FieldLocator<DTypes>,
              R: Reindexer<DTypes>;
}

/// Trait for a function applied to a specified type `T` in `DTypes`. Used with the methods
/// [map](../view/struct.DataView.html#map) and [tmap](../view/struct.DataView.html#tmap)
/// in the [DataView](../view/struct.DataView.html) struct.
pub trait Func<DTypes, T>
{
    /// Return value of this function.
    type Output;
    /// This method is called with a trait object that implements
    /// [DataIndex](../access/trait.DataIndex.html), which provides access to a field's data.
    fn call(
        &mut self,
        data: &dyn DataIndex<DTypes, DType=T>,
    )
        -> Self::Output;
}

impl<DTypes, T, F, FOut> Func<DTypes, T> for F
    where F: FnMut(&dyn DataIndex<DTypes, DType=T>) -> FOut
{
    type Output = FOut;
    fn call(
        &mut self,
        data: &dyn DataIndex<DTypes, DType=T>,
    )
        -> FOut
    {
        self(data)
    }
}

/// Trait for a function applied to a specified type `T` in `DTypes`. Used with the method
/// [map_ext](../view/struct.DataView.html#map_ext) in the [DataView](../view/struct.DataView.html)
/// struct. `FuncExt`s are similar to [Func](trait.Func.html)s except they also provide information
/// about the type / location in storage field the `FuncExt` was called upon.
pub trait FuncExt<DTypes: AssocTypes, T>
{
    /// Return value of this function.
    type Output;
    /// This method is called with a trait object that implements
    /// [DataIndex](../access/trait.DataIndex.html), which provides access to a field's data,
    /// and a [FieldLocator](trait.FieldLocator.html) object containing location information.
    fn call<L>(
        &mut self,
        data: &dyn DataIndex<DTypes, DType=T>,
        locator: &L
    )
        -> Self::Output
        where L: FieldLocator<DTypes>;
}

/// Trait for a function operating over all types present in a list of data types.
pub trait FuncPartial<DTypes: DTypeList> {
    /// Return value of this function.
    type Output;
    /// This method is called with the storage struct associated with `DTypes`, a
    /// [FieldLocator](trait.FieldLocator.html)
    /// which provides the information of the field that was specified in the
    /// [map_partial](../view/struct.DataView.html#map_partial) call, and a
    /// [Reindexer](../frame/trait.Reindexer.html) which should be used to index into the
    /// storage structure.
    ///
    /// This method should return `None` if the function is not implemented for the data type of the
    /// field specified by `locator`, and `Some(...)` when the function is implemented.
    fn call_partial<L, R>(
        &mut self,
        locator: &L,
        reindexer: &R,
        storage: &DTypes::Storage,
    )
        -> Option<Self::Output>
        where L: FieldLocator<DTypes>,
              R: Reindexer<DTypes>;
}

/// Marker trait for types supported in this `DataTypes` set.
pub trait DataType<DTypes>: Debug + Display + GetDType<DTypes>
    where DTypes: AssocTypes
{}

/// Trait used to provide associated types, used with a list of data types.
pub trait AssocTypes {
    /// Associated enumeration of the data types associated with this
    /// [DTypeList](trait.DTypeList.html).
    type DType: Debug + Display + PartialEq + Copy + Eq + Hash;
    /// Associated enumeration which can contain a single value of any of the data types associated
    /// with this [DTypeList](trait.DTypeList.html).
    type DtValue: Debug + Display;
    /// Associated enumeration which can contain a field of values of any of the data types
    /// associated with this [DTypeList](trait.DTypeList.html).
    type DtField: Debug;
    /// Assocated storage structure.
    type Storage: Debug;
}

/// Trait that provides access to a [DType](trait.AssocTypes.html#DType). Implemented by data types
/// within `DTypes` to provide run-time access to the data type.
pub trait GetDType<DTypes> where DTypes: AssocTypes {
    /// [DType](trait.AssocTypes.html#DType) for this data type.
    const DTYPE: DTypes::DType;
}

/// Marker type for commonly needed traits for DataType lists
pub trait DTypeList: Debug + Clone + AssocTypes {}
impl<T> DTypeList for T where T: Debug + Clone + AssocTypes {}

/// Marker trait for a data storage structure that implements
/// [FieldSerialize](trait.FieldSerialize.html)
pub trait Serializable<DTypes>: MaxLen<DTypes> + FieldSerialize<DTypes>
    where DTypes: DTypeList {}
impl<T, DTypes> Serializable<DTypes> for T
    where T: MaxLen<DTypes> + FieldSerialize<DTypes>,
          DTypes: DTypeList {}

#[macro_export]
macro_rules! StorageTypes {
    // base case: ending in nil
    () => { $crate::data_types::StorageNil };
    // append another set of data types
    (++ $others:ty) => { $others };
    // end-comma elision
    ($dtype:ty) => { StorageTypes![$dtype,] };
    // setting up StorageCons type and recursion
    ($dtype:ty, $($tok:tt)*) => {
        $crate::data_types::StorageCons<Types, $dtype, StorageTypes![$($tok)*]>
    };
}

#[macro_export]
macro_rules! DataTypes {
    // base case: ending in nil
    () => { $crate::data_types::DTypeNil };
    // append another set of data types
    (++ $others:ty) => { $others };
    // end-comma elision
    ($dtype:ty) => { DataTypes![$dtype,] };
    // setting up DTypeCons type and recursion
    ($dtype:ty, $($tok:tt)*) => {
        $crate::data_types::DTypeCons<$dtype, DataTypes![$($tok)*]>
    };
}

fn type_mismatch_err<DTypes: AssocTypes, T>(ty: DTypes::DType) -> Result<T> {
    Err(AgnesError::TypeMismatch(format!("No type {:?} found", ty)))
}

#[macro_export]
macro_rules! data_types {
    (@as_item($i:item)) => {$i};

    /* Recurse through `tail` members of DTypeCons struct */

    // match one of the '+' marks, adding a '.tail' to move on to the next element of the DTypeCons
    (@tail_recurse($self:ident, + $($plus:tt)*)) => {
        data_types![@tail_recurse($self, $($plus)*)].tail
    };
    // end case: use `self`
    (@tail_recurse($self:ident,)) => { $self };

    (@impl_type_num_ser
        (init)
        ($($tok:tt)*)
        ->
        ()
    ) => {
        impl $crate::data_types::FieldSerialize<Types> for Storage
        {
            fn serialize<L, R, S>(
                &self,
                locator: &L,
                reindexer: &R,
                serializer: S
            )
                -> ::std::result::Result<S::Ok, S::Error>
                where S: ::serde::Serializer,
                      R: $crate::frame::Reindexer<Types>,
                      L: $crate::data_types::FieldLocator<Types>
            {
                data_types![
                    @impl_type_num_ser
                        (init)
                        (self, locator, reindexer, serializer)
                        ($($tok)*) -> ()
                ]
            }
        }
    };
    (@impl_type_num_ser
        (init$($plus:tt)*)
        ($self:ident, $locator:ident, $reindexer:ident, $serializer:ident)
        ($dtype:ty, $($tok:tt)*)
        ->
        ($($out:tt)*)
    ) => {
        data_types![@impl_type_num_ser
            (init+$($plus)*)
            ($self, $locator, $reindexer, $serializer)
            ($($tok)*)
            -> (
                $($out)*
                <$dtype as $crate::data_types::GetDType<Types>>::DTYPE => {
                    use $crate::field::Value;
                    use $crate::access::DataIndex;
                    use serde::ser::SerializeSeq;

                    let data = data_types![@tail_recurse($self, $($plus)*)].head
                        .get($locator.td_idx()).unwrap();
                    let mut seq = $serializer.serialize_seq(Some($reindexer.len()))?;
                    for idx in 0..$reindexer.len() {
                        match data.get_datum($reindexer.map_index(idx)).unwrap() {
                            Value::Exists(&ref val) =>  seq.serialize_element(val)?,
                            Value::Na =>  seq.serialize_element("null")?
                        }
                    }
                    seq.end()
                }
            )
        ];
    };
    (@impl_type_num_ser
        (init$($plus:tt)*)
        ($self:ident, $ds_field:ident, $reindexer:ident, $serializer:ident)
        ()
        ->
        ($($out:tt)*)
    ) => {
        #[allow(unreachable_patterns)]
        match $ds_field.ty() {
            $($out)*
            _ => Err(::serde::ser::Error::custom(format!("unknown type with type {:?}",
                $ds_field.ty())))
        }
    };

    (@impl_type_num_map(init)($($tok:tt)*) -> ()) => {
        impl<F, FOut> $crate::data_types::Map<Types, F, FOut> for Storage
            where F: FuncAllTypes<FOut>
        {
            fn map<L>(&self, locator: &L, mut f: F)
                -> $crate::error::Result<FOut>
                where L: $crate::data_types::FieldLocator<Types>
            {
                data_types![@impl_type_num_map(init)(self, f, locator)($($tok)*) -> ()]
            }
        }
        impl<F, FOut> $crate::data_types::MapExt<Types, F, FOut> for Storage
            where F: FuncAllTypesExt<FOut>
        {
            fn map_ext<L>(&self, locator: &L, mut f: F)
                -> $crate::error::Result<FOut>
                where L: $crate::data_types::FieldLocator<Types>
            {
                data_types![@impl_type_num_map_ext(init)(self, f, locator)($($tok)*) -> ()]
            }
        }
        impl<F> $crate::data_types::MapPartial<Types, F> for Storage
            where F: $crate::data_types::FuncPartial<Types>
        {
            fn map_partial<L, R>(&self, locator: &L, reindexer: &R, mut f: F)
                -> Option<F::Output>
                where L: $crate::data_types::FieldLocator<Types>,
                      R: $crate::frame::Reindexer<Types>,
            {
                f.call_partial(locator, reindexer, self)
            }
        }
    };
    (@impl_type_num_map
        (init$($plus:tt)*)
        ($self:ident, $f:ident, $locator:ident)
        ($dtype:ty, $($tok:tt)*)
        ->
        ($($out:tt)*)
    ) => {
        data_types![@impl_type_num_map
            (init+$($plus)*)
            ($self, $f, $locator)
            ($($tok)*)
            ->
            (
                $($out)*
                <$dtype as $crate::data_types::GetDType<Types>>::DTYPE =>
                    Ok($f.call(
                        data_types![@tail_recurse($self, $($plus)*)].head.get($locator.td_idx())
                            .unwrap(),
                    )),
            )
            ];
    };
    (@impl_type_num_map_ext
        (init$($plus:tt)*)
        ($self:ident, $f:ident, $locator:ident)
        ($dtype:ty, $($tok:tt)*)
        ->
        ($($out:tt)*)
    ) => {
        data_types![@impl_type_num_map_ext
            (init+$($plus)*)
            ($self, $f, $locator)
            ($($tok)*)
            ->
            (
                $($out)*
                <$dtype as $crate::data_types::GetDType<Types>>::DTYPE =>
                    Ok($f.call(
                        data_types![@tail_recurse($self, $($plus)*)].head.get($locator.td_idx())
                            .unwrap(),
                        $locator
                    )),
            )
            ];
    };
    (@impl_type_num_map
        (init$($plus:tt)*)
        ($self:ident, $f:ident, $locator:ident)
        ()
        ->
        ($($out:tt)*)
    ) => {
        #[allow(unreachable_patterns)]
        match $locator.ty() {
            $($out)*
            _ => $crate::data_types::type_mismatch_err::<Types, _>($locator.ty())
        }
    };
    (@impl_type_num_map_ext
        (init$($plus:tt)*)
        ($self:ident, $f:ident, $locator:ident)
        ()
        ->
        ($($out:tt)*)
    ) => {
        #[allow(unreachable_patterns)]
        match $locator.ty() {
            $($out)*
            _ => $crate::data_types::type_mismatch_err::<Types, _>($locator.ty())
        }
    };

    (@impl_tmap($($dtype:ty,)*)) => {$(
        impl<F> $crate::data_types::TMap<Types, $dtype, F> for Storage
            where $dtype: DataType,
                  F: $crate::data_types::Func<Types, $dtype>
        {
            fn tmap<L>(&self, locator: &L, mut f: F) -> $crate::error::Result<F::Output>
                where L: $crate::data_types::FieldLocator<Types>
            {
                Ok(f.call(self.select_type::<$dtype>().get(locator.td_idx()).unwrap()))
            }
        }
    )*};

    /* type selector implementation */

    // normal step: implement TypeSelector for type at this point of DTypeCons and recurse
    (@impl_type_selector(init$($plus:tt)*) $dtype:ty, $($tok:tt)*) => {
        impl $crate::data_types::TypeSelector<Types, $dtype> for Storage
        {
            fn select_type<'a>(&'a self) -> &'a $crate::data_types::TypeData<Types, $dtype> {
                &data_types![@tail_recurse(self, $($plus)*)].head
            }
            fn select_type_mut<'a>(&'a mut self)
                -> &'a mut $crate::data_types::TypeData<Types, $dtype>
            {
                &mut data_types![@tail_recurse(self, $($plus)*)].head
            }
        }
        impl $crate::data_types::DTypeSelector<Types, $dtype> for Storage
            where Types: $crate::data_types::AssocTypes
        {
            fn select_dtype(&self) -> <Types as $crate::data_types::AssocTypes>::DType {
                <$dtype as $crate::data_types::GetDType<Types>>::DTYPE
            }
        }

        data_types![@impl_type_selector(init+$($plus)*) $($tok)*];
    };
    // end case: noop
    (@impl_type_selector(init$($plus:tt)*)) => {};

    /* add vector implementation */

    // normal step: implement AddVec for type at this point of DTypeCons and recurse
    (@impl_add_vec(init$($plus:tt)*) $dtype:ty, $($tok:tt)*) => {
        impl $crate::data_types::AddVec<$dtype> for Storage
        {
            fn add_vec(&mut self) -> $crate::error::Result<usize> {
                let type_data = &mut data_types![@tail_recurse(self, $($plus)*)].head;
                let td_idx = type_data.len();
                type_data.push($crate::field::FieldData::default());
                Ok(td_idx)
            }
        }

        data_types![@impl_add_vec(init+$($plus)*) $($tok)*];
    };
    // end case: noop
    (@impl_add_vec(init$($plus:tt)*)) => {};


    /* DtField, DType, DtValue enums */

    // handle end-comma elision
    (@impl_dtenums $($tok:tt),*) => {
        data_types![@impl_dtenums $($tok,)*];
    };
    (@impl_dtenums $($dtype:tt,)*) => {
        mashup! {
            $(
                variantify["variant" $dtype] = $dtype;
            )*
        }

        variantify! {
            /// Enumeration which holds a [DataIndex](../../access/trait.DataIndex.html) trait
            /// object of any of the available types.
            #[allow(non_camel_case_types)]
            #[derive(Debug)]
            pub enum DtField {$(
                /// Field of type $dtype.
                "variant" $dtype(Box<dyn $crate::data_types::DataIndex<Types, DType=$dtype>>),
            )*}
        }
        variantify! {
            /// Enumeration which can hold any value of the available data types.
            #[allow(non_camel_case_types)]
            #[derive(Debug, Clone)]
            pub enum DtValue {$(
                /// Value for type $dtype.
                "variant" $dtype($dtype),
            )*}
        }
        variantify! {
            impl ::std::fmt::Display for DtValue {
                fn fmt(&self, f: &mut ::std::fmt::Formatter)
                    -> ::std::result::Result<(), ::std::fmt::Error>
                {
                    match *self {
                        $(
                        DtValue::"variant" $dtype(ref x) => write!(f, "{}", x),
                        )*
                    }
                }
            }
        }
        variantify! {$(
            impl From<$dtype> for DtValue {
                fn from(other: $dtype) -> DtValue {
                    DtValue::"variant" $dtype(other)
                }
            }
        )*}
        variantify! {
            /// Enumeration of the data types available.
            #[allow(non_camel_case_types)]
            #[derive(Debug, Clone, PartialEq, Eq, Copy, Hash)]
            pub enum DType {$(
                /// Data type specifier for type $dtype.
                "variant" $dtype,
            )*}
        }
        variantify! {
            $(
            impl $crate::data_types::GetDType<Types> for $dtype {
                const DTYPE: DType = DType::"variant" $dtype;
            }
            )*
        }
        variantify! {
            impl ::std::fmt::Display for DType {
                fn fmt(&self, f: &mut ::std::fmt::Formatter)
                    -> ::std::result::Result<(), ::std::fmt::Error>
                {
                    match *self {
                        $(
                        DType::"variant" $dtype => write!(f, "{}", stringify![$dtype]),
                        )*
                    }
                }
            }
        }
    };

    /* FuncAllTypes trait declaration */

    // initial step -- no existing output: add initial output
    (@func_all_types()() $dtype:ty, $($tok:tt)*) => {
        data_types![
            @func_all_types
                ($crate::data_types::Func<Types, $dtype, Output=FOut>)
                ($crate::data_types::FuncExt<Types, $dtype, Output=FOut>)
            $($tok)*
        ];
    };
    // non-initial step: continue adding Func traits
    (@func_all_types
        ($($output:tt)*)
        ($($ext_output:tt)*)
        $dtype:ty,
        $($tok:tt)*
    ) => {
        data_types![
            @func_all_types
                ($($output)* + $crate::data_types::Func<Types, $dtype, Output=FOut>)
                ($($ext_output)* + $crate::data_types::FuncExt<Types, $dtype, Output=FOut>)
                $($tok)*
        ];
    };
    // final step: actually declare FuncAllTypes trait and implement
    (@func_all_types
        ($($output:tt)*)
        ($($ext_output:tt)*)
    ) => {
        /// Marker trait for data storage that implements `Map` for `Func`s of all data types
        /// supported by this data storage.
        pub trait FuncAllTypes<FOut>: $($output)* {}
        impl<F, FOut> FuncAllTypes<FOut> for F where F: $($output)* {}
        /// Marker trait for data storage that implements `MapExt` for `FuncExt`s of all data types
        /// supported by this data storage.
        pub trait FuncAllTypesExt<FOut>: $($ext_output)* {}
        impl<F, FOut> FuncAllTypesExt<FOut> for F where F: $($ext_output)* {}
    };

    /* Implementation bundle for stuff that can be coerced into types */

    //handle end-comma elision
    (@impl_ty_based $($dtype:ty),*) => {
        data_types![@impl_ty_based $($dtype,)*];
    };
    // normal ty-based implementation
    (@impl_ty_based $($dtype:ty,)*) => {
        /// The available data types available for storage.
        pub type Types = DataTypes![$($dtype),*];
        /// The storage structure.
        pub type Storage = StorageTypes![$($dtype),*];
        /// Convenience type alias for a [DataFrame](../../frame/struct.DataFrame.html) generic
        /// over type `Types`.
        pub type DataFrame = $crate::frame::DataFrame<Types>;
        /// Convenience type alias for a [DataStore](../../store/struct.DataStore.html) generic
        /// over type `Types`.
        pub type DataStore = $crate::store::DataStore<Types>;
        /// Convenience type alias for a [DataView](../../view/struct.DataView.html) generic over
        /// type `Types`.
        pub type DataView = $crate::view::DataView<Types>;

        /// Convenience marker trait for a [DataType](../trait.DataType.html) generic over type
        /// `Types`.
        pub trait DataType: $crate::data_types::DataType<Types> {}
        impl<T> DataType for T where T: $crate::data_types::DataType<Types> {}

        impl $crate::data_types::AssocTypes for Types {
            type DType = DType;
            type DtValue = DtValue;
            type DtField = DtField;
            type Storage = Storage;
        }

        data_types![@impl_type_selector(init) $($dtype,)*];

        data_types![@func_all_types()() $($dtype,)*];

        data_types![@impl_add_vec(init) $($dtype,)*];

        data_types![@impl_type_num_map(init)($($dtype,)*) -> ()];
        data_types![@impl_tmap($($dtype,)*)];
        data_types![@impl_type_num_ser(init)($($dtype,)*) -> ()];

        $(
            impl $crate::data_types::DataType<Types> for $dtype {}
        )*
    };

    /* Implementation bundle for stuff that cannot be coerced into types */
    (@impl_tt_based $($tok:tt)*) => {
        data_types![@impl_dtenums $($tok)*];
    };

    /* Main entry point */
    ($($tok:tt)*) => {
        // coerce arguments into 'ty's and implement
        data_types![@impl_ty_based $($tok)*];
        // treat arguments as 'tt's and implement (needed for mashup!-based implementations)
        data_types![@impl_tt_based $($tok)*];
    };
}

macro_rules! register_partial_func {
    (
        $(
        impl FuncPartial<$dtypes:path> for $func:path {
            type Output = $output:path;
            implemented = [$($impl_type:ident),*];
        }
        )*
    ) => (
        $(
        impl $crate::data_types::FuncPartial<$dtypes> for $func {
            type Output = $output;

            fn call_partial<L, R> (
                &mut self,
                locator: &L,
                reindexer: &R,
                storage: &<$dtypes as $crate::data_types::AssocTypes>::Storage
            )
                -> Option<$output>
                where L: $crate::data_types::FieldLocator<$dtypes>,
                      R: $crate::frame::Reindexer<$dtypes>
            {
                use $crate::data_types::Func;
                #[allow(unreachable_patterns)]
                match locator.ty() {
                $(
                    DType::$impl_type => Some(
                        self.call(
                            &reindexer.reindex(
                                storage.select_type::<$impl_type>().get(locator.td_idx()).unwrap()
                            )
                        ).into(),
                    ),
                )*
                    _ => None,
                }
            }
        }
        )*
    )
}

pub mod csv {
    //! Data type list for CSV files.

    data_types![u64, i64, String, bool, f64];

    register_partial_func![
        impl FuncPartial<Types> for ::apply::stats::SumSqFn {
            type Output = DtValue;
            implemented = [u64, i64, bool, f64];
        }
        impl FuncPartial<Types> for ::apply::stats::MinFn {
            type Output = DtValue;
            implemented = [u64, i64, bool, f64];
        }
        impl FuncPartial<Types> for ::apply::stats::MaxFn {
            type Output = DtValue;
            implemented = [u64, i64, bool, f64];
        }
        impl FuncPartial<Types> for ::apply::stats::SumFn {
            type Output = DtValue;
            implemented = [u64, i64, bool, f64];
        }
        impl FuncPartial<Types> for ::apply::stats::MeanFn {
            type Output = DtValue;
            implemented = [u64, i64, bool, f64];
        }
        impl FuncPartial<Types> for ::apply::stats::StdevFn {
            type Output = DtValue;
            implemented = [u64, i64, bool, f64];
        }
    ];

    pub mod ops {
        //! All arithmetic operations between fields or between a field and a scalar.

        use super::Types as Types;
        scalar_ops![Types => u64, i64, f64];
        field_ops! [Types => u64, i64, f64];
    }
}
pub mod standard {
    //! A standard data type list for general basic use.

    data_types![u64, i64, String, bool, f64, u32, i32, f32,];

    register_partial_func![
        impl FuncPartial<Types> for ::apply::stats::SumSqFn {
            type Output = DtValue;
            implemented = [u64, i64, bool, f64, u32, i32, f32];
        }
        impl FuncPartial<Types> for ::apply::stats::MinFn {
            type Output = DtValue;
            implemented = [u64, i64, String, bool, f64, u32, i32, f32];
        }
        impl FuncPartial<Types> for ::apply::stats::MaxFn {
            type Output = DtValue;
            implemented = [u64, i64, String, bool, f64, u32, i32, f32];
        }
        impl FuncPartial<Types> for ::apply::stats::SumFn {
            type Output = DtValue;
            implemented = [u64, i64, bool, f64, u32, i32, f32];
        }
        impl FuncPartial<Types> for ::apply::stats::MeanFn {
            type Output = DtValue;
            implemented = [u64, i64, bool, f64, u32, i32, f32];
        }
        impl FuncPartial<Types> for ::apply::stats::StdevFn {
            type Output = DtValue;
            implemented = [u64, i64, bool, f64, u32, i32, f32];
        }
    ];

    pub mod ops {
        //! All arithmetic operations between fields or between a field and a scalar.

        use super::Types as Types;
        scalar_ops![Types => u64, i64, f64, u32, i32, f32];
        field_ops! [Types => u64, i64, f64, u32, i32, f32];
    }
}
