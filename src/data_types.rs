use std::hash::Hash;
use std::fmt::{Debug, Display};
use std::marker::PhantomData;

use serde::Serializer;

use access::DataIndex;
use field::FieldData;
use frame::Reindexer;
use error::*;

pub type TypeNum = usize;
pub type TypeData<DTypes, T> = Vec<FieldData<DTypes, T>>;

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct Nil;
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct Cons<H, T> {
    pub head: H,
    pub tail: T,
}

pub fn cons<H, T>(head: H, tail: T) -> Cons<H, T> {
    Cons {
        head,
        tail
    }
}

pub trait Prepend {
    fn prepend<H>(self, head: H) -> Cons<H, Self> where Self: Sized {
        cons(head, self)
    }
}
impl<H, T> Prepend for Cons<H, T> {}
impl Prepend for Nil {}

pub trait Append<U> {
    type Appended;
    fn append(self, elem: U) -> Self::Appended;
}
impl<U> Append<U> for Nil {
    type Appended = Cons<U, Nil>;
    fn append(self, elem: U) -> Cons<U, Nil> {
        cons(elem, Nil)
    }
}
impl<U, H, T> Append<U> for Cons<H, T> where T: Append<U> {
    type Appended = Cons<H, T::Appended>;
    fn append(self, elem: U) -> Cons<H, T::Appended> {
        cons(self.head, self.tail.append(elem))
    }
}

#[macro_export]
macro_rules! map {
    (@continue($elems:expr)($($output:tt)*) ) => {
        $($output)*
    };
    (@continue($elems:expr)($($output:tt)*) [$($f0:tt)*] $([$($f:tt)*])*) => {
        map![@continue($elems.tail)($($output)*.prepend(($($f0)*)(&$elems.head))) $([$($f)*])*]
    };
    ($elems:expr, $([$($f:tt)*])*) => {{
        use $crate::data_types::Prepend;
        map![@continue($elems)(Nil) $([$($f)*])*]
    }}
}

#[derive(Debug, Clone)]
pub struct DTypeNil;

#[derive(Debug, Clone)]
pub struct DTypeCons<H, T> {
    head: PhantomData<H>,
    tail: PhantomData<T>
}

#[derive(Debug, Clone)]
pub struct StorageNil;

#[derive(Debug, Clone)]
pub struct StorageCons<DTypes: DTypeList, H: DataType<DTypes>, T> {
    head: TypeData<DTypes, H>,
    tail: T,
    _marker: PhantomData<DTypes>,
}

pub trait CreateStorage {
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
            _marker: PhantomData,
        }
    }
}

/// A trait for selecting all fields of `Target` type from storage. Typically, you would use the
/// `select_type` and `select_type_mut` inherent methods on StorageCons instead.
pub trait TypeSelector<DTypes, Target>
    where DTypes: DTypeList,
          Target: DataType<DTypes>
{
    fn select_type<'a>(&'a self) -> &'a TypeData<DTypes, Target>;
    fn select_type_mut<'a>(&'a mut self) -> &'a mut TypeData<DTypes, Target>;
}

/// A trait for finding the `DType` for a specitifed `Target` type. Typically, you would use
/// the `select_type_num` inherent method on StorageCons instead.
pub trait DTypeSelector<DTypes, Target> where DTypes: AssocTypes {
    fn select_dtype(&self) -> DTypes::DType;
}

/// Trait for adding a data vector to the specified `Target` type.
pub trait AddVec<Target>
{
    fn add_vec(&mut self) -> Result<TypeNum>;
}

impl<DTypes, H, T> StorageCons<DTypes, H, T>
    where DTypes: DTypeList,
          H: DataType<DTypes>
{
    pub fn select_type<'a, Target>(&'a self) -> &'a TypeData<DTypes, Target>
        where Target: DataType<DTypes>, Self: TypeSelector<DTypes, Target>
    {
        TypeSelector::select_type(self)
    }
    pub fn select_type_mut<'a, Target>(&'a mut self) -> &'a mut TypeData<DTypes, Target>
        where Target: DataType<DTypes>, Self: TypeSelector<DTypes, Target>
    {
        TypeSelector::select_type_mut(self)
    }
    pub fn select_dtype<Target>(&self) -> DTypes::DType
        where Self: DTypeSelector<DTypes, Target>
    {
        DTypeSelector::select_dtype(self)
    }
}

// Maximum length
pub trait MaxLen<DTypes> {
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

pub trait FieldLocator<DTypes>
    where DTypes: AssocTypes
{
    fn ty(&self) -> DTypes::DType;
    fn td_idx(&self) -> usize;
}

pub trait FieldSerialize<DTypes> where DTypes: DTypeList
{
    fn serialize<L, R, S>(&self, locator: &L, reindexer: &R, serializer: S)
        -> ::std::result::Result<S::Ok, S::Error>
        where L: FieldLocator<DTypes>,
              S: Serializer,
              R: Reindexer<DTypes>;
}

pub trait Map<DTypes, F, FOut> where DTypes: AssocTypes
{
    fn map<L>(&self, locator: &L, f: F) -> Result<FOut>
        where L: FieldLocator<DTypes>;
}
pub trait TMap<DTypes, T, F>
    where DTypes: AssocTypes,
          T: DataType<DTypes>,
          F: Func<DTypes, T>
{
    fn tmap<L>(&self, locator: &L, f: F) -> Result<F::Output>
        where L: FieldLocator<DTypes>;
}
pub trait MapExt<DTypes: AssocTypes, F, FOut>
{
    fn map_ext<L>(&self, locator: &L, f: F) -> Result<FOut>
        where L: FieldLocator<DTypes>;
}
pub trait MapPartial<DTypes, F>
    where DTypes: DTypeList,
          F: FuncPartial<DTypes>
{
    fn map_partial<L, R>(&self, locator: &L, reindexer: &R, f: F)
        -> Option<F::Output>
        where L: FieldLocator<DTypes>,
              R: Reindexer<DTypes>;
}

pub trait Func<DTypes, T>
{
    type Output;
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

pub trait FuncExt<DTypes: AssocTypes, T>
{
    type Output;
    fn call<L>(
        &mut self,
        data: &dyn DataIndex<DTypes, DType=T>,
        locator: &L
    )
        -> Self::Output
        where L: FieldLocator<DTypes>;
}
/// Trait for a function operating over all types present in a `DataType` list.
pub trait FuncPartial<DTypes: DTypeList> {
    type Output;
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

pub trait AssocTypes {
    type DType: Debug + Display + PartialEq + Copy + Eq + Hash;
    type DtValue: Debug + Display;
    type DtField: Debug;
    type Storage: Debug;
}

pub trait GetDType<DTypes> where DTypes: AssocTypes {
    const DTYPE: DTypes::DType;
}

/// Marker type for commonly needed traits for DataType lists
pub trait DTypeList: Debug + Clone + AssocTypes {}
impl<T> DTypeList for T where T: Debug + Clone + AssocTypes {}

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
            _ => Err(::serde::ser::Error::custom(format!("unknown type with TypeNum {:?}",
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
                type_data.push($crate::field::FieldData::new());
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
            #[allow(non_camel_case_types)]
            #[derive(Debug)]
            pub enum DtField {$(
                "variant" $dtype(Box<dyn $crate::data_types::DataIndex<Types, DType=$dtype>>),
            )*}
        }
        variantify! {
            #[allow(non_camel_case_types)]
            #[derive(Debug, Clone)]
            pub enum DtValue {$(
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
            #[allow(non_camel_case_types)]
            #[derive(Debug, Clone, PartialEq, Eq, Copy, Hash)]
            pub enum DType {$(
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
        pub trait FuncAllTypes<FOut>: $($output)* {}
        impl<F, FOut> FuncAllTypes<FOut> for F where F: $($output)* {}
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
        pub type Types = DataTypes![$($dtype),*];
        pub type Storage = StorageTypes![$($dtype),*];
        pub type DataFrame = $crate::frame::DataFrame<Types>;
        pub type DataStore = $crate::store::DataStore<Types>;
        pub type DataView = $crate::view::DataView<Types>;

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
        use super::Types as Types;
        scalar_ops![Types => u64, i64, f64];
        field_ops! [Types => u64, i64, f64];
    }
}
pub mod standard {
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
        use super::Types as Types;
        scalar_ops![Types => u64, i64, f64, u32, i32, f32];
        field_ops! [Types => u64, i64, f64, u32, i32, f32];
    }
}
