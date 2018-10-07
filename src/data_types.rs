use std::iter::FromIterator;
use std::borrow::Borrow;
use std::hash::{Hash, Hasher};
use std::collections::HashSet;
use std::fmt::{Debug, Display};
use std::marker::PhantomData;

use serde::Serializer;

use access::{DataIndex};
use field::{FieldIdent, FieldData, Value};
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

// pub struct ValueNil;
// pub struct ValueCons<'a, DTypes, H, T>
//     where DTypes: DTypeList,
//           H: 'a + DataType<DTypes>,
// {
//     head: Vec<Value<&'a H>>,
//     tail: T,
//     _marker: PhantomData<DTypes>
// }

#[derive(Debug, Clone)]
pub struct PartialValueNil;
#[derive(Debug, Clone)]
pub struct PartialValueCons<'a, DTypes, H, T>
    where DTypes: DTypeList,
          H: 'a + DataType<DTypes>,
{
    head: Vec<Option<Value<&'a H>>>,
    tail: T,
    _marker: PhantomData<DTypes>
}

// pub trait RetrieveValues<'a, S> {
//     fn retrieve_values(storage: &'a S, idx: usize,) -> Self;
// }
// impl<'a> RetrieveValues<'a, StorageNil> for ValueNil {
//     fn retrieve_values(_storage: &'a StorageNil, _idx: usize) -> ValueNil {
//         ValueNil
//     }
// }
// impl<'a, DTypes, H, T> RetrieveValues<'a, StorageCons<DTypes, H, T>>
//     for ValueCons<'a, DTypes, H, T>
//     where DTypes: DTypeList,
//           H: DataType<DTypes>,
//           T: RetrieveValues<'a, T>
// {
//     fn retrieve_values(storage: &'a StorageCons<DTypes, H, T>, idx: usize)
//         -> ValueCons<'a, DTypes, H, T>
//     {
//         ValueCons {
//             head: storage.head.iter()
//                 .map(|field_data| field_data.get_datum(idx).unwrap())
//                 .collect(),
//             tail: T::retrieve_values(&storage.tail, idx),
//             _marker: PhantomData
//         }
//     }
// }

pub trait RetrieveValuesPartial<'a, DTypes, S>
    where DTypes: AssocTypes
{
    fn retrieve_values_partial(
        storage: &'a S,
        locators: &FieldLocatorSet<DTypes>,
        idx: usize
    )
        -> Self;
}
impl<'a, DTypes> RetrieveValuesPartial<'a, DTypes, StorageNil> for PartialValueNil
    where DTypes: AssocTypes,
{
    fn retrieve_values_partial(
        _storage: &'a StorageNil,
        _locators: &FieldLocatorSet<DTypes>,
        _idx: usize
    )
        -> PartialValueNil
    {
        PartialValueNil
    }
}
impl<'a, DTypes, H, T> RetrieveValuesPartial<'a, DTypes, StorageCons<DTypes, H, T>>
    for PartialValueCons<'a, DTypes, H, T>
    where DTypes: DTypeList,
          H: DataType<DTypes>,
          T: RetrieveValuesPartial<'a, DTypes, T>
{
    fn retrieve_values_partial(
        storage: &'a StorageCons<DTypes, H, T>,
        locators: &FieldLocatorSet<DTypes>,
        idx: usize,
    )
        -> PartialValueCons<'a, DTypes, H, T>
    {
        let values = (0..storage.head.len()).map(|td_idx| {
            let locator = SimpleFieldLocator {
                ty: H::DTYPE,
                td_idx,
            };
            if locators.contains(&locator) {
                Some(storage.head[td_idx].get_datum(idx).unwrap())
            } else {
                None
            }
        }).collect();
        PartialValueCons {
            head: values,
            tail: T::retrieve_values_partial(&storage.tail, locators, idx),
            _marker: PhantomData,
        }
    }
}

// pub trait CombineValues<'a, DTypes, Other>
//     where DTypes: AssocTypes
// {
//     fn combine_values(
//         self,
//         other: Other
//     )
//         -> Self;
// }
// impl<'a, DTypes> CombineValues<'a, DTypes, PartialValueNil> for PartialValueNil
//     where DTypes: AssocTypes,
// {
//     fn combine_values(
//         self,
//         _other: PartialValueNil
//     )
//         -> PartialValueNil
//     {
//         self
//     }
// }
// impl<'a, DTypes, H, T> CombineValues<'a, DTypes, PartialValueCons<'a, DTypes, H, T>>
//     for PartialValueCons<'a, DTypes, H, T>
//     where DTypes: AssocTypes,
// {
//     fn combine_values(
//         self,
//         other: PartialValueCons<'a, DTypes, H, T>
//     )
//         -> PartialValueCons<'a, DTypes, H, T>
//     {
//         //TODO: figure out if / when this can happen: PartialRecords from two different DataStores?
//         // which is definitely something that can, and normally will, happen.
//         assert_eq!(self.head.len(), other.head.len());

//         PartialValueCons {
//             head: ,
//             tail: ,
//             _marker: PhantomData
//         }
//     }
// }

// pub trait DTypeList: TypeNumMappable {}
// impl<D> DTypeList for D where D: TypeNumMappable {}

// pub trait TypeNumMappable {
//     const TYPE_NUM: TypeNum;
//     fn is_type_num(type_num: TypeNum) -> bool { type_num == Self::TYPE_NUM }
//     fn type_num() -> usize { Self::TYPE_NUM }
// }
// impl TypeNumMappable for StorageNil {
//     const TYPE_NUM: TypeNum = 0;
// }
// impl<H, T: TypeNumMappable> TypeNumMappable for StorageCons<H, T> {
//     const TYPE_NUM: TypeNum = T::TYPE_NUM + 1;
// }

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

// pub struct Record<'a, DTypes>
//     where DTypes: DTypeList,
// {
//     fields: DTypes::RecordValues<'a>,
//     idents: Vec<FieldIdent>,
// }


// pub trait RecordSelector<DTypes>
//     where DTypes: DTypeList,
// {
//     fn record<'a>(&'a self) -> Record<'a, DTypes>;
// }


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

// pub trait Map<F, FOut>
// {
//     fn map(&mut self, ds_field: &DsField, f: F) -> Result<FOut>;
// }


// pub trait AssociatedValue<'a> {
//     type DtValue;
// }



// impl TypeNumMappable for DTypeNil {
//     const TYPE_NUM: TypeNum = 0;
// }
// impl<H, T: IsTypeNum> IsTypeNum for DTypeCons<H, T> {
//     const TYPE_NUM: TypeNum = T::TYPE_NUM + 1;
// }
// impl NewTypeData for DTypeNil {
//     fn new() -> DTypeNil { DTypeNil }
// }


// impl<H: Clone, T: DTypeList> NewTypeData for DTypeCons<H, T> {
//     fn new() -> DTypeCons<H, T> {
//         DTypeCons {
//             head: TypeData::<H>::new(),
//             tail: T::new(),
//         }
//     }
// }
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

// pub struct TypeSelection<'a, T: 'a> {
//     pub(crate) data: &'a TypeData<T>,
//     pub(crate) num: TypeNum,
// }
// pub struct TypeSelectionMut<'a, T: 'a> {
//     pub(crate) data: &'a mut TypeData<T>,
//     pub(crate) num: TypeNum,
// }

// Dummy index structs
pub struct Idx0 { _marker: () }
pub struct Idx1<T> { _marker: PhantomData<T> }

// THOUGHT: move Idx into DTypeCons type definition?
// pub struct DTypeCons<H, I, T> { head: TypeData<H>, tail: T, phantom }

// impl<Target, Tail> TypeSelector<Target> for DTypeCons<Target, Tail> {
//     fn select_type(&self) -> &TypeData<Target> {
//         &self.head
//     }
//     fn select_type_mut(&mut self) -> (TypeNum, &mut TypeData<Target>) {
//         (0, &mut self.head)
//     }
// }
// impl<Head, Target, Tail> TypeSelector<Target> for DTypeCons<Head, DTypeCons<Target, Tail>> {
//     fn select_type(&self) -> &TypeData<Target> {
//         &self.head
//     }
//     fn select_type_mut(&mut self) -> (TypeNum, &mut TypeData<Target>) {
//         (0, &mut self.head)
//     }

// }


// pub trait TypeSelector<Target, Index> {
//     fn select_type(&self) -> &TypeData<Target>;
//     fn select_type_mut(&mut self) -> (TypeNum, &mut TypeData<Target>);
// }
// impl<Target, Tail> TypeSelector<Target, Idx0> for DTypeCons<Target, Tail>
//     where DTypeCons<Target, Tail>: DTypeList
// {
//     fn select_type(&self) -> &TypeData<Target> {
//         &self.head
//     }
//     fn select_type_mut(&mut self) -> (TypeNum, &mut TypeData<Target>) {
//         (Self::TYPE_NUM, &mut self.head)
//     }
// }
// impl<Head, Tail, FromTail, TailIndex> TypeSelector<FromTail, Idx1<TailIndex>>
//     for DTypeCons<Head, Tail>
//     where Tail: TypeSelector<FromTail, TailIndex>,
//           DTypeCons<Head, Tail>: DTypeList
// {
//     fn select_type(&self) -> &TypeData<FromTail> {
//         self.tail.select_type()
//     }
//     fn select_type_mut(&mut self) -> (TypeNum, &mut TypeData<FromTail>) {
//         self.tail.select_type_mut()
//     }
// }

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

// impl<'a, Head: 'a, Tail> AssociatedValue<'a> for DTypeCons<Head, Tail>
//     where Tail: AssociatedValue<'a>
// {
//     type DtValue = DtValue<'a, Head, Tail::DtValue>;
// }

// pub trait DtValueForTypeNum<'a, DTypes, Idx>
//     where DTypes: AssociatedValue<'a>
// {
//     fn dt_value_for_type_num(&'a self, type_num: TypeNum) -> Result<DTypes::DtValue>;
// }
// impl<'a, DTypes, Idx> DtValueForTypeNum<'a, DTypes, Idx> for DTypeNil
//     where DTypes: AssociatedValue<'a>
// {
//     fn dt_value_for_type_num(&'a self, type_num: TypeNum) -> Result<DTypes::DtValue> {
//         Err(AgnesError::TypeMismatch(format!("No type with TypeNum {} found", type_num)))
//     }
// }
// impl<'a, DTypes, Head, Tail, Idx> DtValueForTypeNum<'a, DTypes, Idx> for DTypeCons<Head, Tail>
//     where DTypes: DTypeList + AssociatedValue<'a>,
//           DTypes::DtValue: DtValueAdder<'a, Head, Idx>,
//           Head: 'static + DataType + Default,
//           // F: FnAll<'a, DTypes>,
//           DTypeCons<Head, Tail>: DTypeList,
//           Tail: DtValueForTypeNum<'a, DTypes, Idx>,
// {
//     fn dt_value_for_type_num(&'a self, type_num: TypeNum) -> Result<DTypes::DtValue>
//     {
//         if Self::is_type_num(type_num) {
//             Ok(DTypes::DtValue::add(&self.head))
//             // Ok(f.call(&self.head))
//             // Ok(f.call(DTypes::DtValue::add(&self.head)))
//         } else {
//             self.tail.dt_value_for_type_num(type_num)
//         }
//     }
// }

fn type_mismatch_err<DTypes: AssocTypes, T>(ty: DTypes::DType) -> Result<T> {
    Err(AgnesError::TypeMismatch(format!("No type {:?} found", ty)))
}

// pub trait TypeNumAddVec
// {
//     fn add_vec(&mut self, type_num: TypeNum) -> Result<usize>;
// }
// impl TypeNumAddVec for DTypeNil
// {
//     fn add_vec(&mut self, type_num: TypeNum) -> Result<usize> {
//         type_mismatch_err(type_num)
//     }
// }
// impl<Head, Tail> TypeNumAddVec
//     for DTypeCons<Head, Tail>
//     where DTypeCons<Head, Tail>: DTypeList,
//           Head: DataType,
//           Tail: TypeNumAddVec
// {
//     fn add_vec(&mut self, type_num: TypeNum) -> Result<usize> {
//         if DTypeCons::<Head, Tail>::is_type_num(type_num) {
//             let td_idx = self.head.len();
//             self.head.push(FieldData::new());
//             Ok(td_idx)
//         } else {
//             self.tail.add_vec(type_num)
//         }
//     }
// }

pub trait FieldLocator<DTypes>
    where DTypes: AssocTypes
{
    fn ty(&self) -> DTypes::DType;
    fn td_idx(&self) -> usize;
}

#[derive(Debug, Clone)]
pub struct SimpleFieldLocator<DTypes> where DTypes: AssocTypes {
    ty: DTypes::DType,
    td_idx: usize,
}
impl<DTypes> PartialEq for SimpleFieldLocator<DTypes>
    where DTypes: AssocTypes
{
    fn eq(&self, other: &SimpleFieldLocator<DTypes>) -> bool {
        self.ty == other.ty && self.td_idx == other.td_idx
    }
}
impl<DTypes> Eq for SimpleFieldLocator<DTypes> where DTypes: AssocTypes {}
impl<DTypes> Hash for SimpleFieldLocator<DTypes>
    where DTypes: AssocTypes
{
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.ty.hash(state);
        self.td_idx.hash(state);
    }
}
impl<DTypes> SimpleFieldLocator<DTypes> where DTypes: AssocTypes {
    pub fn from_locator<L>(orig: L) -> SimpleFieldLocator<DTypes> where L: FieldLocator<DTypes> {
        SimpleFieldLocator {
            ty: orig.ty(),
            td_idx: orig.td_idx()
        }
    }
}
impl<DTypes> FieldLocator<DTypes> for SimpleFieldLocator<DTypes>
    where DTypes: AssocTypes
{
    fn ty(&self) -> DTypes::DType { self.ty }
    fn td_idx(&self) -> usize { self.td_idx }
}

pub struct FieldLocatorSet<DTypes>(HashSet<SimpleFieldLocator<DTypes>>)
    where DTypes: AssocTypes;
impl<DTypes, L> FromIterator<L> for FieldLocatorSet<DTypes>
    where DTypes: AssocTypes,
          L: FieldLocator<DTypes>
{
    fn from_iter<T>(iter: T) -> FieldLocatorSet<DTypes>
        where T: IntoIterator<Item=L>
    {
        FieldLocatorSet(
            iter.into_iter()
                .map(|field_locator| SimpleFieldLocator::from_locator(field_locator))
                .collect()
        )
    }
}
impl<DTypes> FieldLocatorSet<DTypes>
    where DTypes: AssocTypes,
{
    pub fn contains(&self, value: &SimpleFieldLocator<DTypes>) -> bool {
        self.0.contains(value)
    }
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
// pub trait FuncMut<T> {
//     type Output;
//     fn call(
//         &mut self,
//         data: &mut dyn DataIndex<DType=T>,
//         ds_field: &DsField,
//     )
//         -> Self::Output;
// }
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




// pub trait TypeNumMapOpt<F, FOut, Flags>
// {
//     fn map_opt(&self, ds_field: &DsField, f: F) -> Result<Option<FOut>>;
// }
// pub trait TypeNumMapMut<F, FOut>
// {
//     fn map_mut(&mut self, ds_field: &DsField, f: F) -> Result<FOut>;
// }
// impl<F, FOut> TypeNumMapMut<F, FOut> for DTypeNil
// {
//     fn map_mut(&mut self, ds_field: &DsField, _f: F) -> Result<FOut> {
//         type_mismatch_err(ds_field.type_num)
//     }
// }
// impl<Head, Tail, F, FOut> TypeNumMapMut<F, FOut>
//     for DTypeCons<Head, Tail>
//     where Head: 'static + DataType + Default,
//           F: FuncMut<Head, FOut>,
//           DTypeCons<Head, Tail>: DTypeList,
//           Tail: TypeNumMapMut<F, FOut>,
// {
//     fn map_mut(&mut self, ds_field: &DsField, mut f: F)
//         -> Result<FOut>
//     {
//         if DTypeCons::<Head, Tail>::is_type_num(ds_field.type_num) {
//             Ok(f.call(self.head.get_mut(ds_field.td_index).unwrap(), ds_field))
//         } else {
//             self.tail.map_mut(ds_field, f)
//         }
//     }
// }


// pub trait TypeNumMapInto<F, FOut>
// {
//     fn map_into(&self,
//         src_ds_field: &DsField,
//         target: &mut Self,
//         target_ds_field: &DsField,
//         f: F
//     )
//         -> Result<FOut>;
//         // where F: FnAll<'b, Self::DType>;
// }
// impl<F, FOut> TypeNumMapInto<F, FOut> for DTypeNil
// {
//     fn map_into(
//         &self, src_ds_field: &DsField, _: &mut DTypeNil, _: &DsField, _: F
//     )
//         -> Result<FOut>
//     {
//         type_mismatch_err(src_ds_field.type_num)
//     }
// }
// impl<Head, Tail, F, FOut> TypeNumMapInto<F, FOut>
//     for DTypeCons<Head, Tail>
//     where Head: 'static + DataType + Default,
//           F: IntoFunc<Head, FOut>,
//           DTypeCons<Head, Tail>: DTypeList,
//           Tail: TypeNumMapInto<F, FOut>,
// {
//     // type DType = Head;
//     // type Output = F::Output;

//     fn map_into(
//         &self,
//         src_ds_field: &DsField,
//         target: &mut DTypeCons<Head, Tail>,
//         target_ds_field: &DsField,
//         mut f: F
//     )
//         -> Result<FOut>
//         // where F: FnAll<'a, Self::DType>
//     {
//         debug_assert_eq!(src_ds_field.type_num, target_ds_field.type_num);
//         if DTypeCons::<Head, Tail>::is_type_num(src_ds_field.type_num) {
//             Ok(f.call(self.head.get(src_ds_field.td_index).unwrap(), //ds_field,
//                 target.head.get_mut(target_ds_field.td_index).unwrap()))
//             // Ok(f.call(&self.head, ds_field, &mut target.head))
//         } else {
//             self.tail.map_into(src_ds_field, &mut target.tail, target_ds_field, f)
//         }
//     }
// }


// pub trait GetDtValue<'a, DTypes> where DTypes: AssociatedValue<'a> {
//     fn get_dt_value(&self) -> DTypes::DtValue;
// }
// impl<'a, DTypes, Head, Tail> GetDtValue<'a, DTypes> for DTypeCons<Head, Tail>
//     where DTypes: AssociatedValue<'a>
// {
//     fn get_dt_value(&self) -> DTypes::DtValue {
//         DTypes::DtValue::add(&self.head)
//     }
// }

// pub trait IntoFunc<T, Out>
// {
//     fn call(
//         &mut self,
//         left: &dyn DataIndex<DType=T>,
//         // ds_field: &DsField,
//         right: &mut dyn DataIndexMut<DType=T>
//     )
//         -> Out;
// }

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

pub trait RefAssocTypes<'a> {
    // type RecordValues;
    type PartialRecordValues: Debug + Clone;
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

// pub struct OptionalFnNone;
// pub struct OptionalFn<F, Out, Backup> {
//     f: F,
//     _out: PhantomData<Out>,
//     backup: Backup,
// }

// pub struct ImplFlagTrue;
// pub struct ImplFlagNot<T> { _marker: PhantomData<T> }

// pub trait FuncOpt<T, Out, ImplFlag> {
//     fn call_opt(&mut self, data: &dyn DataIndex<DType=T>) -> Option<Out>;
// }
// impl<T, F, Out, Backup>
//     FuncOpt<T, Out, ImplFlagTrue>
//     for OptionalFn<F, Out, Backup>
//     where F: Func<T, Out>
// {
//     fn call_opt(&mut self, data: &dyn DataIndex<DType=T>) -> Option<Out> {
//         println!("call_opt OptionalFn ImplFlagTrue");
//         Some(self.f.call(data))
//     }
// }
// impl<T, F, Out, Backup, Flag>
//     FuncOpt<T, Out, ImplFlagNot<Flag>>
//     for OptionalFn<F, Out, Backup>
//     where Backup: FuncOpt<T, Out, Flag>
// {
//     fn call_opt(&mut self, data: &dyn DataIndex<DType=T>) -> Option<Out> {
//         println!("call_opt OptionalFn ImplFlagNot");
//         self.backup.call_opt(data)
//     }
// }
// impl<T, Out>
//     FuncOpt<T, Out, ImplFlagTrue>
//     for OptionalFnNone
// {
//     fn call_opt(&mut self, _data: &dyn DataIndex<DType=T>) -> Option<Out> {
//         println!("call_opt OptionalFnNone");
//         None
//     }
// }



// pub trait ImplFlag { fn implemented() -> bool; }
// pub struct ImplFlagFalse;
// impl ImplFlag for ImplFlagFalse { fn implemented() -> bool { false } }
// pub struct ImplFlagTrue;
// impl ImplFlag for ImplFlagTrue { fn implemented() -> bool { true } }
// pub trait FuncOpt<T, Out, Flag: ImplFlag = ImplFlagTrue>
// {
//     fn call(
//         &mut self,
//         type_data: &dyn DataIndex<DType=T>,
//     )
//         -> Out;
//     fn call_opt(
//         &mut self,
//         type_data: &dyn DataIndex<DType=T>,
//     )
//         -> Option<Out>
//     {
//         if Flag::implemented() {
//             Some(self.call(type_data))
//         } else {
//             None
//         }
//     }
// }
// impl<T, Out, U> FuncOpt<T, Out, ImplFlagFalse> for U {
//     fn call(
//         &mut self,
//         _type_data: &dyn DataIndex<DType=T>,
//     )
//         -> Out
//     {
//         unreachable![]
//     }

// }
// impl<T, Out, U> FuncOpt<T, Out> for U where U: Func<T, Out> {
//     fn call(
//         &mut self,
//         type_data: &dyn DataIndex<DType=T>,
//     )
//         -> Out
//     {
//         self.call(type_data)
//     }
// }


// pub trait MapToDtValue<'a, 'b>  {
//     type ValueType;
//     fn map<F>(&'a self, f: F) where F: FnAll<'a, 'b, Self::ValueType>;
// }

// pub enum DtValue<'a, Head: 'a, Tail> {
//     InHead(&'a TypeData<Head>),
//     InTail(Tail)
// }
// impl<'a, Head, Tail> DtValue<'a, Head, Tail> {
//     pub fn add<P, Idx>(payload: &'a TypeData<P>) -> Self
//         where Self: DtValueAdder<'a, P, Idx>
//     {
//         DtValueAdder::add(payload)
//     }
//     pub fn get<P, Idx>(&'a self) -> Option<&'a TypeData<P>>
//         where Self: DtValueGetter<'a, P, Idx>
//     {
//         DtValueGetter::get(self)
//     }
// }
// impl<'a, Head, Tail> MapToDtValue<'a> for DtValue<'a, Head, Tail>
//     where Tail: MapToDtValue<'a>,
// {
//     type ValueType = Head;

//     fn map<F>(&'a self, f: F) where F: FnAll<'a, Head> {
//         match *self {
//             DtValue::InHead(&ref head) => f.call(head),
//             DtValue::InTail(ref tail) => tail.map(f)
//         }
//     }
// }

// pub trait DtValueAdder<'a, P, Idx> {
//     fn add(payload: &'a TypeData<P>) -> Self;
// }
// impl<'a, P, Tail> DtValueAdder<'a, P, Idx0> for DtValue<'a, P, Tail> {
//     fn add(payload: &'a TypeData<P>) -> Self {
//         DtValue::InHead(payload)
//     }
// }
// impl<'a, Head, P, Tail, TailIndex> DtValueAdder<'a, P, Idx1<TailIndex>> for DtValue<'a, Head , Tail>
//     where Tail: DtValueAdder<'a, P, TailIndex>
// {
//     fn add(payload: &'a TypeData<P>) -> Self {
//         DtValue::InTail(DtValueAdder::<P, TailIndex>::add(payload))
//     }
// }

// pub trait DtValueGetter<'a, P, Idx> {
//     fn get(&'a self) -> Option<&'a TypeData<P>>;
// }
// impl<'a, P, Tail> DtValueGetter<'a, P, Idx0> for DtValue<'a, P, Tail> {
//     fn get(&'a self) -> Option<&'a TypeData<P>> {
//         match *self {
//             DtValue::InHead(ref payload) => Some(payload),
//             _ => None
//         }
//     }
// }
// impl<'a, Head, PayloadFromTail, Tail, TailIndex> DtValueGetter<'a, PayloadFromTail, Idx1<TailIndex>>
//     for DtValue<'a, Head, Tail>
//     where Tail: DtValueGetter<'a, PayloadFromTail, TailIndex>
// {
//     fn get(&'a self) -> Option<&'a TypeData<PayloadFromTail>> {
//         match *self {
//             DtValue::InTail(ref tail) => tail.get(),
//             _ => None
//         }
//     }
// }

#[derive(Debug, Clone, Hash)]
pub struct HashableFieldNil;
#[derive(Debug, Clone)]
pub struct HashableFieldCons<H, T>
    where T: HashListMember,
{
    ident: FieldIdent,
    head: PhantomData<H>,
    tail: T,
}
impl<H, T> HashableFieldCons<H, T>
    where H: Hash,
          T: HashListMember
{
    pub fn new<I>(ident: I, tail: T) -> HashableFieldCons<H, T> where I: Into<FieldIdent> {
        HashableFieldCons {
            ident: ident.into(),
            head: PhantomData,
            tail: tail,
        }
    }
}

pub trait HashListMember {
    type DType: Hash;
    type Tail: HashListMember;
}
impl HashListMember for HashableFieldNil {
    type DType = HashableFieldNil;
    type Tail = HashableFieldNil;
}
impl<H, T> HashListMember for HashableFieldCons<H, T> where H: Hash, T: HashListMember {
    type DType = H;
    type Tail = T;
}

// const NIL_IDENT: FieldIdent = FieldIdent::Name("NIL_FIELD".to_string());
// #[derive(Debug, Clone)]
// pub struct FieldNil;
// #[derive(Debug, Clone)]
// pub struct FieldCons<DTypes, H, T>
//     where DTypes: DTypeList,
//           DTypes::Storage: TypeSelector<DTypes, H>,
//           H: DataType<DTypes>
// {
//     ident: FieldIdent,
//     head: PhantomData<H>,
//     tail: T,
//     _marker: PhantomData<DTypes>,
// }
// impl<DTypes, H, T> FieldCons<DTypes, H, T>
//     where DTypes: DTypeList,
//           DTypes::Storage: TypeSelector<DTypes, H>,
//           H: DataType<DTypes>
//           {
//     pub fn new<I>(ident: I, tail: T) -> FieldCons<DTypes, H, T> where I: Into<FieldIdent>{
//         FieldCons {
//             ident: ident.into(),
//             head: PhantomData,
//             tail: tail,
//             _marker: PhantomData
//         }
//     }
// }

// pub trait FieldList<DTypes>
//     where DTypes: DTypeList
// {
//     type Head;
//     type Tail: FieldList<DTypes>;
//     fn ident(&self) -> &FieldIdent;
// }
// impl<DTypes> FieldList<DTypes> for FieldNil
//     where DTypes: DTypeList
// {
//     type Head = FieldNil;
//     type Tail = FieldNil;

//     fn ident(&self) -> &FieldIdent { &NIL_IDENT }
// }
// impl<DTypes, H, T> FieldList<DTypes> for FieldCons<DTypes, H, T>
//     where DTypes: DTypeList,
//           DTypes::Storage: TypeSelector<DTypes, H>,
//           H: DataType<DTypes>,
//           T: FieldList<DTypes>
// {
//     type Head = H;
//     type Tail = T;

//     fn ident(&self) -> &FieldIdent { &self.ident }
// }

// #[derive(Debug, Clone)]
// pub struct Hashable<DTypes, F>
//     where DTypes: DTypeList,
//           F: FieldList<DTypes>,
//           F::Head: DataType<DTypes> + Hash,
//           DTypes::Storage: TypeSelector<DTypes, F::Head>,
// {
//     pub fields: F,
//     _marker: PhantomData<DTypes>,
// }

// impl<DTypes, F> FieldList<DTypes> for Hashable<DTypes, F>
//     where DTypes: DTypeList,
//           F: FieldList<DTypes>,
//           F::Head: DataType<DTypes> + Hash,
//           DTypes::Storage: TypeSelector<DTypes, F::Head>,
// {
//     type Head = F::Head;
//     type Tail = F::Tail;

//     fn ident(&self) -> &FieldIdent { self.fields.ident() }
// }

// pub trait HashableFieldList {}
// impl<DTypes, F> HashableFieldList for Hashable<DTypes, F>
//     where DTypes: DTypeList,
//           F: FieldList<DTypes>,
//           DTypes::Storage: TypeSelector<DTypes, F::Head>,
//           F::Head: DataType<DTypes> + Hash {}

// pub trait HashAll<Index> {}
// impl HashAll<Idx0> for FieldNil {}
// impl<I, H, T> HashAll<Idx1<I>> for FieldCons<H, T>
//     where H: Hash,
//           T: HashAll<I> {}

// pub trait TypeSelectorAll<DTypes, FList, Index>
//     where DTypes: DTypeList,
//           FList: FieldList,
//           FList::Head: DataType<DTypes>,
//           DTypes::Storage: TypeSelector<DTypes, FList::Head>
//                            + TypeSelectorAll<DTypes, FList::Tail> {}
// impl<DTypes> TypeSelectorAll<DTypes, FieldNil> for FieldNil
//     where DTypes: DTypeList {}
// impl<DTypes, F, S> TypeSelectorAll<DTypes, F> for S
//     where DTypes: DTypeList,
//           F: FieldList,
//           F::Head: DataType<DTypes>,
//           S: TypeSelector<DTypes, F::Head> + TypeSelectorAll<DTypes, F::Tail> {}

// pub trait TypeSelectorAll<DTypes, Index> {}
// impl<DTypes> TypeSelectorAll<DTypes, Idx0> for FieldNil {}
// impl<DTypes, I, H, T> TypeSelectorAll<DTypes, Idx1<I>> for FieldCons<H, T>
//     where DTypes: DTypeList,
//           DTypes::Storage: TypeSelector<DTypes, H>,
//           H: DataType<DTypes>,
//           T: TypeSelectorAll<DTypes, I> {}

// pub trait HashFieldsI<I>: FieldList
//     where Self::Head: Hash,
//           Self::Tail: HashFieldsI<Idx1<I>> {}

// impl<I> HashFieldsI<I> for FieldNil {}
// impl<I, H, T> HashFieldsI<I> for FieldCons<H, T>
//     where H: Hash,
//           T: HashFieldsI<Idx1<I>> {}


#[macro_export]
macro_rules! hashable_fields {
    (@impl ) => {
        $crate::data_types::HashableFieldNil
    };
    (@impl $id0:expr => $ty0:ty, $($id:expr => $ty:ty,)*) => {
        $crate::data_types::HashableFieldCons::<$ty0, _>::new(
            $id0,
            hashable_fields![@impl $($id => $ty,)*]
        )
    };
    ($($id:expr => $ty:ty),*$(,)*) => {{
        hashable_fields![@impl $($id => $ty,)*]
    }};
}

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

// #[macro_export]
// macro_rules! RecordValueTypes {
//     // base case: ending in nil
//     () => { $crate::data_types::ValueNil };
//     // append another set of data types
//     (++ $others:ty) => { $others };
//     // end-comma elision
//     ($dtype:ty) => { RecordValueTypes![$dtype,] };
//     // setting up ValueCons type and recursion
//     ($dtype:ty, $($tok:tt)*) => {
//         $crate::data_types::ValueCons<'a, Types, $dtype, RecordValueTypes![$($tok)*]>
//     };
// }
#[macro_export]
macro_rules! PartialRecordValueTypes {
    // base case: ending in nil
    () => { $crate::data_types::PartialValueNil };
    // append another set of data types
    (++ $others:ty) => { $others };
    // end-comma elision
    ($dtype:ty) => { PartialRecordValueTypes![$dtype,] };
    // setting up PartialValueCons type and recursion
    ($dtype:ty, $($tok:tt)*) => {
        $crate::data_types::PartialValueCons<
            'a, Types, $dtype,
            PartialRecordValueTypes![$($tok)*]
        >
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

// macro_rules! count_tts {
//     (@replace_expr($_t:tt $sub:expr)) => { $sub };
//     ($($toks:tt)*) => {<[()]>::len(&[$(count_tts![@replace_expr($toks ())]),*])};
// }

// macro_rules! FlagNum {
//     ($($plus:tt)*) => {
//         mashup! {
//             flag["Flag"] = Flag count_tts![$($plus)*];
//         }
//         flag! { "Flag" }
//     }
// }


// #[macro_export]
// macro_rules! Flags {
//     // base case: ending in nil
//     (@cons()) => { $crate::data_types::FlagNil };
//     // setting up FlagCons type and recursion
//     (@cons(+$($plus:tt)*)) => {
//         $crate::data_types::FlagCons<FlagNum![$($plus)*], Flags![@cons($($plus)*)]>
//     };

//     (@list_impl()) => {};
//     (@list_impl(+$($plus:tt)*)) => {
//         , FlagNum![$($plus)*] Flags![@list_impl($($plus)*)]
//     };
//     (@list(+$($plus:tt)*)) => {
//         FlagNum![$($plus)*] Flags![@list_impl($($plus)*)]
//     }
// }

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

    // // match one of the '+' marks, adding a '.tail' to move on to the next element of the DTypeCons
    // (@TailRecurse($self:ty, + $($plus:tt)*)) => {
    //     <data_types![@TailRecurse($self, $($plus)*)]>::Tail
    // };
    // // end case: use `self`
    // (@TailRecurse($self:ty,)) => { $self };

    // /* serialize implementation */
    // (@impl_serialize(init$($plus:tt)*) $dtype:ty, $($tok:tt)*) => {
    //     struct FieldInData
    //     impl Serialize for Types
    //     {
    //         fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error> where S: Serializer {

    //         }
    //     }
    // }

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
                // use $crate::data_types::TypeNumMappable;
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
                // x if x == Self::TYPE_NUM - {count_tts![$($plus)*]} => {
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
                // use $crate::data_types::TypeNumMappable;
                // use typenum::{U0, Unsigned, Sub1, Add1};
                data_types![@impl_type_num_map(init)(self, f, locator)($($tok)*) -> (<U0>)()]
            }
        }
        impl<F, FOut> $crate::data_types::MapExt<Types, F, FOut> for Storage
            where F: FuncAllTypesExt<FOut>
        {
            fn map_ext<L>(&self, locator: &L, mut f: F)
                -> $crate::error::Result<FOut>
                where L: $crate::data_types::FieldLocator<Types>
            {
                // use $crate::data_types::TypeNumMappable;
                // use typenum::{U0, Unsigned, Sub1, Add1};
                data_types![@impl_type_num_map_ext(init)(self, f, locator)($($tok)*) -> (<U0>)()]
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
                // data_types![@impl_type_num_map_partial(init)(self, f, locator)($($tok)*) -> ()()]
            }
        }
    };
    (@impl_type_num_tmap($($dtype:ty,)*)) => {$(
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

    // (@impl_type_num_map_opt(init)($($tok:tt)*) -> ()) => {

    //     //TODONEXT need to have # of flags equal to # of types (diff flag for each type)
    //     // impl<F, FOut, Flags> $crate::data_types::TypeNumMapOpt<F, FOut, Flags> for Types
    //     //     // where F: FuncAllTypesOpt<FOut, Flag>
    //     //     where $crate::data_types::OptionalFn<F, FOut, $crate::data_types::OptionalFnNone>:
    //     //         FuncAllTypesOpt<FOut, Flags>
    //     // {
    //     //     fn map_opt(&self, ds_field: &$crate::store::DsField, f: F)
    //     //         -> $crate::error::Result<Option<FOut>>
    //     //     {
    //     //         use $crate::data_types::IsTypeNum;
    //     //         data_types![@impl_type_num_map_opt(init)(self, f, ds_field)($($tok)*) -> ()]
    //     //     }
    //     // }

    //     //Flags![@list(+++)]
    //     impl<F, FOut, Flag1> $crate::data_types::TypeNumOpt<F, FOut,
    //             Flags![@cons(+++)]>
    //         for Types
    //         where $crate::data_types::OptionalFn<F, FOut, $crate::data_types::OptionalFnNone>:
    //             FuncOpt<Type1, FOut, Flag1> + FuncOpt<Type2, FOut, Flag2>
    //                 + FuncOpt<Type3, FOut, Flag3>
    //     {
    //         fn map_opt(&self, ds_field: &$crate::store::DsField, f: F)
    //             -> $crate::error::Result<Option<FOut>>
    //         {
    //             use $crate::data_types::IsTypeNum;
    //             data_types![@impl_type_num_map_opt(init)(self, f, ds_field)($($tok)*) -> ()]
    //         }
    //     }
    // };
    (@impl_type_num_map
        (init$($plus:tt)*)
        ($self:ident, $f:ident, $locator:ident)
        ($dtype:ty, $($tok:tt)*)
        ->
        ($($add:tt)*)
        ($($out:tt)*)
    ) => {
        data_types![@impl_type_num_map
            (init+$($plus)*)
            ($self, $f, $locator)
            ($($tok)*)
            ->
            (<Add1 $($add)*>)
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
        ($($add:tt)*)
        ($($out:tt)*)
    ) => {
        data_types![@impl_type_num_map_ext
            (init+$($plus)*)
            ($self, $f, $locator)
            ($($tok)*)
            ->
            (<Add1 $($add)*>)
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
    // (@impl_type_num_map_partial
    //     (init$($plus:tt)*)
    //     ($self:ident, $f:ident, $locator:ident)
    //     ($dtype:ty, $($tok:tt)*)
    //     ->
    //     ($($add:tt)*)
    //     ($($out:tt)*)
    // ) => {
    //     data_types![@impl_type_num_map_partial
    //         (init+$($plus)*)
    //         ($self, $f, $locator)
    //         ($($tok)*)
    //         ->
    //         (<Add1 $($add)*>)
    //         (
    //             $($out)*
    //             <$dtype as $crate::data_types::GetDType<Types>>::DTYPE =>
    //                 Ok($f.call(
    //                     data_types![@tail_recurse($self, $($plus)*)].head.get($locator.td_idx())
    //                         .unwrap(),
    //                     &$locator
    //                 )),
    //         )
    //         ];
    // };
    // (@impl_type_num_map_opt
    //     (init$($plus:tt)*)
    //     ($self:ident, $f:ident, $ds_field:ident)
    //     ($dtype:ty, $($tok:tt)*)
    //     ->
    //     ($($out:tt)*)
    // ) => {
    //     data_types![@impl_type_num_map_opt(init+$($plus)*)($self, $f, $ds_field)($($tok)*) -> (
    //         $($out)*
    //         x if x == Self::TYPE_NUM - {count_tts![$($plus)*]} => {
    //             // Ok($f.call_opt(
    //             //     data_types![@tail_recurse($self, $($plus)*)].head.get($ds_field.td_index)
    //             //         .unwrap(),
    //             // )),
    //                 use $crate::data_types::FuncOpt;
    //                 println!("in map_opt {}", x);
    //                 Ok($crate::data_types::OptionalFn {
    //                     f: $f,
    //                     _out: ::std::marker::PhantomData,
    //                     backup: $crate::data_types::OptionalFnNone
    //                 }.call_opt(
    //                     data_types![@tail_recurse($self, $($plus)*)].head.get($ds_field.td_index)
    //                         .unwrap(),
    //                 ))
    //             },
    //         )];
    // };
    (@impl_type_num_map
        (init$($plus:tt)*)
        ($self:ident, $f:ident, $locator:ident)
        ()
        ->
        ($($add:tt)*)
        ($($out:tt)*)
    ) => {
        // match Self::TYPE_NUM - $locator.type_num {
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
        ($($add:tt)*)
        ($($out:tt)*)
    ) => {
        // match Self::TYPE_NUM - $locator.type_num {
        #[allow(unreachable_patterns)]
        match $locator.ty() {
            $($out)*
            _ => $crate::data_types::type_mismatch_err::<Types, _>($locator.ty())
        }
    };
    // (@impl_type_num_map_opt
    //     (init$($plus:tt)*)
    //     ($self:ident, $f:ident, $ds_field:ident)
    //     ()
    //     ->
    //     ($($out:tt)*)
    // ) => {
    //     match $ds_field.type_num {
    //         $($out)*
    //         _ => $crate::data_types::type_mismatch_err($ds_field.type_num)
    //     }
    // };

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
                // use $crate::data_types::TypeNumMappable;
                // Self::TYPE_NUM - {count_tts![$($plus)*]}
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

    /* GetFieldData trait implementation */

    // // normal step: implement AddVec for type at this point of DTpeCons and recurse
    // (@impl_get_field_data(init$($plus:tt)*) $dtype:ty, $($tok:tt)*) => {
    //     impl Types
    //     {
    //         pub(crate) fn field_data<'a>(&'a self, td_idx: usize)
    //             -> $crate::data_types::FieldData<'a>
    //         {
    //             self.head.
    //         }
    //     }

    //     impl $crate::data_types::GetFieldData<$dtype> for Types
    //     {
    //         fn add_vec(&mut self) -> $crate::error::Result<usize> {
    //             let type_data = &mut data_types![@tail_recurse(self, $($plus)*)].head;
    //             let td_idx = type_data.len();
    //             type_data.push($crate::field::FieldData::new());
    //             Ok(td_idx)
    //         }
    //     }
    // };
    // // end case: noop
    // (@impl_get_field_data(init$($plus:tt)*)) => {};



    /* SelectAllType trait declaration */

    // initial step -- no existing output: add initial output
    (@type_selector_bounds() $dtype:ty, $($tok:tt)*) => {
        data_types![@type_selector_bounds
            ($crate::data_types::TypeSelector<Types, $dtype>)
            $($tok)*
        ];
    };
    // non-initial step: continue adding TypeSelector outputs
    (@type_selector_bounds($($output:tt)*) $dtype:ty, $($tok:tt)*) => {
        data_types![@type_selector_bounds
            ($($output)* + $crate::data_types::TypeSelector<Types, $dtype>)
            $($tok)*
        ];
    };
    // final step: actually declare SelectAllType trait and implement
    (@type_selector_bounds($($output:tt)*)) => {
        pub trait SelectAllType: $($output)* {}
        impl<T> SelectAllType for T where T: $($output)* {}
    };

    /* FuncAllTypes trait declaration */

    // initial step -- no existing output: add initial output
    (@func_all_types()() $dtype:ty, $($tok:tt)*) => {
        data_types![
            @func_all_types
                ($crate::data_types::Func<Types, $dtype, Output=FOut>)
                ($crate::data_types::FuncExt<Types, $dtype, Output=FOut>)
                // ($crate::data_types::FuncOpt<$dtype, FOut, Flag>)
                // ($crate::data_types::FuncOpt<$dtype, FOut,
                //     <data_types!(@TailRecurse(Flags,$($plus)*))>::Head>)
            $($tok)*
        ];
    };
    // non-initial step: continue adding Func traits
    (@func_all_types
        ($($output:tt)*)
        ($($ext_output:tt)*)
        // ($($opt_output:tt)*)
        $dtype:ty,
        $($tok:tt)*
    ) => {
        data_types![
            @func_all_types
                ($($output)* + $crate::data_types::Func<Types, $dtype, Output=FOut>)
                ($($ext_output)* + $crate::data_types::FuncExt<Types, $dtype, Output=FOut>)
                // ($($opt_output)* + $crate::data_types::FuncOpt<$dtype, FOut, Flag>)
                // ($($opt_output)* + $crate::data_types::FuncOpt<$dtype, FOut,
                //     <data_types!(@TailRecurse(Flags,$($plus)*))>::Head>)
                $($tok)*
        ];
    };
    // final step: actually declare FuncAllTypes trait and implement
    (@func_all_types
        ($($output:tt)*)
        ($($ext_output:tt)*)
        // ($($opt_output:tt)*)
    ) => {
        pub trait FuncAllTypes<FOut>: $($output)* {}
        impl<F, FOut> FuncAllTypes<FOut> for F where F: $($output)* {}
        pub trait FuncAllTypesExt<FOut>: $($ext_output)* {}
        impl<F, FOut> FuncAllTypesExt<FOut> for F where F: $($ext_output)* {}
        // pub trait FuncAllTypesOpt<FOut, Flags>: $($opt_output)* {}
        // // impl<F, FOut, Flags: $crate::data_types::FlagList> FuncAllTypesOpt<FOut, Flags>
        // //     for F where F: $($opt_output)* {}
        // impl<F, FOut, Flag1, Flag2, Flag3> FuncAllTypesOpt<FOut,
        //         FlagCons<Flag1, FlagCons<Flag2, FlagCons<Flag3, FlagNil>>>>
        //     for F where F: FuncOpt<T1, FOut, Flag1>
        //            + FuncOpt<T2, FOut, Flag2>
        //            + FuncOpt<T3, FOut, Flag3> {}

        // pub trait FuncAllTypesOpt<FOut, Flags: FlagList>: FuncOpt<T1, FOut, Flags::Head>
        //     + FuncOpt<T2, FOut, Flags::Tail::Head>
        //     + ...
        //     + FuncOpt<Tend, FOut, Flags::Tail::...::Tail::Head>
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
        // pub type RecordValues<'a> = RecordValueTypes![$($dtype),*];
        pub type PartialRecordValues<'a> = PartialRecordValueTypes![$($dtype),*];
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

        impl<'a> $crate::data_types::RefAssocTypes<'a> for Types {
            // type RecordValues = RecordValues<'a>;
            type PartialRecordValues = PartialRecordValues<'a>;
        }

        data_types![@impl_type_selector(init) $($dtype,)*];

        data_types![@type_selector_bounds() $($dtype,)*];
        data_types![@func_all_types()() $($dtype,)*];

        data_types![@impl_add_vec(init) $($dtype,)*];

        data_types![@impl_type_num_map(init)($($dtype,)*) -> ()];
        data_types![@impl_type_num_tmap($($dtype,)*)];
        // data_types![@impl_type_num_map_opt(init)($($dtype,)*) -> ()];
        data_types![@impl_type_num_ser(init)($($dtype,)*) -> ()];

        $(
            impl $crate::data_types::DataType<Types> for $dtype {}
        )*

        // data_types![@impl_get_field_data(init) $($dtype,)*];
    };

    /* Implementation bundle for stuff that cannot be coerced into types */
    // normal ty-based implementation
    (@impl_tt_based $($tok:tt)*) => {
        data_types![@impl_dtenums $($tok)*];

    };

    /* Main entry point */
    ($($tok:tt)*) => {
        data_types![@impl_ty_based $($tok)*];

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
