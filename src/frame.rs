/*!
Structs and implementation for Frame-level data structure. A `DataFrame` is a reference to an
underlying data store, along with record-based filtering and sorting details.
*/
use std::fmt::Debug;
use std::sync::Arc;
use std::marker::PhantomData;
use serde::{Serialize, Serializer};

use store::{DataStore, StoreRecord};
use data_types::*;
use field::{FieldIdent};
use access::{OwnedOrRef, DataIndex, DataIndexMut};
use select::{SelectField, Field};
// use apply::mapfn::*;
use apply::matches::DataFilter;
use apply::sort::SortOrderFunc;
use error;
use field::{Value};

/// A data frame. A `DataStore` reference along with record-based filtering and sorting details.
#[derive(Debug, Clone)]
pub struct DataFrame<DTypes>
    where DTypes: DTypeList
{
    pub(crate) permutation: Option<Vec<usize>>,
    pub(crate) store: Arc<DataStore<DTypes>>,
}
impl<DTypes> DataFrame<DTypes>
    where DTypes: DTypeList
{
    /// Number of rows that pass the filter in this frame.
    pub fn nrows(&self) -> usize
        where DTypes::Storage: MaxLen<DTypes>
    {
        self.len()
    }
    #[cfg(test)]
    pub(crate) fn store_ref_count(&self) -> usize {
        Arc::strong_count(&self.store)
    }
    /// Get the field type of a particular field in the underlying `DataStore`.
    pub fn get_field_type(&self, ident: &FieldIdent) -> Option<DTypes::DType> {
        self.store.get_field_type(ident)
    }
    pub(crate) fn has_same_store(&self, other: &DataFrame<DTypes>) -> bool {
        Arc::ptr_eq(&self.store, &other.store)
    }
    /// Returns `true` if this `DataFrame` contains this field.
    pub fn has_field(&self, s: &FieldIdent) -> bool {
        self.store.has_field(s)
    }
    pub(crate) fn update_permutation(&mut self, new_permutation: &Vec<usize>) {
        // check if we already have a permutation
        self.permutation = match self.permutation {
            Some(ref prev_perm) => {
                // we already have a permutation, map the filter indices through it
                Some(new_permutation.iter().map(|&new_idx| prev_perm[new_idx]).collect())
            },
            None => Some(new_permutation.clone())
        };
    }

    pub fn map<F, FOut>(&self, ident: &FieldIdent, f: F)
        -> error::Result<FOut>
        where DTypes::Storage: FramedMap<DTypes, F, FOut>,
    {
        self.store.map(ident, FramedFunc::new(self, f))
    }
    pub fn tmap<T, F>(&self, ident: &FieldIdent, f: F)
        -> error::Result<F::Output>
        where F: Func<DTypes, T>,
              T: DataType<DTypes>,
              DTypes::Storage: MaxLen<DTypes> + FramedTMap<DTypes, T, F>,
    {
        self.store.tmap(ident, FramedFunc::new(self, f))
    }
    pub fn map_ext<F, FOut>(&self, ident: &FieldIdent, f: F)
        -> error::Result<FOut>
        where DTypes::Storage: FramedMapExt<DTypes, F, FOut>,
    {
        self.store.map_ext(ident, FramedFunc::new(self, f))
    }
    pub fn map_partial<F>(&self, ident: &FieldIdent, f: F)
        -> error::Result<Option<F::Output>>
        where DTypes::Storage: MapPartial<DTypes, F> + MaxLen<DTypes>,
              F: FuncPartial<DTypes>
    {
        self.store.map_partial(ident, self, f)
    }
    // pub fn map_opt<'a, F, FOut, Flag>(&'a self, ident: &FieldIdent, f: F)
    //     -> error::Result<Option<FOut>>
    //     where DTypes::Storage: TypeNumMapOpt<FramedFunc<'a, DTypes, F>, FOut, Flag>,
    // {
    //     self.store.map_opt(ident, FramedFunc::new(self, f))
    // }
    // pub fn copy_into(
    //     &self,
    //     ident: &FieldIdent,
    //     idx: usize,
    //     target_ds: &mut DataStore<DTypes>,
    //     target_ident: &FieldIdent,
    // )
    //     -> error::Result<()>
    //     where DTypes: TypeNumMapInto<CopyInto, ()> + TypeNumAddVec
    //     // where DTypes: MapForTypeNum<DTypes, CopyInto<'a, 'b, DTypes>>
    // {
    //     self.store.copy_into(ident, self.map_index(idx), target_ds, target_ident)
    // }

    pub fn sort_by<'a>(&mut self, ident: &FieldIdent) -> error::Result<Vec<usize>>
        where DTypes::Storage: FramedMap<DTypes, SortOrderFunc, Vec<usize>>
    {
        let sort_order = self.sort_order_by(ident)?;
        self.update_permutation(&sort_order);
        Ok(sort_order)
    }

    fn sort_order_by(&self, ident: &FieldIdent) -> error::Result<Vec<usize>>
        where DTypes::Storage: FramedMap<DTypes, SortOrderFunc, Vec<usize>>,
    {
        self.map(ident, SortOrderFunc)
    }
}

impl<DTypes> DataFrame<DTypes>
    where DTypes: DTypeList,
          Self: Reindexer<DTypes>
{
    // pub fn record<'a>(&'a self, idx: usize)
    //     -> Record<'a, DTypes>
    //     where DTypes: AssocTypes + RefAssocTypes<'a>,
    //           DTypes::RecordValues: RetrieveValues<'a, DTypes::Storage>,
    // {
    //     self.store.record(self.map_index(idx))
    // }

    pub fn store_record<'a, I, Iter, IntoIter>(&'a self, idx: usize, idents: IntoIter)
       -> StoreRecord<'a, DTypes>
        where DTypes: AssocTypes + RefAssocTypes<'a>,
              DTypes::PartialRecordValues: RetrieveValuesPartial<'a, DTypes, DTypes::Storage>,
              I: Into<FieldIdent>,
              Iter: Iterator<Item=I>,
              IntoIter: IntoIterator<Item=I, IntoIter=Iter>,
    {
        self.store.store_record(self.map_index(idx), idents)
    }
}

pub trait FramedMap<DTypes, F, FOut>:
    for<'a> Map<DTypes, FramedFunc<'a, DTypes, F>, FOut>
    where DTypes: AssocTypes
{}
impl<DTypes, F, FOut, T> FramedMap<DTypes, F, FOut> for T
    where T: for<'a> Map<DTypes, FramedFunc<'a, DTypes, F>, FOut>,
          DTypes: AssocTypes
{}

pub trait FramedTMap<DTypes, T, F>:
    for<'a> TMap<DTypes, T, FramedFunc<'a, DTypes, F>>
    where DTypes: AssocTypes,
          T: DataType<DTypes>
{}
impl<DTypes, T, F, U> FramedTMap<DTypes, T, F> for U
    where U: for<'a> TMap<DTypes, T, FramedFunc<'a, DTypes, F>>,
          DTypes: AssocTypes,
          T: DataType<DTypes>
{}

pub trait FramedMapExt<DTypes, F, FOut>:
    for<'a> MapExt<DTypes, FramedFunc<'a, DTypes, F>, FOut>
    where DTypes: AssocTypes
{}
impl<DTypes, F, FOut, T> FramedMapExt<DTypes, F, FOut> for T
    where T: for<'a> MapExt<DTypes, FramedFunc<'a, DTypes, F>, FOut>,
          DTypes: AssocTypes
{}

pub trait Reindexer<DTypes: DTypeList>: Debug {
    fn len(&self) -> usize;
    fn map_index(&self, requested: usize) -> usize;
    fn reindex<'a, 'b, DI>(&'a self, data_index: &'b DI) -> Reindexed<'a,'b, Self, DI>
        where DI: 'b + DataIndex<DTypes>,
              Self: Sized
    {
        Reindexed {
            orig: data_index,
            reindexer: self
        }
    }
}

impl<DTypes> Reindexer<DTypes> for DataFrame<DTypes>
    where DTypes: DTypeList,
          DTypes::Storage: MaxLen<DTypes>
{
    fn len(&self) -> usize
    {
        match self.permutation {
            Some(ref perm) => perm.len(),
            None => self.store.nrows()
        }
    }

    fn map_index(&self, requested: usize) -> usize {
        match self.permutation {
            Some(ref perm) => perm[requested],
            None => requested
        }
    }
}
#[derive(Debug)]
pub struct Reindexed<'a, 'b, R: 'a, DI: 'b>
{
    reindexer: &'a R,
    orig: &'b DI,
}
impl<'a, 'b, DI, R, DTypes> DataIndex<DTypes> for Reindexed<'a, 'b, R, DI>
    where DTypes: DTypeList,
          R: 'a + Reindexer<DTypes>,
          DI: 'b + DataIndex<DTypes>,
{
    type DType = DI::DType;
    fn get_datum(&self, idx: usize) -> error::Result<Value<&Self::DType>> {
        self.orig.get_datum(self.reindexer.map_index(idx))
    }
    fn len(&self) -> usize {
        self.reindexer.len()
    }
}

impl<'a, DTypes, T> SelectField<'a, T, DTypes>
    for DataFrame<DTypes>
    where DTypes: 'a + DTypeList,
          DTypes::Storage: 'a + MaxLen<DTypes>,
          T: 'static + DataType<DTypes>
{
    type Output = Framed<'a, DTypes, T>;

    fn select(&'a self, ident: FieldIdent)
        -> error::Result<Framed<'a, DTypes, T>>
        where DTypes::Storage: TypeSelector<DTypes, T>
    {
        // let ident = ident.into();
        Ok(Framed::new(&self, self.store.select(ident)?))
        // self.field_map
        //     .get(&ident)
        //     .ok_or(AgnesError::FieldNotFound(ident.clone()))
        //     .map(|&field_idx| self.fields[field_idx])
        //     .and_then(|ds_field| self.data[&ds_field.ty].get(ds_field.ds_index))
        //     .map(|field| OwnedOrRef::Ref(field))
    }
}
impl<DTypes> Field<DTypes> for DataFrame<DTypes>
    where DTypes: DTypeList
{}

pub struct FramedFunc<'a, DTypes, F>
    where DTypes: 'a + DTypeList,
{
    func: F,
    frame: &'a DataFrame<DTypes>,
}
impl<'a, DTypes, F> FramedFunc<'a, DTypes, F>
    where DTypes: 'a + DTypeList,
{
    fn new(frame: &'a DataFrame<DTypes>, func: F) -> FramedFunc<'a, DTypes, F> {
        FramedFunc {
            func,
            frame,
        }
    }
}
impl<'a, DTypes, T, F> Func<DTypes, T> for FramedFunc<'a, DTypes, F>
    where F: Func<DTypes, T>,
          T: DataType<DTypes>,
          DTypes: DTypeList,
          DTypes::Storage: 'a + MaxLen<DTypes>
{
    type Output = F::Output;
    fn call(
        &mut self,
        type_data: &dyn DataIndex<DTypes, DType=T>,
    )
        -> F::Output
    {
        self.func.call(&Framed::new(self.frame, OwnedOrRef::Ref(type_data)))
    }
}
impl<'a, DTypes, T, F> FuncExt<DTypes, T> for FramedFunc<'a, DTypes, F>
    where F: FuncExt<DTypes, T>,
          T: DataType<DTypes>,
          DTypes: DTypeList,
          DTypes::Storage: 'a + MaxLen<DTypes>
{
    type Output = F::Output;
    fn call<L>(
        &mut self,
        type_data: &dyn DataIndex<DTypes, DType=T>,
        locator: &L,
            )
        -> F::Output
        where L: FieldLocator<DTypes>
    {
        self.func.call(&Framed::new(self.frame, OwnedOrRef::Ref(type_data)), locator)
    }
}

// impl<'a, T: 'static + DataType> DIter<'a, T> for Selection<'a, DataFrame> {
//     type DI = OwnedOrRef<'a, T>;
//     fn diter(&'a self) -> error::Result<DataIterator<'a, T, OwnedOrRef<'a, T>>> {
//         Framed::new(&self.data, self.data.store.select_one(self.ident))
//             .diter()
//     }
// }



/// Trait that provides a function for filtering a data structure's contents.
pub trait Filter<DTypes, T>: Field<DTypes>
    where T: 'static + DataType<DTypes>,
          DTypes: DTypeList,
          DTypes::Storage: MaxLen<DTypes> + TypeSelector<DTypes, T>
{
    /// Filter the contents of this data structure by applying the supplied predicate on the
    /// specified field.
    fn filter<I: Into<FieldIdent>, F: Fn(&T) -> bool>(&mut self, ident: I, pred: F)
        -> error::Result<Vec<usize>>;
}
impl<DTypes, T> Filter<DTypes, T> for DataFrame<DTypes>
    where DTypes: DTypeList,
          DTypes::Storage: MaxLen<DTypes> + TypeSelector<DTypes, T>,
          T: 'static + DataType<DTypes>,
          Self: Field<DTypes>
{
    fn filter<I: Into<FieldIdent>, F: Fn(&T) -> bool>(&mut self, ident: I, pred:F)
        -> error::Result<Vec<usize>>
    {
        let filter = self.field(ident)?.data_filter(pred);
        self.update_permutation(&filter);
        Ok(filter)
    }
}

// macro_rules! impl_filter {
//     ($($dtype:tt)*) => {$(

// impl Filter<$dtype> for DataFrame {
//     fn filter<F: Fn(&$dtype) -> bool>(&mut self, ident: &FieldIdent, pred: F)
//         -> error::Result<Vec<usize>>
//     {
        // let filter = self.field(ident)?.data_filter(pred);
        // self.update_permutation(&filter);
        // Ok(filter)
//     }
// }

//     )*}
// }
// impl_filter!(u64 i64 String bool f64);

// /// Trait that provides a function for sorting a data structure's contents.
// pub trait SortBy {
//     /// Sort the contents of this data structure (ascending) by the specified field.
//     fn sort_by(&mut self, ident: &FieldIdent) -> error::Result<Vec<usize>>;
// }
// impl SortBy for DataFrame {
    // fn sort_by(&mut self, ident: &FieldIdent) -> error::Result<Vec<usize>> {
    //     let sort_order = self.sort_order_by(ident)?;
    //     self.update_permutation(&sort_order);
    //     Ok(sort_order)
    // }
// }

// impl ApplyTo for DataFrame {
//     fn apply_to<F: MapFn>(&self, f: &mut F, ident: &FieldIdent)
//         -> error::Result<Vec<F::Output>>
//     {
//         (0..self.nrows()).map(|idx| {
//             self.store.apply_to_elem(f, &ident, self.map_index(idx))
//         }).collect()
//     }
// }
// impl ApplyToElem for DataFrame {
//     fn apply_to_elem<F: MapFn>(&self, f: &mut F, ident: &FieldIdent, idx: usize)
//         -> error::Result<F::Output>
//     {
//         self.store.apply_to_elem(f, &ident, self.map_index(idx))
//     }
// }
// impl FieldApplyTo for DataFrame {
//     fn field_apply_to<F: FieldMapFn>(&self, f: &mut F, ident: &FieldIdent)
//         -> error::Result<F::Output>
//     {
//         self.store.field_apply_to(&mut FrameFieldMapFn { frame: &self, field_fn: f }, &ident)
//     }
// }

// impl<'a> ApplyFieldReduce<'a> for Selection<'a, DataFrame> {
//     fn apply_field_reduce<F: FieldReduceFn<'a>>(&self, f: &mut F)
//         -> error::Result<F::Output>
//     {
//         self.data.store.select_one(&self.ident)
//             .apply_field_reduce(&mut FrameFieldReduceFn {
//                 frames: vec![&self.data],
//                 reduce_fn: f,
//             })
//     }
// }
// impl<'a> ApplyFieldReduce<'a> for Vec<Selection<'a, DataFrame>> {
//     fn apply_field_reduce<F: FieldReduceFn<'a>>(&self, f: &mut F)
//         -> error::Result<F::Output>
//     {
//         let frames = self.iter().map(|selection| selection.data).collect::<Vec<_>>();
//         self.iter().map(|selection| {
//             selection.data.store.select_one(&selection.ident)
//         }).collect::<Vec<_>>().apply_field_reduce(&mut FrameFieldReduceFn {
//             frames: frames,
//             reduce_fn: f,
//         })
//     }
// }

impl<DTypes> From<DataStore<DTypes>> for DataFrame<DTypes>
    where DTypes: DTypeList
{
    fn from(store: DataStore<DTypes>) -> DataFrame<DTypes> {
        DataFrame {
            permutation: None,
            store: Arc::new(store),
        }
    }
}

/// Structure to hold references to a data structure (e.g. DataStore) and a frame used to view
/// that structure. Provides DataIndex for the underlying data structure, as viewed through the
/// frame.
#[derive(Debug)]
pub struct Framed<'a, DTypes, T>
    where T: 'a + DataType<DTypes>,
          DTypes: 'a + DTypeList
{
    frame: &'a DataFrame<DTypes>,
    data: OwnedOrRef<'a, DTypes, T>,
    dtype: PhantomData<T>,
}
impl<'a, DTypes, T> Framed<'a, DTypes, T>
    where DTypes: DTypeList,
          T: DataType<DTypes>
{
    /// Create a new framed view of some data, as view through a particular `DataFrame`.
    pub fn new(frame: &'a DataFrame<DTypes>, data: OwnedOrRef<'a, DTypes, T>)
        -> Framed<'a, DTypes, T>
    {
        Framed { frame, data, dtype: PhantomData }
    }
}

macro_rules! impl_framed_data_index {
    ($($t:tt)*) => {$(

impl<'a, DTypes, T> DataIndex<DTypes> for $t<'a, DTypes, T>
    where T: DataType<DTypes>,
          DTypes: DTypeList,
          DTypes::Storage: MaxLen<DTypes>
{
    type DType = T;

    fn get_datum(&self, idx: usize) -> error::Result<Value<&T>> {
        self.data.get_datum(self.frame.map_index(idx))
    }
    fn len(&self) -> usize
    {
        self.frame.nrows()
    }
}

    )*}
}
impl_framed_data_index![Framed FramedMut];

#[derive(Debug)]
pub struct FramedMut<'a, DTypes, T>
    where T: 'a + DataType<DTypes>,
          DTypes: 'a + DTypeList
{
    frame: &'a mut DataFrame<DTypes>,
    data: OwnedOrRef<'a, DTypes, T>,
    dtype: PhantomData<T>
}
impl<'a, DTypes, T> FramedMut<'a, DTypes, T>
    where DTypes: DTypeList,
          T: DataType<DTypes>,
{
    /// Create a new framed view of some data, as view through a particular `DataFrame`.
    pub fn new(frame: &'a mut DataFrame<DTypes>, data: OwnedOrRef<'a, DTypes, T>)
        -> FramedMut<'a, DTypes, T>
    {
        FramedMut { frame, data, dtype: PhantomData }
    }
}
impl<'a, DTypes, T: DataType<DTypes>> DataIndexMut<DTypes> for FramedMut<'a, DTypes, T>
    where DTypes: DTypeList,
          DTypes::Storage: MaxLen<DTypes>
{

    // fn set_datum(&mut self, idx: usize, value: Value<T>) -> error::Result<()> {
    //     self.data.set_datum(self.frame.map_index(idx), value)
    // }
    fn push(&mut self, value: Value<T>) {
        let new_idx = self.data.len();
        self.data.push(value);
        match self.frame.permutation {
            Some(ref mut perm) => { perm.push(new_idx); }
            None => {}
        }
    }
}

// struct FrameFieldMapFn<'a, 'b, F: 'b + FieldMapFn> {
//     frame: &'a DataFrame,
//     field_fn: &'b mut F,
// }
// impl<'a, 'b, F: 'b + FieldMapFn> FieldMapFn for FrameFieldMapFn<'a, 'b, F> {
//     type Output = F::Output;
//     fn apply_unsigned<T: DataIndex<u64>>(&mut self, field: &T) -> F::Output {
//         self.field_fn.apply_unsigned(&Framed::new(self.frame, OwnedOrRef::Ref(field)))
//     }
//     fn apply_signed<T: DataIndex<i64>>(&mut self, field: &T) -> F::Output {
//         self.field_fn.apply_signed(&Framed::new(self.frame, OwnedOrRef::Ref(field)))
//     }
//     fn apply_text<T: DataIndex<String>>(&mut self, field: &T) -> F::Output {
//         self.field_fn.apply_text(&Framed::new(self.frame, OwnedOrRef::Ref(field)))
//     }
//     fn apply_boolean<T: DataIndex<bool>>(&mut self, field: &T) -> F::Output {
//         self.field_fn.apply_boolean(&Framed::new(self.frame, OwnedOrRef::Ref(field)))
//     }
//     fn apply_float<T: DataIndex<f64>>(&mut self, field: &T) -> F::Output {
//         self.field_fn.apply_float(&Framed::new(self.frame, OwnedOrRef::Ref(field)))
//     }
// }

// struct FrameFieldReduceFn<'a, 'b, F: 'b + FieldReduceFn<'a>> {
//     frames: Vec<&'a DataFrame>,
//     reduce_fn: &'b mut F,
// }
// impl<'a, 'b, F: FieldReduceFn<'a>> FieldReduceFn<'a> for FrameFieldReduceFn<'a, 'b, F>
// {
//     type Output = F::Output;
//     fn reduce(&mut self, mut fields: Vec<FieldData<'a>>) -> F::Output {
//         let data_vec = fields.drain(..).zip(self.frames.iter()).map(|(field, frame)| {
//             let field: FieldData<'a> = field;
//             match field {
//                 FieldData::Unsigned(field) =>
//                     FieldData::Unsigned(OwnedOrRef::Owned(Box::new(
//                         Framed::new(frame, field)))),
//                 FieldData::Signed(field) =>
//                     FieldData::Signed(OwnedOrRef::Owned(Box::new(
//                         Framed::new(frame, field)))),
//                 FieldData::Text(field) =>
//                     FieldData::Text(OwnedOrRef::Owned(Box::new(
//                         Framed::new(frame, field)))),
//                 FieldData::Boolean(field) =>
//                     FieldData::Boolean(OwnedOrRef::Owned(Box::new(
//                         Framed::new(frame, field)))),
//                 FieldData::Float(field) =>
//                     FieldData::Float(OwnedOrRef::Owned(Box::new(
//                         Framed::new(frame, field)))),
//             }
//         }
//         ).collect::<Vec<FieldData<'a>>>();
//         self.reduce_fn.reduce(data_vec)
//     }
// }

pub(crate) struct SerializedField<'a, DTypes>
    where DTypes: 'a + DTypeList
{
    ident: FieldIdent,
    frame: &'a DataFrame<DTypes>,
}
impl<'a, DTypes> SerializedField<'a, DTypes>
    where DTypes: DTypeList
{
    pub fn new(ident: FieldIdent, frame: &'a DataFrame<DTypes>) -> SerializedField<'a, DTypes> {
        SerializedField {
            ident,
            frame,
        }
    }
}

// struct SerializeFn<S: Serializer> {
//     serializer: Option<S>,
// }
// macro_rules! SResult { ($s:tt) => (Result<$s::Ok, $s::Error>) }
// fn do_serialize<'a, 'b, T: DataType + Serialize, S: 'a + Serializer>(
//         sfunc: &mut SerializeFunc<S>, field: &dyn DataIndex<DType=T>
//     ) -> Result<S::Ok, S::Error>
// {
//     let serializer = sfunc.serializer.take().unwrap();
//     let mut seq = serializer.serialize_seq(Some(field.len()))?;
//     for idx in 0..field.len() {
//         match field.get_datum(idx).unwrap() {
//             Value::Exists(&ref val) =>  seq.serialize_element(val)?,
//             Value::Na =>  seq.serialize_element("null")?
//         }
//     }
//     seq.end()
// }
// impl<Ser: Serializer> FieldMapFn for SerializeFn<Ser> {
//     type Output = sresult![Ser];
//     fn apply_unsigned<T: DataIndex<u64>>(&mut self, field: &T) -> sresult![Ser] {
//         do_serialize(self, field)
//     }
//     fn apply_signed<T: DataIndex<i64>>(&mut self, field: &T) -> sresult![Ser] {
//         do_serialize(self, field)
//     }
//     fn apply_text<T: DataIndex<String>>(&mut self, field: &T) -> sresult![Ser] {
//         do_serialize(self, field)
//     }
//     fn apply_boolean<T: DataIndex<bool>>(&mut self, field: &T) -> sresult![Ser] {
//         do_serialize(self, field)
//     }
//     fn apply_float<T: DataIndex<f64>>(&mut self, field: &T) -> sresult![Ser] {
//         do_serialize(self, field)
//     }
// }

// struct SerializeFunc<S: Serializer> {
//     serializer: S
// }
// impl<T, S> Func<T, ()> for SerializeFunc<S>
//     where T: DataType + Serialize,
//           S: Serializer
// {
//     fn call(
//         &mut self,
//         data: &dyn DataIndex<DType=T>,
//         _: &DsField,
//     )
//         -> ()
//     {
//         // let serializer = self.serializer.take().unwrap();
//         let mut seq = self.serializer.serialize_seq(Some(data.len()))?;
//         for idx in 0..data.len() {
//             match data.get_datum(idx).unwrap() {
//                 Value::Exists(&ref val) =>  seq.serialize_element(val)?,
//                 Value::Na =>  seq.serialize_element("null")?
//             }
//         }
//         seq.end();
//     }
// }

impl<'a, DTypes> Serialize for SerializedField<'a, DTypes>
    where DTypes: DTypeList,
          DTypes::Storage: MaxLen<DTypes> + FieldSerialize<DTypes>,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error> where S: Serializer,
    {
        self.frame.store.serialize_field(&self.ident, self.frame, serializer)
        // let data = self.frame.field(self.ident)
        //     .or(Err(ser::Error::custom(format!("missing field: {}",
        //         self.ident.to_string()))))?;
        // let mut seq = serializer.serialize_seq(Some(data.len()))?;
        // for idx in 0..data.len() {
        //     match data.get_datum(idx).unwrap() {
        //         Value::Exists(&ref val) =>  seq.serialize_element(val)?,
        //         Value::Na =>  seq.serialize_element("null")?
        //     }
        // }
        // seq.end()

        // struct SerializeFunc<S: Serializer> {
        //     serializer: Option<S>
        // }
        // impl<T, S: Serializer> Func<T, Result<S::Ok, S::Error>> for SerializeFunc<S>
        //     where T: DataType + Serialize,
        // {
        //     fn call(
        //         &mut self,
        //         data: &dyn DataIndex<DType=T>,
        //         _: &DsField
        //     )
        //         -> Result<S::Ok, S::Error>
        //     {
        //         let serializer = self.serializer.take().unwrap();
        //         let mut seq = serializer.serialize_seq(Some(data.len()))?;
        //         for idx in 0..data.len() {
        //             match data.get_datum(idx).unwrap() {
        //                 Value::Exists(&ref val) =>  seq.serialize_element(val)?,
        //                 Value::Na =>  seq.serialize_element("null")?
        //             }
        //         }
        //         seq.end()
        //     }
        // }

        // self.frame.map(&self.ident, SerializeFunc { serializer })
        //     .unwrap_or(
        //         Err(ser::Error::custom(format!("missing field: {}", self.ident.to_string())))
        //     )
        // self.frame.field_apply_to(
        //     &mut SerializeFn { serializer: Some(serializer) },
        //     &self.ident
        // ).unwrap_or(
        //     Err(ser::Error::custom(format!("missing field: {}", self.ident.to_string())))
        // )
    }
}
