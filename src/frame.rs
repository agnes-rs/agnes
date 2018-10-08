/*!
Structs and implementation for Frame-level data structure. A `DataFrame` is a reference to an
underlying data store, along with record-based filtering and sorting details.
*/
use std::fmt::Debug;
use std::sync::Arc;
use std::marker::PhantomData;
use serde::{Serialize, Serializer};

use filter::{Filter, DataFilter};
use store::DataStore;
use data_types::*;
use field::{FieldIdent};
use access::{OwnedOrRef, DataIndex};
use select::{SelectField, Field};
use apply::sort::SortOrderFn;
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
    pub(crate) fn update_permutation(&mut self, new_permutation: &[usize]) {
        // check if we already have a permutation
        self.permutation = match self.permutation {
            Some(ref prev_perm) => {
                // we already have a permutation, map the filter indices through it
                Some(new_permutation.iter().map(|&new_idx| prev_perm[new_idx]).collect())
            },
            None => Some(new_permutation.to_vec())
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

    pub fn sort_by(&mut self, ident: &FieldIdent) -> error::Result<Vec<usize>>
        where DTypes::Storage: FramedMap<DTypes, SortOrderFn, Vec<usize>>
    {
        let sort_order = self.sort_order_by(ident)?;
        self.update_permutation(&sort_order);
        Ok(sort_order)
    }

    fn sort_order_by(&self, ident: &FieldIdent) -> error::Result<Vec<usize>>
        where DTypes::Storage: FramedMap<DTypes, SortOrderFn, Vec<usize>>,
    {
        self.map(ident, SortOrderFn)
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
    fn is_empty(&self) -> bool { self.len() == 0 }
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
        Ok(Framed::new(&self, self.store.select(ident)?))
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

impl<'a, DTypes, T> DataIndex<DTypes> for Framed<'a, DTypes, T>
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

impl<'a, DTypes> Serialize for SerializedField<'a, DTypes>
    where DTypes: DTypeList,
          DTypes::Storage: MaxLen<DTypes> + FieldSerialize<DTypes>,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error> where S: Serializer,
    {
        self.frame.store.serialize_field(&self.ident, self.frame, serializer)
    }
}
