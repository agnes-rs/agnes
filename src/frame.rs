/*!
Structs and implementation for Frame-level data structure. A `DataFrame` is a reference to an
underlying data store, along with record-based filtering and sorting details.
*/
use std::fmt::Debug;
use std::sync::Arc;
use std::marker::PhantomData;
use serde::{Serialize, Serializer};

// use filter::{Filter, DataFilter};
use store::{DataStore, AssocStorage};
// use data_types::*;
use field::{FieldIdent, Value};
use access::{OwnedOrRef, DataIndex};
use select::{SelectField, FSelect};
// use apply::sort::SortOrderFn;
use fieldlist::FSelector;
use error;
// use field::{Value};

/// A data frame. A `DataStore` reference along with record-based filtering and sorting details.
#[derive(Debug, Clone)]
pub struct DataFrame<Fields: AssocStorage>
{
    pub(crate) permutation: Option<Vec<usize>>,
    pub(crate) store: Arc<DataStore<Fields>>,
}
impl<Fields> DataFrame<Fields>
    where Fields: AssocStorage
{
    /// Number of rows that pass the filter in this frame.
    pub fn nrows(&self) -> usize
        // where DTypes::Storage: MaxLen<DTypes>
    {
        self.len()
    }
    #[cfg(test)]
    pub(crate) fn store_ref_count(&self) -> usize {
        Arc::strong_count(&self.store)
    }
    // /// Get the field type of a particular field in the underlying `DataStore`.
    // pub fn get_field_type(&self, ident: &FieldIdent) -> Option<DTypes::DType> {
    //     self.store.get_field_type(ident)
    // }
    pub(crate) fn has_same_store(&self, other: &DataFrame<Fields>) -> bool {
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

    // /// Applies the provided `Func` to the data in the specified field. This `Func` must be
    // /// implemented for all types in `DTypes`.
    // ///
    // /// Fails if the specified identifier is not found in this `DataFrame`.
    // pub fn map<F, FOut>(&self, ident: &FieldIdent, f: F)
    //     -> error::Result<FOut>
    //     where DTypes::Storage: FramedMap<DTypes, F, FOut>,
    // {
    //     self.store.map(ident, FramedFunc::new(self, f))
    // }

    // /// Applies the provided `Func` to the data in the specified field. This `Func` must be
    // /// implemented for type `T`.
    // ///
    // /// Fails if the specified identifier is not found in this `DataFrame` or the incorrect type `T`
    // /// is used.
    // pub fn tmap<T, F>(&self, ident: &FieldIdent, f: F)
    //     -> error::Result<F::Output>
    //     where F: Func<DTypes, T>,
    //           T: DataType<DTypes>,
    //           DTypes::Storage: MaxLen<DTypes> + FramedTMap<DTypes, T, F>,
    // {
    //     self.store.tmap(ident, FramedFunc::new(self, f))
    // }

    // /// Applies the provided `FuncExt` to the data in the specified field. This `FuncExt` must be
    // /// implemented for all types in `DTypes`.
    // ///
    // /// Fails if the specified identifier is not found in this `DataFrame`.
    // pub fn map_ext<F, FOut>(&self, ident: &FieldIdent, f: F)
    //     -> error::Result<FOut>
    //     where DTypes::Storage: FramedMapExt<DTypes, F, FOut>,
    // {
    //     self.store.map_ext(ident, FramedFunc::new(self, f))
    // }

    // /// Applies the provided `FuncPartial` to the data in the specified field.
    // ///
    // /// Fails if the specified identifier is not found in this `DataFrame`.
    // pub fn map_partial<F>(&self, ident: &FieldIdent, f: F)
    //     -> error::Result<Option<F::Output>>
    //     where DTypes::Storage: MapPartial<DTypes, F> + MaxLen<DTypes>,
    //           F: FuncPartial<DTypes>
    // {
    //     self.store.map_partial(ident, self, f)
    // }

    // /// Returns the permutation (list of indices in sorted order) of values in field identified
    // /// by `ident`.
    // ///
    // /// Fails if the field is not found in this `DataFrame`.
    // pub fn sort_by(&mut self, ident: &FieldIdent) -> error::Result<Vec<usize>>
    //     where DTypes::Storage: FramedMap<DTypes, SortOrderFn, Vec<usize>>
    // {
    //     let sort_order = self.sort_order_by(ident)?;
    //     self.update_permutation(&sort_order);
    //     Ok(sort_order)
    // }

    // fn sort_order_by(&self, ident: &FieldIdent) -> error::Result<Vec<usize>>
    //     where DTypes::Storage: FramedMap<DTypes, SortOrderFn, Vec<usize>>,
    // {
    //     self.map(ident, SortOrderFn)
    // }
}

// /// Marker trait for a storage structure that implements [Map](../data_types/trait.Map.html), as
// /// accessed through a [DataFrame](struct.DataFrame.html).
// pub trait FramedMap<DTypes, F, FOut>:
//     for<'a> Map<DTypes, FramedFunc<'a, DTypes, F>, FOut>
//     where DTypes: AssocTypes
// {}
// impl<DTypes, F, FOut, T> FramedMap<DTypes, F, FOut> for T
//     where T: for<'a> Map<DTypes, FramedFunc<'a, DTypes, F>, FOut>,
//           DTypes: AssocTypes
// {}

// /// Marker trait for a storage structure that implements [TMap](../data_types/trait.TMap.html), as
// /// accessed through a [DataFrame](struct.DataFrame.html).
// pub trait FramedTMap<DTypes, T, F>:
//     for<'a> TMap<DTypes, T, FramedFunc<'a, DTypes, F>>
//     where DTypes: AssocTypes,
//           T: DataType<DTypes>
// {}
// impl<DTypes, T, F, U> FramedTMap<DTypes, T, F> for U
//     where U: for<'a> TMap<DTypes, T, FramedFunc<'a, DTypes, F>>,
//           DTypes: AssocTypes,
//           T: DataType<DTypes>
// {}

// /// Marker trait for a storage structure that implements [MapExt](../data_types/trait.MapExt.html),
// /// as accessed through a [DataFrame](struct.DataFrame.html).
// pub trait FramedMapExt<DTypes, F, FOut>:
//     for<'a> MapExt<DTypes, FramedFunc<'a, DTypes, F>, FOut>
//     where DTypes: AssocTypes
// {}
// impl<DTypes, F, FOut, T> FramedMapExt<DTypes, F, FOut> for T
//     where T: for<'a> MapExt<DTypes, FramedFunc<'a, DTypes, F>, FOut>,
//           DTypes: AssocTypes
// {}

/// Trait for a data structure that re-indexes data and provides methods for accessing that
/// reorganized data.
pub trait Reindexer: Debug {
    /// Returns the length of this field.
    fn len(&self) -> usize;
    /// Returns `true` if this field is empty.
    fn is_empty(&self) -> bool { self.len() == 0 }
    /// Returns the re-organized index of a requested index.
    fn map_index(&self, requested: usize) -> usize;
    /// Returns a [Reindexed](struct.Reindexed.html) structure implementing
    /// [DataIndex](../access/trait.DataIndex.html) that provides access to the reorganized data.
    fn reindex<'a, 'b, DI>(&'a self, data_index: &'b DI) -> Reindexed<'a,'b, Self, DI>
        where DI: 'b + DataIndex,
              Self: Sized
    {
        Reindexed {
            orig: data_index,
            reindexer: self
        }
    }
}

impl<DTypes> Reindexer<DTypes> for DataFrame<DTypes>
    // where DTypes: DTypeList,
    //       DTypes::Storage: MaxLen<DTypes>
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

/// Data structure that provides [DataIndex](../access/trait.DataIndex.html) access to a reorganized
/// (sorted / shuffled) data field.
#[derive(Debug)]
pub struct Reindexed<'a, 'b, R: 'a, DI: 'b>
{
    reindexer: &'a R,
    orig: &'b DI,
}
impl<'a, 'b, DI, R> DataIndex for Reindexed<'a, 'b, R, DI>
    where R: 'a + Reindexer,
          DI: 'b + DataIndex,
{
    type DType = DI::DType;
    fn get_datum(&self, idx: usize) -> error::Result<Value<&Self::DType>> {
        self.orig.get_datum(self.reindexer.map_index(idx))
    }
    fn len(&self) -> usize {
        self.reindexer.len()
    }
}

impl<'a, Fields, Ident, FIdx> SelectField<'a, Ident, FIdx>
    for DataFrame<Fields>
    where Fields: FSelector<Ident, FIdx>
          // DTypes: 'a + DTypeList,
          // DTypes::Storage: 'a + MaxLen<DTypes>,
          // T: 'a + DataType

{
    type Output = Framed<'a, Fields::DType>;

    fn select_field(&'a self)
        -> Framed<'a, Fields::DType>
        // where DTypes::Storage: TypeSelector<T>
    {
        Ok(Framed::new(&self, self.store.select()?))
    }
}
impl<Fields> FSelect for DataFrame<Fields>
{}

// /// Wrapper for a [Func](../data_types/trait.Func.html) that calls the underlying `Func` with the
// /// field data organized by this [DataFrame](struct.DataFrame.html).
// pub struct FramedFunc<'a, DTypes, F>
//     where DTypes: 'a + DTypeList,
// {
//     func: F,
//     frame: &'a DataFrame<DTypes>,
// }
// impl<'a, DTypes, F> FramedFunc<'a, DTypes, F>
//     where DTypes: 'a + DTypeList,
// {
//     fn new(frame: &'a DataFrame<DTypes>, func: F) -> FramedFunc<'a, DTypes, F> {
//         FramedFunc {
//             func,
//             frame,
//         }
//     }
// }

// impl<'a, DTypes, T, F> Func<DTypes, T> for FramedFunc<'a, DTypes, F>
//     where F: Func<DTypes, T>,
//           T: DataType<DTypes>,
//           DTypes: DTypeList,
//           DTypes::Storage: 'a + MaxLen<DTypes>
// {
//     type Output = F::Output;
//     fn call(
//         &mut self,
//         type_data: &dyn DataIndex<DTypes, DType=T>,
//     )
//         -> F::Output
//     {
//         self.func.call(&Framed::new(self.frame, OwnedOrRef::Ref(type_data)))
//     }
// }

// impl<'a, DTypes, T, F> FuncExt<DTypes, T> for FramedFunc<'a, DTypes, F>
//     where F: FuncExt<DTypes, T>,
//           T: DataType<DTypes>,
//           DTypes: DTypeList,
//           DTypes::Storage: 'a + MaxLen<DTypes>
// {
//     type Output = F::Output;
//     fn call<L>(
//         &mut self,
//         type_data: &dyn DataIndex<DTypes, DType=T>,
//         locator: &L,
//             )
//         -> F::Output
//         where L: FieldLocator<DTypes>
//     {
//         self.func.call(&Framed::new(self.frame, OwnedOrRef::Ref(type_data)), locator)
//     }
// }

// impl<Fields> Filter for DataFrame<Fields>
//     where
//           // DTypes: DTypeList,
//           // DTypes::Storage: MaxLen<DTypes> + TypeSelector<DTypes, T>,
//           // T: 'static + DataType<DTypes>,
//           Self: FSelect<Fields>
// {
//     fn filter<Ident, FIdx, F>(&mut self, pred: F)
//         -> error::Result<Vec<usize>>
//         where Fields: FSelector<Ident, FIdx>,
//               F: Fn(&Fields::DType) -> bool
//     {
//         let filter = self.field::<Ident, _>()?.data_filter(pred);
//         self.update_permutation(&filter);
//         Ok(filter)
//     }
// }

impl<Fields> From<DataStore<Fields>> for DataFrame<Fields>
{
    fn from(store: DataStore<Fields>) -> DataFrame<Fields> {
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
pub struct Framed<'a, Fields, T>
    // where T: 'a + DataType<DTypes>,
    //       DTypes: 'a + DTypeList
{
    frame: &'a DataFrame<Fields>,
    data: OwnedOrRef<'a, T>,
    // dtype: PhantomData<T>,
}
impl<'a, Fields, T> Framed<'a, Fields, T>
    // where DTypes: DTypeList,
    //       T: DataType<DTypes>
{
    /// Create a new framed view of some data, as view through a particular `DataFrame`.
    pub fn new(frame: &'a DataFrame<Fields>, data: OwnedOrRef<'a, Fields, T>)
        -> Framed<'a, Fields, T>
    {
        Framed { frame, data }
    }
}

impl<'a, Fields, T> DataIndex<Fields> for Framed<'a, Fields, T>
    // where T: DataType<DTypes>,
    //       DTypes: DTypeList,
    //       DTypes::Storage: MaxLen<DTypes>
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

pub(crate) struct SerializedField<'a, Ident, FIdx, Fields>
    // where DTypes: 'a + DTypeList
{
    _ident: PhantomData<Ident>,
    _fidx: PhantomData<FIdx>,
    frame: &'a DataFrame<Fields>,
}
impl<'a, Ident, FIdx, Fields> SerializedField<'a, Ident, FIdx, Fields>
    // where DTypes: DTypeList
{
    pub fn new(frame: &'a DataFrame<Fields>)
        -> SerializedField<'a, Ident, FIdx, Fields>
    {
        SerializedField {
            _ident: PhantomData,
            _fidx: PhantomData,
            frame,
        }
    }
}

impl<'a, Ident, FIdx, Fields> Serialize for SerializedField<'a, Ident, FIdx, Fields>
    // where DTypes: DTypeList,
    //       DTypes::Storage: MaxLen<DTypes> + FieldSerialize<DTypes>,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error> where S: Serializer,
    {
        self.frame.store.serialize_field(&self.ident, self.frame, serializer)
    }
}
