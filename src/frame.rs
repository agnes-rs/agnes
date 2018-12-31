/*!
Structs and implementation for Frame-level data structure. A `DataFrame` is a reference to an
underlying data store, along with record-based filtering and sorting details.
*/
#[cfg(serialize)]
use serde::{Serialize, Serializer};
use std::fmt::Debug;
use std::rc::Rc;
use std::sync::Arc;

// use filter::{Filter, DataFilter};
use store::{AssocStorage, DataStore, DataRef, NRows};
// use data_types::*;
use access::{self, DataIndex};
use field::{FieldData, Value};
use label::{ElemOf, LookupElemByLabel, TypeOf, TypeOfElemOf, Typed, Valued};
use select::{FieldSelect, SelectFieldByLabel};
// use apply::sort::SortOrderFn;
use error;
// use field::{Value};

type Permutation = access::Permutation<Vec<usize>>;

/// A data frame. A `DataStore` reference along with record-based filtering and sorting details.
#[derive(Debug, Clone)]
pub struct DataFrame<Fields>
where
    Fields: AssocStorage,
    Fields::Storage: Debug,
{
    permutation: Rc<Permutation>,
    store: Arc<DataStore<Fields>>,
}
impl<Fields> DataFrame<Fields>
where
    Fields: AssocStorage,
    DataStore<Fields>: NRows,
{
    pub fn len(&self) -> usize {
        match self.permutation.len() {
            Some(len) => len,
            None => self.store.nrows(),
        }
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
impl<Fields> NRows for DataFrame<Fields>
where
    Fields: AssocStorage,
    DataStore<Fields>: NRows,
{
    fn nrows(&self) -> usize {
        self.len()
    }
}
#[cfg(test)]
impl<Fields> DataFrame<Fields>
where
    Fields: AssocStorage,
{
    pub fn store_ref_count(&self) -> usize {
        Arc::strong_count(&self.store)
    }
}
impl<Fields> DataFrame<Fields>
where
    Fields: AssocStorage,
{
    // /// Get the field type of a particular field in the underlying `DataStore`.
    // pub fn get_field_type(&self, ident: &FieldIdent) -> Option<DTypes::DType> {
    //     self.store.get_field_type(ident)
    // }
    #[cfg(test)]
    pub(crate) fn has_same_store(&self, other: &DataFrame<Fields>) -> bool {
        Arc::ptr_eq(&self.store, &other.store)
    }
    // /// Returns `true` if this `DataFrame` contains this field.
    // pub fn has_field(&self, s: &FieldIdent) -> bool {
    //     self.store.has_field(s)
    // }
    pub(crate) fn update_permutation(&mut self, new_permutation: &[usize]) {
        Rc::make_mut(&mut self.permutation).update(new_permutation);
    }
}

pub trait FrameFields {
    type FrameFields;
}
impl<Fields> FrameFields for DataFrame<Fields>
where
    Fields: AssocStorage,
{
    type FrameFields = Fields;
}
pub type FrameFieldsOf<T> = <T as FrameFields>::FrameFields;

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

// /// Trait for a data structure that re-indexes data and provides methods for accessing that
// /// reorganized data.
// pub trait Reindexer: Debug {
//     // /// Returns the length of this field.
//     // fn len(&self) -> usize;
//     // /// Returns `true` if this field is empty.
//     // fn is_empty(&self) -> bool { self.len() == 0 }
//     /// Returns a [Reindexed](struct.Reindexed.html) structure implementing
//     /// [DataIndex](../access/trait.DataIndex.html) that provides access to the reorganized data.
//     fn reindex<'a, 'b, DI>(&'a self, data_index: &'b DI) -> Reindexed<'a,'b, Self, DI>
//         where DI: 'b + DataIndex,
//               Self: Sized
//     {
//         Reindexed {
//             orig: data_index,
//             reindexer: self
//         }
//     }
// }

// impl<Fields> Reindexer for DataFrame<Fields>
//     where Fields: AssocStorage + Debug,
//           Fields::Storage: NRows
//     // where DTypes: DTypeList,
//     //       DTypes::Storage: MaxLen<DTypes>
// {
// fn len(&self) -> usize
// {
//     match self.permutation {
//         Some(ref perm) => perm.len(),
//         None => self.store.nrows()
//     }
// }

//     fn map_index(&self, requested: usize) -> usize {
//         match self.permutation {
//             Some(ref perm) => perm[requested],
//             None => requested
//         }
//     }
// }

// /// Data structure that provides [DataIndex](../access/trait.DataIndex.html) access to a reorganized
// /// (sorted / shuffled) data field.
// #[derive(Debug)]
// pub struct Reindexed<'a, 'b, R: 'a, DI: 'b>
// {
//     reindexer: &'a R,
//     orig: &'b DI,
// }
// impl<'a, 'b, DI, R> DataIndex for Reindexed<'a, 'b, R, DI>
//     where R: 'a + Reindexer,
//           DI: 'b + DataIndex,
// {
//     type DType = DI::DType;
//     fn get_datum(&self, idx: usize) -> error::Result<Value<&Self::DType>> {
//         self.orig.get_datum(self.reindexer.map_index(idx))
//     }
//     fn len(&self) -> usize {
//         self.reindexer.len()
//     }
// }

// impl<'a, Fields, Ident, FIdx> SelectField<'a, Ident>
//     for DataFrame<Fields>
//     where Fields: FSelector<Ident, FIdx>
//           // DTypes: 'a + DTypeList,
//           // DTypes::Storage: 'a + MaxLen<DTypes>,
//           // T: 'a + DataType

// {
//     type Output = Framed<'a, Fields, Fields::DType>;

//     fn select_field(&'a self)
//         -> Framed<'a, Fields, Fields::DType>
//         // where DTypes::Storage: TypeSelector<T>
//     {
//         Ok(Framed::new(&self, self.store.select()?))
//     }
// }
// impl<Fields> FSelect for DataFrame<Fields>
// {}

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
where
    Fields: AssocStorage,
{
    fn from(store: DataStore<Fields>) -> DataFrame<Fields> {
        DataFrame {
            permutation: Rc::new(Permutation::default()),
            store: Arc::new(store),
        }
    }
}

/// Structure to hold references to a data structure (e.g. DataStore) and a frame used to view
/// that structure. Provides DataIndex for the underlying data structure, as viewed through the
/// frame.
#[derive(Debug)]
pub struct Framed<T> {
    permutation: Rc<Permutation>,
    data: DataRef<T>,
    // dtype: PhantomData<T>,
}
impl<T> Framed<T> {
    /// Create a new framed view of some data, as view through a particular `DataFrame`.
    pub fn new(permutation: Rc<Permutation>, data: DataRef<T>) -> Framed<T> {
        Framed { permutation, data }
    }
}
impl<T> Clone for Framed<T> {
    fn clone(&self) -> Framed<T> {
        Framed {
            permutation: Rc::clone(&self.permutation),
            data: DataRef::clone(&self.data),
        }
    }
}

impl<T> DataIndex for Framed<T>
where
    T: Debug,
    // where T: DataType<DTypes>,
    //       DTypes: DTypeList,
    //       DTypes::Storage: MaxLen<DTypes>
{
    type DType = T;

    fn get_datum(&self, idx: usize) -> error::Result<Value<&T>> {
        self.data.get_datum(self.permutation.map_index(idx))
    }
    fn len(&self) -> usize {
        match self.permutation.len() {
            Some(len) => len,
            None => self.data.len(),
        }
        // self.frame.nrows()
    }
}

#[cfg(serialize)]
pub(crate) struct SerializedField<'a, Ident, FIdx, Fields>
where
    Fields: 'a + AssocStorage, // where DTypes: 'a + DTypeList
{
    _ident: PhantomData<Ident>,
    _fidx: PhantomData<FIdx>,
    frame: &'a DataFrame<Fields>,
}
#[cfg(serialize)]
impl<'a, Ident, FIdx, Fields> SerializedField<'a, Ident, FIdx, Fields>
// where DTypes: DTypeList
where
    Fields: 'a + AssocStorage,
{
    pub fn new(frame: &'a DataFrame<Fields>) -> SerializedField<'a, Ident, FIdx, Fields> {
        SerializedField {
            _ident: PhantomData,
            _fidx: PhantomData,
            frame,
        }
    }
}

#[cfg(serialize)]
impl<'a, Ident, FIdx, Fields> Serialize for SerializedField<'a, Ident, FIdx, Fields>
// where DTypes: DTypeList,
//       DTypes::Storage: MaxLen<DTypes> + FieldSerialize<DTypes>,
where
    Fields: 'a + AssocStorage,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.frame
            .store
            .serialize_field(&self.ident, self.frame, serializer)
    }
}

impl<Fields, Label> SelectFieldByLabel<Label> for DataFrame<Fields>
where
    //Label: 'a,
    Fields: AssocStorage + Debug,
    Fields::Storage: LookupElemByLabel<Label> + NRows,
    ElemOf<Fields::Storage, Label>: Typed,
    ElemOf<Fields::Storage, Label>:
        Valued<Value = DataRef<TypeOfElemOf<Fields::Storage, Label>>>,
    // ValueOfElemOf<Fields::Storage, Label>:
    //   DataIndex<DType=TypeOfElemOf<Fields::Storage, Label>>,
    TypeOf<ElemOf<Fields::Storage, Label>>: Debug,
{
    type Output = Framed<TypeOf<ElemOf<Fields::Storage, Label>>>;

    fn select_field(&self) -> Self::Output {
        Framed::new(
            Rc::clone(&self.permutation),
            DataRef::clone(&self.store.field::<Label>()),
        )
    }
}

impl<Fields> FieldSelect for DataFrame<Fields> where Fields: AssocStorage {}

// impl<Fields> DataFrame<Fields>
//     where Fields: AssocStorage
// {
//     pub fn field<'a, Label>(&'a self)
//         -> Framed<
//             'a,
//             Fields,
//             TypeOf<<Fields::Storage as LookupElemByLabel<Label>>::Elem>
//         >
//         where Fields::Storage: LookupElemByLabel<Label>,
//               ElemOf<Fields::Storage, Label>: 'a + Typed + SelfValued
//                 + DataIndex<DType=TypeOf<ElemOf<Fields::Storage, Label>>>
//     {
//         Framed::new(
//             self,
//             self.store.field::<Label>()
//         )
//     }
// }

#[cfg(test)]
mod tests {

    use std::path::Path;

    use csv_sniffer::metadata::Metadata;

    use super::*;

    use select::FieldSelect;
    use source::csv::{CsvReader, CsvSource, IntoCsvSrcSpec};

    fn load_csv_file<Spec>(filename: &str, spec: Spec) -> (CsvReader<Spec::CsvSrcSpec>, Metadata)
    where
        Spec: IntoCsvSrcSpec,
    {
        let data_filepath = Path::new(file!()) // start as this file
            .parent()
            .unwrap() // navigate up to src directory
            .parent()
            .unwrap() // navigate up to root directory
            .join("tests") // navigate into integration tests directory            .join("data")                      // navigate into data directory
            .join("data") // navigate into data directory
            .join(filename); // navigate to target file

        let source = CsvSource::new(data_filepath.into()).unwrap();
        (
            CsvReader::new(&source, spec).unwrap(),
            source.metadata().clone(),
        )
    }

    namespace![
        pub namespace gdp {
            field CountryName: String;
            field CountryCode: String;
            field Year1983: f64;
        }
    ];

    #[test]
    fn frame_select() {
        let gdp_spec = spec![
            fieldname gdp::CountryName = "Country Name";
            fieldname gdp::CountryCode = "Country Code";
            fieldname gdp::Year1983 = "1983";
        ];

        let (mut csv_rdr, _metadata) = load_csv_file("gdp.csv", gdp_spec.clone());
        let ds = csv_rdr.read().unwrap();

        // println!("{:?}", ds);
        // println!("{:?}", ds.field::<CountryName>());

        let frame = DataFrame::from(ds);
        println!("{:?}", frame.field::<gdp::CountryName>());

        // let (mut csv_rdr, _metadata) = load_csv_file("gdp.csv", gdp_spec);
        // let ds = csv_rdr.read().unwrap();
        // let view = ds.into_view();
        // println!("{:?}", view.field::<CountryName>());
        // println!("{}", view);
    }
}
