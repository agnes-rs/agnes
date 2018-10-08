//! Data storage struct and implentation.

use std::collections::HashMap;

use serde::Serializer;
use serde::ser;

use field::{FieldIdent, TFieldIdent, FieldData, Value};
use error::*;
use frame::{Reindexer};
use access::{DataIndex, DataIndexMut, OwnedOrRef};
use select::{SelectField};
use data_types::*;
use view::DataView;

/// Details of a field within a data store
#[derive(Debug, Clone)]
pub struct DsField<DTypes: AssocTypes> {
    /// Field identifier
    ident: FieldIdent,
    /// Index of field within 'fields' vector in the data store
    ds_index: usize,
    /// `DataType` for this field
    ty: DTypes::DType,
    /// Index of field within the `TypeData` vector of fields of a specific type
    td_index: usize,
}
impl<DTypes> DsField<DTypes>
    where DTypes: AssocTypes
{
    /// Create a new `DsField`.
    pub(crate) fn new(
        ident: FieldIdent, ds_index: usize, ty: DTypes::DType, td_index: usize,
    )
        -> DsField<DTypes>
    {
        DsField {
            ident,
            ds_index,
            ty,
            td_index
        }
    }
}
impl<'a, DTypes> FieldLocator<DTypes> for &'a DsField<DTypes> where DTypes: AssocTypes {
    fn ty(&self) -> DTypes::DType {
        self.ty
    }
    fn td_idx(&self) -> usize {
        self.td_index
    }
}

/// Data storage underlying a dataframe. Data is retrievable both by index (of the fields vector)
/// and by field name.
///
/// DataStores are growable (through `AddData` and `AddDataVec`), but existing data is immutable.
#[derive(Debug)]
pub struct DataStore<DTypes: AssocTypes> {
    /// List of fields within the data store
    fields: Vec<DsField<DTypes>>,
    /// Map of field names to index of the fields vector
    field_map: HashMap<FieldIdent, usize>,

    /// Storage
    data: DTypes::Storage
}
impl<DTypes> DataStore<DTypes>
    where DTypes: AssocTypes,
          DTypes::Storage: CreateStorage,
{
    /// Generate and return an empty data store
    pub fn empty() -> DataStore<DTypes> {
        DataStore {
            fields: Vec::new(),
            field_map: HashMap::new(),

            // type_registry: HashMap::new(),
            data: DTypes::Storage::create_storage(),
        }
    }
}
impl<DTypes> DataStore<DTypes>
    where DTypes: DTypeList
{
    fn add_field_from_iter<T, I, V>(&mut self, field: TFieldIdent<T>, iter: I)
        -> Result<()>
        where T: 'static + DataType<DTypes> + Default + Clone,
              DTypes::Storage: TypeSelector<DTypes, T> + DTypeSelector<DTypes, T>,
              I: Iterator<Item=V>,
              V: Into<Value<T>>
    {
        match self.field_map.get(&field.ident) {
            Some(_) => {
                // field already exists
                Err(AgnesError::FieldCollision(vec![field.ident.clone()]))
            },
            None => {
                // add data to self.data structure
                let (dtype, data) = (self.data.select_dtype(), self.data.select_type_mut());
                let td_idx = data.len();
                data.push(iter.map(|v| v.into()).collect::<FieldData<DTypes, T>>());

                // add indexing information
                let fields_idx = self.fields.len();
                self.field_map.insert(field.ident.clone(), fields_idx);
                self.fields.push(DsField::new(field.ident, fields_idx, dtype, td_idx));

                Ok(())
            }
        }
    }

    /// Add an empty field to a DataStore.
    fn add_empty_field<T>(&mut self, field: TFieldIdent<T>)
        -> Result<()>
        where T: 'static + DataType<DTypes>,
              DTypes::Storage: TypeSelector<DTypes, T> + DTypeSelector<DTypes, T>,
    {
        match self.field_map.get(&field.ident) {
            Some(_) => {
                // field already exists
                Err(AgnesError::FieldCollision(vec![field.ident.clone()]))
            },
            None => {
                // add data to self.data structure
                let (dtype, data) = (self.data.select_dtype(), self.data.select_type_mut());
                let td_idx = data.len();
                data.push(FieldData::default());

                // add indexing information
                let fields_idx = self.fields.len();
                self.field_map.insert(field.ident.clone(), fields_idx);
                self.fields.push(DsField::new(field.ident, fields_idx, dtype, td_idx));

                Ok(())
            }
        }
    }

    fn insert<T>(&mut self, ident: &FieldIdent, value: Value<T>)
        -> Result<()>
        where T: 'static + DataType<DTypes> + Default + Clone,
              DTypes::Storage: TypeSelector<DTypes, T>
    {
        match self.field_map.get(ident) {
            Some(&idx) => {
                let ds_field = &self.fields[idx];
                let data = self.data.select_type_mut();
                data[ds_field.td_index].push(value);
                Ok(())
            },
            None => {
                Err(AgnesError::FieldNotFound(ident.clone()))
            }
        }
    }

    pub fn map<F, FOut>(&self, ident: &FieldIdent, f: F) -> Result<FOut>
        where DTypes::Storage: Map<DTypes, F, FOut>,
    {
        let ds_field = self.field_map
            .get(&ident)
            .ok_or_else(|| AgnesError::FieldNotFound(ident.clone()))
            .map(|&field_idx| &self.fields[field_idx])?;

        self.data.map(
            &ds_field,
            f,
        )
    }
    pub fn tmap<T, F>(&self, ident: &FieldIdent, f: F) -> Result<F::Output>
        where F: Func<DTypes, T>,
              T: DataType<DTypes>,
              DTypes::Storage: TMap<DTypes, T, F>,
    {
        let ds_field = self.field_map
            .get(&ident)
            .ok_or_else(|| AgnesError::FieldNotFound(ident.clone()))
            .map(|&field_idx| &self.fields[field_idx])?;

        if ds_field.ty != T::DTYPE {
            return Err(AgnesError::IncompatibleTypes {
                expected: ds_field.ty.to_string(),
                actual: T::DTYPE.to_string()
            });
        }

        self.data.tmap(
            &ds_field,
            f,
        )
    }
    pub fn map_ext<F, FOut>(&self, ident: &FieldIdent, f: F) -> Result<FOut>
        where DTypes::Storage: MapExt<DTypes, F, FOut>,
    {
        let ds_field = self.field_map
            .get(&ident)
            .ok_or_else(|| AgnesError::FieldNotFound(ident.clone()))
            .map(|&field_idx| &self.fields[field_idx])?;

        self.data.map_ext(
            &ds_field,
            f,
        )
    }
    pub fn map_partial<F, R>(&self, ident: &FieldIdent, reindexer: &R, f: F)
        -> Result<Option<F::Output>>
        where DTypes::Storage: MapPartial<DTypes, F>,
              F: FuncPartial<DTypes>,
              R: Reindexer<DTypes>
    {
        let ds_field = self.field_map
            .get(&ident)
            .ok_or_else(|| AgnesError::FieldNotFound(ident.clone()))
            .map(|&field_idx| &self.fields[field_idx])?;

        Ok(self.data.map_partial(
            &ds_field,
            reindexer,
            f,
        ))
    }

    pub(crate) fn serialize_field<R, S>(&self, ident: &FieldIdent, reindexer: &R, serializer: S)
        -> ::std::result::Result<S::Ok, S::Error>
        where R: Reindexer<DTypes>,
              S: Serializer,
              DTypes: AssocTypes,
              DTypes::Storage: FieldSerialize<DTypes>
    {
        match self.field_map.get(ident) {
            Some(&idx) => {
                let ds_field = &self.fields[idx];
                self.data.serialize(&ds_field, reindexer, serializer)
            },
            None => {
                Err(ser::Error::custom(format!("missing field: {}", ident.to_string())))
            }
        }
    }
}

pub struct CopyInto<'a, DTypes: 'a + AssocTypes> {
    pub src_idx: usize,
    pub target_ident: FieldIdent,
    pub target_ds: &'a mut DataStore<DTypes>
}
impl<'a, T, DTypes> FuncExt<DTypes, T> for CopyInto<'a, DTypes>
    where T: 'static + DataType<DTypes> + Default + Clone,
          DTypes: 'a + DTypeList,
          DTypes::Storage: CreateStorage + AddVec<T> + TypeSelector<DTypes, T>,
{
    type Output = ();
    fn call<L>(
        &mut self,
        data: &dyn DataIndex<DTypes, DType=T>,
        locator: &L,
    )
        where L: FieldLocator<DTypes>
    {
        // ensure that there is a place to put the data
        if !self.target_ds.field_map.contains_key(&self.target_ident) {
            // add new data field in TypeData structure
            // add_vec only fails if the type number doesn't exist, but we know it exists
            // because DTypes is the same for both data structures
            let td_idx = self.target_ds.data.add_vec().unwrap();

            // add indexing information
            let field_idx = self.target_ds.fields.len();
            self.target_ds.field_map.insert(self.target_ident.clone(), field_idx);
            self.target_ds.fields.push(DsField::new(self.target_ident.clone(), field_idx,
                locator.ty(), td_idx));
        }

        // insert only fails if identifier doesn't exist, but we just ensured it does.
        // unwrap is safe.
        self.target_ds.insert(
            &self.target_ident.clone(),
            data.get_datum(self.src_idx).unwrap().cloned()
        ).unwrap();
    }

}

impl<DTypes: AssocTypes> DataStore<DTypes> {
    pub fn fields(&self) -> impl Iterator<Item=&FieldIdent> {
        self.fields.iter().map(|ds_field| &ds_field.ident)
    }

    /// Returns `true` if this `DataStore` contains this field.
    pub fn has_field(&self, ident: &FieldIdent) -> bool {
        self.field_map.contains_key(ident)
    }

    /// Get the field information struct for a given field name
    pub fn get_field_type(&self, ident: &FieldIdent) -> Option<DTypes::DType> {
        self.field_map.get(ident)
            .and_then(|&index| self.fields.get(index).map(|dsfield| dsfield.ty))
    }

    /// Retrieve number of rows for this data store
    pub fn nrows(&self) -> usize
        where DTypes: AssocTypes,
              DTypes::Storage: MaxLen<DTypes>
    {
        self.data.max_len()
    }
}
impl<DTypes> Default for DataStore<DTypes>
    where DTypes: AssocTypes,
          DTypes::Storage: CreateStorage
{
    fn default() -> DataStore<DTypes> {
        DataStore::empty()
    }
}

impl<'a, DTypes, T> SelectField<'a, T, DTypes> for DataStore<DTypes>
    where T: 'static + DataType<DTypes>,
          DTypes: 'a + DTypeList
{
    type Output = OwnedOrRef<'a, DTypes, T>;

    fn select(&'a self, ident: FieldIdent)
        -> Result<OwnedOrRef<'a, DTypes, T>>
        where DTypes::Storage: TypeSelector<DTypes, T>
    {
        self.field_map
            .get(&ident)
            .ok_or_else(|| AgnesError::FieldNotFound(ident.clone()))
            .map(|&field_idx| &self.fields[field_idx])
            .and_then(|ds_field| {
                // by construction, td_index is always in range, so unwrap is safe
                Ok(self.data.select_type().get(ds_field.td_index).unwrap())
            })
            .map(|field| OwnedOrRef::Ref(field) )
    }
}

impl<DTypes> DataStore<DTypes>
    where DTypes: DTypeList
{
    pub fn add<T, V>(&mut self, ident: FieldIdent, value: V) -> Result<()>
        where T: 'static + DataType<DTypes> + Default,
              V: Into<Value<T>>,
              Self: AddData<T, DTypes>,
              DTypes::Storage: TypeSelector<DTypes, T>
    {
        AddData::<T, DTypes>::add(self, ident, value)
    }
}

/// Trait for adding data (of valid types) to a `DataStore`.
pub trait AddData<T, DTypes>
    where T: DataType<DTypes>,
          DTypes: DTypeList,
          DTypes::Storage: TypeSelector<DTypes, T>
{
    /// Add a single value to the specified field.
    fn add<V: Into<Value<T>>>(&mut self, ident: FieldIdent, value: V) -> Result<()>;
}

impl<DTypes, T> AddData<T, DTypes>
    for DataStore<DTypes>
    where T: 'static + DataType<DTypes> + Default + Clone,
          DTypes: DTypeList,
          DTypes::Storage: TypeSelector<DTypes, T> + DTypeSelector<DTypes, T>
{
    fn add<V: Into<Value<T>>>(&mut self, ident: FieldIdent, value: V)
        -> Result<()>
    {
        if !self.has_field(&ident) {
            self.add_empty_field::<T>(TFieldIdent::new(ident.clone()))?;
        }
        self.insert(&ident, value.into())
    }
}

/// Trait for adding a vector of data (of valid types) to a `DataStore`.
pub trait AddDataVec<T, DTypes>
    where T: DataType<DTypes>,
          DTypes: DTypeList,
          DTypes::Storage: TypeSelector<DTypes, T>
{
    /// Add a vector of data values to the specified field.
    fn add_data_vec<I: Into<FieldIdent>, V: Into<Value<T>>>(
        &mut self, ident: I, data: Vec<V>
    )
        -> Result<()>;
}

impl<DTypes, T> AddDataVec<T, DTypes>
    for DataStore<DTypes>
    where T: 'static + DataType<DTypes> + Default + Clone,
          DTypes: DTypeList,
          DTypes::Storage: TypeSelector<DTypes, T> + DTypeSelector<DTypes, T>
{
    fn add_data_vec<I: Into<FieldIdent>, V: Into<Value<T>>>(
        &mut self, ident: I, mut data: Vec<V>
    )
        -> Result<()>
    {
        self.add_field_from_iter::<T, _, V>(TFieldIdent::new(ident.into()), data.drain(..))
    }
}



/// Trait for adding data to a data structure (e.g. `DataStore`) from an iterator.
pub trait AddDataFromIter<T, DTypes>
    where T: DataType<DTypes>,
          DTypes: DTypeList,
          DTypes::Storage: TypeSelector<DTypes, T>
{
    /// Add data to `self` with provided field identifier from an iterator over items of type
    /// `Value<T>`.
    fn add_data_from_iter<I, Iter, V>(&mut self, ident: I, iter: Iter)
        -> Result<()>
        where I: Into<FieldIdent>, V: Into<Value<T>>, Iter: Iterator<Item=V>;
}

impl<DTypes, T> AddDataFromIter<T, DTypes>
    for DataStore<DTypes>
    where T: 'static + DataType<DTypes> + Default + Clone,
          DTypes: DTypeList,
          DTypes::Storage: TypeSelector<DTypes, T> + DTypeSelector<DTypes, T>
{
    fn add_data_from_iter<I, Iter, V>(&mut self, ident: I, iter: Iter)
        -> Result<()>
        where I: Into<FieldIdent>,
              V: Into<Value<T>>,
              Iter: Iterator<Item=V>,
    {
        self.add_field_from_iter::<T, _, V>(TFieldIdent::new(ident.into()), iter)
    }
}



pub trait AddClonedDataFromIter<'a, T, DTypes>
    where T: 'a + DataType<DTypes>,
          DTypes: DTypeList,
          DTypes::Storage: TypeSelector<DTypes, T>
{
    fn add_cloned_data_from_iter<I, Iter, V>(&mut self, ident: I, iter: Iter)
        -> Result<()>
        where I: Into<FieldIdent>,
              V: Into<Value<&'a T>>,
              Iter: Iterator<Item=V>;
}

impl<'a, DTypes, T> AddClonedDataFromIter<'a, T, DTypes>
    for DataStore<DTypes>
    where T: 'static + DataType<DTypes> + Default + Clone,
          DTypes: DTypeList,
          DTypes::Storage: TypeSelector<DTypes, T> + DTypeSelector<DTypes, T>
{
    fn add_cloned_data_from_iter<I, Iter, V>(&mut self, ident: I, iter: Iter)
        -> Result<()>
        where I: Into<FieldIdent>, V: Into<Value<&'a T>>, Iter: Iterator<Item=V>,
    {
        self.add_data_from_iter(
            ident,
            iter.map(|datum| datum.into().map(|val| val.clone()))
        )
    }
}

pub trait WithDataVec<T, DTypes>
    where T: DataType<DTypes>,
          DTypes: DTypeList,
          DTypes::Storage: TypeSelector<DTypes, T>
{
    fn with_data_vec<I: Into<FieldIdent>, V: Into<Value<T>>>(self, ident: I, data: Vec<V>)
        -> Result<Self>
        where Self: Sized;
}
impl<T, U, DTypes> WithDataVec<T, DTypes> for U
    where T: DataType<DTypes>,
          U: AddDataVec<T, DTypes>,
          DTypes: DTypeList,
          DTypes::Storage: TypeSelector<DTypes, T>
{
    fn with_data_vec<I: Into<FieldIdent>, V: Into<Value<T>>>(mut self, ident: I,
        data: Vec<V>) -> Result<Self>
    {
        self.add_data_vec(ident, data)?;
        Ok(self)
    }
}
impl<DTypes> DataStore<DTypes>
    where DTypes: DTypeList,
{
    pub fn with_data_vec<T: DataType<DTypes>, I: Into<FieldIdent>, V: Into<Value<T>>>(
        self,
        ident: I,
        data: Vec<V>
    )
        -> Result<Self>
        where Self: WithDataVec<T, DTypes>,
              DTypes::Storage: TypeSelector<DTypes, T>
    {
        WithDataVec::<T, DTypes>::with_data_vec(self, ident, data)
    }
}

pub trait WithDataFromIter<T, DTypes>
    where T: DataType<DTypes>,
          DTypes: DTypeList,
          DTypes::Storage: TypeSelector<DTypes, T>
{
    fn with_data_from_iter<I, Iter, V>(self, ident: I, iter: Iter) -> Result<Self>
        where I: Into<FieldIdent>,
              V: Into<Value<T>>,
              Iter: Iterator<Item=V>,
              Self: Sized;
}
impl<T, U, DTypes> WithDataFromIter<T, DTypes> for U
    where T: DataType<DTypes>,
          U: AddDataFromIter<T, DTypes>,
          DTypes: DTypeList,
          DTypes::Storage: TypeSelector<DTypes, T>
{
    fn with_data_from_iter<I, Iter, V>(mut self, ident: I, iter: Iter) -> Result<Self>
        where I: Into<FieldIdent>,
              V: Into<Value<T>>,
              Iter: Iterator<Item=V>,
    {
        self.add_data_from_iter(ident, iter)?;
        Ok(self)
    }
}
pub trait WithClonedDataFromIter<'a, T, DTypes>
    where T: 'a + DataType<DTypes>,
          DTypes: DTypeList,
          DTypes::Storage: TypeSelector<DTypes, T>,
{
    fn with_cloned_data_from_iter<I, Iter, V>(self, ident: I, iter: Iter)
        -> Result<Self>
        where I: Into<FieldIdent>, V: Into<Value<&'a T>>,
              Iter: Iterator<Item=V>,
              Self: Sized;
}
impl<'a, DTypes, T> WithClonedDataFromIter<'a, T, DTypes>
    for DataStore<DTypes>
    where T: 'static + DataType<DTypes> + Default,
          DTypes: DTypeList,
          DTypes::Storage: TypeSelector<DTypes, T>,
          DataStore<DTypes>: AddClonedDataFromIter<'a, T, DTypes>
{
    fn with_cloned_data_from_iter<I, Iter, V>(mut self, ident: I, iter: Iter)
        -> Result<Self>
        where I: Into<FieldIdent>,
              V: Into<Value<&'a T>>,
              Iter: Iterator<Item=V>,
    {
        self.add_cloned_data_from_iter(ident, iter)?;
        Ok(self)
    }
}

pub trait IntoDataStore<DTypes: DTypeList> {
    fn into_datastore<I: Into<FieldIdent>>(self, ident: I) -> Result<DataStore<DTypes>>;

    fn into_ds<I: Into<FieldIdent>>(self, ident: I) -> Result<DataStore<DTypes>> where Self: Sized {
        self.into_datastore(ident)
    }

    fn into_dataview<I: Into<FieldIdent>>(self, ident: I) -> Result<DataView<DTypes>>
        where Self: Sized
    {
        self.into_datastore(ident).map(DataView::from)
    }

    fn into_dv<I: Into<FieldIdent>>(self, ident: I) -> Result<DataView<DTypes>> where Self: Sized {
        self.into_dataview(ident)
    }
}
