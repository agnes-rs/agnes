//! Data storage struct and implentation.

use std::collections::{HashMap, HashSet};

use serde::Serializer;
use serde::ser;

use field::{FieldIdent, TFieldIdent, FieldData, Value};
use error::*;
use frame::{Reindexer};
use access::{DataIndex, DataIndexMut, OwnedOrRef};
// use apply::mapfn::*;
use select::{SelectField};
use data_types::*;
use view::DataView;

// trait StoreData {
//     fn new<'a, T: 'static + DataType>() -> Self;
//     fn get<'a, T: 'static + DataType>(&'a self, td_index: usize)
//         -> &'a FieldData<T>;
//     fn insert_from_iter<T, I, V>(&mut self, iter: I) -> usize
//         where T: 'static + DataType + Default,
//               I: Iterator<Item=V>,
//               V: Into<Value<T>>;
//     fn insert<T>(&mut self, td_idx: usize, value: Value<T>)
//         where T: 'static + DataType + Default;
//     fn insert_empty<T>(&mut self) -> usize
//         where T: 'static + DataType;
//     fn nrows(&self) -> usize;
// }

// impl<T: DataType> StoreData for TypeData<T> {

// }

// #[derive(Debug)]
// pub struct BoxedTypeData {
//     boxed: Box<dyn Any>,
//     nrows: usize
// }
// impl BoxedTypeData {
//     fn new<'a, T: 'static + DataType>() -> BoxedTypeData {
//         BoxedTypeData {
//             boxed: Box::new(TypeData::<T>::new()),
//             nrows: 0,
//         }
//     }
//     // fn data_iter<'a, T: 'static + DataType>(&'a self, ds_index: usize)
//     //     -> DataIterator<'a, T>
//     //     where FieldData<T>: DataIndex<Output=T>
//     // {
//     //     match self.boxed.downcast_ref::<TypeData<T>>() {
//     //         Some(ref data) => DataIterator::new(&data[ds_index]),
//     //         None => panic!["type error while retrieving from TypeData"]
//     //     }
//     // }
//     fn get<'a, T: 'static + DataType>(&'a self, td_index: usize)
//         -> &'a FieldData<T>
//     {
//         match self.boxed.downcast_ref::<TypeData<T>>() {
//             Some(ref data) => &data[td_index],
//             None => panic!["type error while retrieving from TypeData"]
//         }
//     }
//     fn insert_from_iter<T, I, V>(&mut self, iter: I) -> usize
//         where T: 'static + DataType + Default,
//               I: Iterator<Item=V>,
//               V: Into<Value<T>>
//     {
//         match self.boxed.downcast_mut::<TypeData<T>>() {
//             Some(type_data) => {
//                 let td_idx = type_data.len();
//                 let data = iter.map(|v| v.into()).collect::<FieldData<T>>();
//                 self.nrows = data.len();
//                 type_data.push(data);
//                 td_idx
//             },
//             None => panic!["type error while inserting into TypeData"]
//         }
//     }
//     // TODO: update this to assert properly sized stores
//     fn insert_empty<T>(&mut self) -> usize
//         where T: 'static + DataType
//     {
//         match self.boxed.downcast_mut::<TypeData<T>>() {
//             Some(type_data) => {
//                 let td_idx = type_data.len();
//                 type_data.push(FieldData::new());
//                 td_idx
//             },
//             None => panic!["type error while inserting into TypeData"]
//         }
//     }
//     fn insert<T>(&mut self, td_idx: usize, value: Value<T>)
//         where T: 'static + DataType + Default
//     {
//         match self.boxed.downcast_mut::<TypeData<T>>() {
//             Some(type_data) => {
//                 type_data[td_idx].push(value);
//                 self.nrows = type_data[td_idx].len();
//             },
//             None => panic!["type error while inserting into TypeData"]
//         }
//     }
//     // // TODO: update this to assert properly sized stores
//     // fn insert<T>(&mut self, ds_index: usize, value: Value<T>)
//     //     -> Result<()>
//     //     where T: 'static + DataType + Default
//     // {
//     //     match self.boxed.downcast_mut::<TypeData<T>>() {
//     //         Some(type_data) => {
//     //             type_data.get_mut(ds_index).unwrap().push(value);
//     //                 // .entry(ident)
//     //                 // .or_insert(FieldData::new())
//     //             self.nrows += 1;
//     //             Ok(())
//     //         },
//     //         None => Err(AgnesError::TypeMismatch("type error".into()))
//     //     }

//     // }
//     fn nrows(&self) -> usize {
//         self.nrows
//     }
// }

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
    // /// Create a new `DsField` from field identifier, type, and data store index
    // pub(crate) fn new<T: 'static + DataType>(tident: TFieldIdent<T>, ds_index: usize,
    //     td_index: usize)
    //     -> DsField
    // {
    //     DsField {
    //         ident: tident.ident,
    //         ty: TypeId::of::<T>(),
    //         ds_index: ds_index,
    //         td_index: td_index,
    //     }
    // }
    /// Create a new `DsField` from a typed field identifier and a data store index
    pub(crate) fn new(
        ident: FieldIdent, ds_index: usize, ty: DTypes::DType, td_index: usize,
    )
        -> DsField<DTypes>
    {
        DsField {
            ident: ident,
            ds_index,
            ty,
            td_index
        }
    }
    /// Accessor for the field identifier
    pub(crate) fn ident<'a>(&'a self) -> &'a FieldIdent {
        &self.ident
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

// pub struct Record<'a, DTypes>
//     where DTypes: DTypeList + RefAssocTypes<'a>,
// {
//     values: DTypes::RecordValues,
//     idents: Vec<FieldIdent>,
// }

// pub struct PartialRecord<'a, DTypes>
//     where DTypes: DTypeList + RefAssocTypes<'a>,
// {
//     values: DTypes::PartialRecordValues,
//     idents: Vec<FieldIdent>
// }

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StoreRecord<'a, DTypes>
    where DTypes: DTypeList + RefAssocTypes<'a>,
{
    values: DTypes::PartialRecordValues,
    idents: Vec<FieldIdent>
}

// impl<'a, DTypes> PartialRecord<'a, DTypes>
//     where DTYpes: DTypeList + RefAssocTypes<'a>,
// {
//     pub fn combine(self, other: PartialRecord<'a, DTypes>) -> PartialRecord<'a, DTypes> {

//     }
//     pub fn complete(self) -> Result<Record<'a, DTypes>> {

//     }
// }

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
    // data: HashMap<TypeId, BoxedTypeData>,
    // type_registry: HashMap<TypeId, TypeNum>,
    data: DTypes::Storage
    // registry: HashMap<TypeId, DType>,

    // /// Storage for unsigned integers
    // unsigned: TypeData<u64>,
    // /// Storage for signed integers
    // signed: TypeData<i64>,
    // /// Storage for strings
    // text: TypeData<String>,
    // /// Storage for booleans
    // boolean: TypeData<bool>,
    // /// Storage for floating-point numbers
    // float: TypeData<f64>,
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
                // let btd = self.data.entry(TypeId::of::<T>())
                    // .or_insert(BoxedTypeData::new::<T>());
                // let td_idx = td.insert_from_iter(iter);
                let (dtype, data) = (self.data.select_dtype(), self.data.select_type_mut());
                // let TypeSelectionMut { data: type_data, num: type_num } =
                //     self.data.select_type_mut();
                // self.type_registry.insert(TypeId::of::<T>(), type_num);
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
                // let btd = self.data.entry(TypeId::of::<T>())
                //     .or_insert(BoxedTypeData::new::<T>());
                // let td_idx = btd.insert_empty::<T>();
                let (dtype, data) = (self.data.select_dtype(), self.data.select_type_mut());
                // let TypeSelectionMut { data: type_data, num: type_num } =
                //     self.data.select_type_mut();
                // self.type_registry.insert(TypeId::of::<T>(), type_num);
                let td_idx = data.len();
                data.push(FieldData::new());

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
                // by construction, ident won't exist in field_map unless type ID exists in data
                // map, so unwrap is safe.
                // let btd = self.data.get_mut(&ds_field.ty).unwrap();
                let data = self.data.select_type_mut();
                // let TypeSelectionMut { data: type_data, .. } = self.data.select_type_mut();
                data[ds_field.td_index].push(value);
                // btd.insert(ds_field.td_index, value);
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
            .ok_or(AgnesError::FieldNotFound(ident.clone()))
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
            .ok_or(AgnesError::FieldNotFound(ident.clone()))
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
            .ok_or(AgnesError::FieldNotFound(ident.clone()))
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
            .ok_or(AgnesError::FieldNotFound(ident.clone()))
            .map(|&field_idx| &self.fields[field_idx])?;

        Ok(self.data.map_partial(
            &ds_field,
            reindexer,
            f,
        ))
    }

    // pub fn map_opt<F, FOut, Flag>(&self, ident: &FieldIdent, f: F) -> Result<Option<FOut>>
    //     where DTypes::Storage: TypeNumMapOpt<F, FOut, Flag>,
    // {
    //     let ds_field = self.field_map
    //         .get(&ident)
    //         .ok_or(AgnesError::FieldNotFound(ident.clone()))
    //         .map(|&field_idx| &self.fields[field_idx])?;

    //     self.data.map_opt(
    //         ds_field,
    //         f,
    //     )
    // }

    // pub fn map_into<F, FOut>(
    //     &self, ident: &FieldIdent, target: &mut DTypes, f: F
    // )
    //     -> Result<FOut>
    //     where DTypes: TypeNumMapInto<F, FOut>
    // {
    //     let ds_field = self.field_map
    //         .get(&ident)
    //         .ok_or(AgnesError::FieldNotFound(ident.clone()))
    //         .map(|&field_idx| &self.fields[field_idx])?;

    //     self.data.map_into(
    //         target,
    //         ds_field,
    //         f
    //     )
    // }

    // pub fn copy_into2(
    //     &self,
    //     ident: &FieldIdent,
    //     idx: usize,
    //     target_ds: &mut DataStore<DTypes>,
    //     target_ident: &FieldIdent,
    // )
    //     -> Result<()>
    // {
    //     // get the source ds field details
    //     let src_ds_field = self.field_map
    //         .get(&ident)
    //         .ok_or(AgnesError::FieldNotFound(ident.clone()))
    //         .map(|&field_idx| &self.fields[field_idx])?;

    //     // ensure that there is a place to put the data
    //     let target_ds_field_idx = match target_ds.field_map.get(&target_ident).map(|v| *v) {
    //         Some(field_idx) => {
    //             field_idx
    //         },
    //         None => {
    //             // add new data field in TypeData structure
    //             // add_vec only fails if the type number doesn't exist, but we know it exists
    //             // because DTypes is the same for both data structures
    //             let td_idx = target_ds.data.add_vec().unwrap();

    //             // add indexing information
    //             let field_idx = target_ds.fields.len();
    //             target_ds.field_map.insert(target_ident.clone(), field_idx);
    //             target_ds.fields.push(DsField::new(target_ident.clone(), field_idx,
    //                 src_ds_field.type_num, td_idx));
    //             field_idx
    //         }
    //     };

    // }
    // pub fn copy_into(
    //     &self,
    //     ident: &FieldIdent,
    //     idx: usize,
    //     // index_mapper: F,
    //     target_ds: &mut DataStore<DTypes>,
    //     target_ident: &FieldIdent,
    // )
    //     -> Result<()>
    //     where DTypes: TypeNumMapInto<CopyInto, ()>
    //                   + TypeNumAddVec,// + TypeNumMapMut<Insert<T>, ()>
    //           // F: Fn(usize) -> usize,
    //     // where DTypes: AssociatedValue<'a> + DtValueForTypeNum<'a, DTypes, Idx>
    //     // where DTypes: MapForTypeNum<DTypes, CopyInto<'a, 'b, DTypes>>
    // {
    //     // get the source ds field details10
    //     let src_ds_field = self.field_map
    //         .get(&ident)
    //         .ok_or(AgnesError::FieldNotFound(ident.clone()))
    //         .map(|&field_idx| &self.fields[field_idx])?;

    //     // ensure that there is a place to put the data
    //     let target_ds_field_idx = match target_ds.field_map.get(&target_ident).map(|v| *v) {
    //         Some(field_idx) => {
    //             field_idx
    //         },
    //         None => {
    //             // add new data field in TypeData structure
    //             // add_vec only fails if the type number doesn't exist, but we know it exists
    //             // because DTypes is the same for both data structures
    //             let td_idx = target_ds.data.add_vec(src_ds_field.type_num).unwrap();

    //             // add indexing information
    //             let field_idx = target_ds.fields.len();
    //             target_ds.field_map.insert(target_ident.clone(), field_idx);
    //             target_ds.fields.push(DsField::new(target_ident.clone(), field_idx,
    //                 src_ds_field.type_num, td_idx));
    //             field_idx
    //         }
    //     };

    //     self.data.map_into(
    //         src_ds_field,
    //         &mut target_ds.data,
    //         &target_ds.fields[target_ds_field_idx],
    //         CopyInto {
    //             src_idx: idx,
    //             // index_mapper
    //             // target_ds_fields: (&mut target_ds.fields, &mut target_ds.field_map)
    //         }
    //     )?;
    //     Ok(())

    //     // self.map_into(
    //     //     ident,
    //     //     &mut target_ds.data,
    //     //     CopyInto {
    //     //         src_idx: idx,
    //     //         // target_ds: target_ds,
    //     //         target_ds_fields: (&mut target_ds.fields, &mut target_ds.field_map)
    //     //     }
    //     // )
    //     // let ds_field = self.field_map
    //     //     .get(&ident)
    //     //     .ok_or(AgnesError::FieldNotFound(ident.clone()))
    //     //     .map(|&field_idx| &self.fields[field_idx])?;

    //     // self.data
    //     //     .map_into(
    //     //         &mut target_ds.data,
    //     //         ds_field.type_num,
    //     //         CopyInto {
    //     //             src_idx: idx,
    //     //             src_ds_field: ds_field,
    //     //             target_ds_fields: (&mut target_ds.fields, &mut target_ds.field_map)
    //     //         }
    //     //     )

    //             // .and_then(move |ds_field| {
    //             //     // let target_type_num = 1;
    //             // let dt_value = self.data.dt_value_for_type_num(target_type_num)?;

    //             // fn do_insert<H, Idx, DTypes>(
    //             //     td: &TypeData<H>,
    //             //     src_idx: usize,
    //             //     ds_field: &DsField,
    //             //     target_ds: &mut DataStore<DTypes>,
    //             // )
    //             //     where DTypes: TypeSelector<H, Idx>,
    //             //           H: 'static + DataType + Default,
    //             // {
    //             //     target_ds.insert::<H, _>(
    //             //         &ds_field.ident,
    //             //         td.get(ds_field.td_index).unwrap().get_datum(src_idx).unwrap().cloned()
    //             //     );
    //             // }
    //             // self.data.get_dt_value()

    //             // self.data.map_for_type_num(
    //             //     target_type_num,
    //             //     CopyInto::<DTypes> {
    //             //         src_idx: idx,
    //             //         src_ds_field: &ds_field,
    //             //         target_ds
    //             //     }
    //             // )?;

    //             // fn transit<DTypes, H, T, I1, I2>(
    //             //     cons: DTypeCons<H, T>,
    //             //     target_type_num: usize,
    //             //     src_idx: usize,
    //             //     src_ds_field: &DsField,
    //             //     target_ds: &mut DataStore<DTypes>
    //             // )
    //             //     where DTypes: DTypeList + TypeSelector<H, I1> + TypeSelector<T, I2>,
    //             //           H: DataType + Default,
    //             //           DTypeCons<H, T>: DTypeList,
    //             // {
    //             //     if DTypeCons::<H, T>::TYPE_NUM == target_type_num {
    //             //         target_ds.insert::<H, _>(
    //             //             &src_ds_field.ident,
    //             //             cons.head.get(src_ds_field.td_index).unwrap()
    //             //                 .get_datum(src_idx).unwrap().cloned()
    //             //         );
    //             //     } else {
    //             //         transit::<DTypes, T, _, _, _>(
    //             //             cons.tail,
    //             //             target_type_num,
    //             //             src_idx,
    //             //             src_ds_field,
    //             //             target_ds
    //             //         );
    //             //     }
    //             // }
    //             // transit(self.data, target_type_num, idx, ds_field, target_ds);
    //             // Ok(())
    //         // })

    // }

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

    // pub fn record<'a>(&'a self, idx: usize)
    //     -> Record<'a, DTypes>
    //     where DTypes: AssocTypes + RefAssocTypes<'a>,
    //           DTypes::RecordValues: RetrieveValues<'a, DTypes::Storage>
    // {
    //     Record {
    //         values: DTypes::RecordValues::retrieve_values(&self.data, idx),
    //         idents: self.fields.iter().map(|ds_field| ds_field.ident).collect()
    //     }
    // }

    /// Returns a StoreRecord containings values for the specified `FieldIdent`s. Ignores any
    /// `FieldIdent`s that doesn't exist in this `DataStore`.
    pub fn store_record<'a, I, Iter, IntoIter>(&'a self, idx: usize, idents: IntoIter)
        -> StoreRecord<'a, DTypes>
        where DTypes: AssocTypes + RefAssocTypes<'a>,
              DTypes::PartialRecordValues: RetrieveValuesPartial<'a, DTypes, DTypes::Storage>,
              I: Into<FieldIdent>,
              Iter: Iterator<Item=I>,
              IntoIter: IntoIterator<Item=I, IntoIter=Iter>
    {
        let ds_fields = idents.into_iter()
            .filter_map(|ident| {
                let ident = ident.into();
                self.field_map.get(&ident)
            })
            .map(|&field_idx| &self.fields[field_idx])
            .collect::<Vec<_>>();
        let idents = ds_fields.iter().map(|ds_field| ds_field.ident.clone()).collect();
        let field_set: FieldLocatorSet<DTypes> = ds_fields.iter()
            .map(|&ds_field| ds_field).collect();
        StoreRecord {
            values: DTypes::PartialRecordValues::retrieve_values_partial(
                &self.data,
                &field_set,
                idx
            ),
            idents: idents,
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

        // // get the source ds field details
        // let src_ds_field = self.field_map
        //     .get(&ident)
        //     .ok_or(AgnesError::FieldNotFound(ident.clone()))
        //     .map(|&field_idx| &self.fields[field_idx])?;

        // ensure that there is a place to put the data
        let target_ds_field_idx = match self.target_ds.field_map
            .get(&self.target_ident).map(|v| *v)
        {
            Some(field_idx) => {
                field_idx
            },
            None => {
                // add new data field in TypeData structure
                // add_vec only fails if the type number doesn't exist, but we know it exists
                // because DTypes is the same for both data structures
                let td_idx = self.target_ds.data.add_vec().unwrap();

                // add indexing information
                let field_idx = self.target_ds.fields.len();
                self.target_ds.field_map.insert(self.target_ident.clone(), field_idx);
                self.target_ds.fields.push(DsField::new(self.target_ident.clone(), field_idx,
                    locator.ty(), td_idx));
                field_idx
            }
        };

        self.target_ds.insert(
            &self.target_ident.clone(),
            data.get_datum(self.src_idx).unwrap().cloned()
        );
    }

}

// pub struct CopyInto {
//     src_idx: usize,
//     // index_mapper: Box<dyn Fn(usize) -> usize>
//     // target_ds: &'b mut DataStore<DTypes>,
// }
// // impl<'a, 'b, DTypes: DTypeList> FnAll<'b, DTypes> for CopyInto<'a, 'b, DTypes>
// //     where DTypes: AssociatedValue<'b>

// impl<'a, T> IntoFunc<T, ()> for CopyInto
//     where T: 'static + DataType + Default,
//           // DTypes: TypeNumAddVec + DTypeList
// {
//     // type Output = ();
//     // type Target = TypeData<T>;

//     fn call(
//         &mut self,
//         left: &dyn DataIndex<DType=T>,
//         right: &mut dyn DataIndexMut<DType=T>,
//     )
//         // where T: 'static + DataType + Default,
//         //       DTypes: TypeSelector<T, Idx>
//     {
//         // type_data.get()
//         // if !right.has_field(&self.src_ds_field.ident) {
//         //     self.add_empty_field::<T, Idx>(TFieldIdent::new(ident.clone()))?;
//         // }
//         // self.insert(&ident, value.into())

//         // // make sure this field already exists in right
//         // let (ref mut fields, ref mut field_map) = self.target_ds_fields;
//         // let fields_idx = match field_map.get(&src_ds_field.ident) {
//         //     Some(&fields_idx) => fields_idx,
//         //     None => {
//         //         // add new data field in TypeData structure
//         //         // add_vec only fails if the type number doesn't exist, but we know it exists
//         //         // because DTypes is the same for both data structures
//         //         let td_idx = right.add_vec(src_ds_field.type_num).unwrap();
//         //         // let (type_num, type_data) = data.select_type_mut();
//         //         // self.type_registry.insert(TypeId::of::<T>(), type_num);
//         //         // let td_idx = type_data.len();
//         //         // type_data.push(FieldData::new());
//         //         // let td_idx = right.len();
//         //         // right.push(FieldData::new());

//         //         // add indexing information
//         //         let ident = src_ds_field.ident.clone();
//         //         let fields_idx = fields.len();
//         //         field_map.insert(ident.clone(), fields_idx);
//         //         fields.push(DsField::new(ident, fields_idx,
//         //             src_ds_field.type_num, td_idx));
//         //         fields_idx
//         //     }
//         // };
//         // let target_ds_field = &fields[fields_idx];

//         // add the value
//         // self.target_ds.data.map_mut(
//         //     &src_ds_field,
//         //     Insert {
//         //         value: type_data.get_datum(self.src_idx).unwrap().cloned()
//         //     }
//         // );
//         // self.target_ds.insert(
//         //     &src_ds_field.ident,
//         right.push(
//             left//.get(src_ds_field.td_index).unwrap()
//                 .get_datum(self.src_idx).unwrap().cloned()
//         );

//         // right[self.src_ds_field.td_index].push(
//         //     left.get(self.src_ds_field.td_index).unwrap()
//         //         .get_datum(self.src_idx).unwrap().cloned()
//         // );

//         // self.target_ds.insert::<T, _>(
//         //     &self.src_ds_field.ident,
//         //     type_data.get(self.src_ds_field.td_index).unwrap()
//         //         .get_datum(self.src_idx).unwrap().cloned()
//         // ).unwrap();
//     }
// }

// pub struct Insert<T: DataType> {
//     value: Value<T>
// }
// impl<T> FuncMut<T, ()> for Insert<T>
//     where T: 'static + DataType + Default,
// {
//     fn call(
//         &mut self,
//         data: &mut dyn DataIndex<DType=T>,
//         src_ds_field: &DsField,
//     )
//     {
//         data.push(self.value);
//     }
// }



impl<DTypes: AssocTypes> DataStore<DTypes> {
    // /// Create a new `DataStore` which will contain the provided fields.
    // pub fn with_fields(mut fields: Vec<TypedFieldIdent>) -> DataStore {
    //     let mut ds = DataStore {
    //         fields: Vec::with_capacity(fields.len()),
    //         field_map: HashMap::with_capacity(fields.len()),

    //         data: HashMap::new(),
    //         // // could precompute lengths here to guess capacity, not sure if it'd be necessarily
    //         // // faster
    //         // unsigned: HashMap::new(),
    //         // signed: HashMap::new(),
    //         // text: HashMap::new(),
    //         // boolean: HashMap::new(),
    //         // float: HashMap::new(),
    //     };
    //     for field in fields.drain(..) {
    //         ds.add_field(field);
    //     }
    //     ds
    // }
    // /// Create a new `DataStore` from an interator of fields.
    // pub fn with_field_iter<I: Iterator<Item=TypedFieldIdent>>(field_iter: I) -> DataStore {
    //     let mut ds = DataStore::empty();
    //     for field in field_iter {
    //         ds.add_field(field);
    //     }
    //     ds
    // }

    /// Create a new `DataStore` with provided data. Data is provided in type-specific vectors of
    /// field identifiers along with data for the identifier.
    ///
    /// NOTE: This function provides no protection against field name collisions.
    // pub fn with_data<FI, U, S, T, B, F>(
    //     unsigned: U, signed: S, text: T, boolean: B, float: F
    //     ) -> DataStore
    //     where FI: Into<FieldIdent>,
    //           U: Into<Option<Vec<(FI, FieldData<u64>)>>>,
    //           S: Into<Option<Vec<(FI, FieldData<i64>)>>>,
    //           T: Into<Option<Vec<(FI, FieldData<String>)>>>,
    //           B: Into<Option<Vec<(FI, FieldData<bool>)>>>,
    //           F: Into<Option<Vec<(FI, FieldData<f64>)>>>,
    // {
    //     let mut ds = DataStore::empty();
    //     macro_rules! add_to_ds {
    //         ($($hm:tt; $fty:path)*) => {$({
    //             if let Some(src_h) = $hm.into() {
    //                 for (ident, data) in src_h {
    //                     let ident: FieldIdent = ident.into();
    //                     ds.add_field(TypedFieldIdent { ident: ident.clone(), ty: $fty });
    //                     ds.$hm.insert(ident, data.into());
    //                 }
    //             }
    //         })*}
    //     }
    //     add_to_ds!(
    //         unsigned; FieldType::Unsigned
    //         signed;   FieldType::Signed
    //         text;     FieldType::Text
    //         boolean;  FieldType::Boolean
    //         float;    FieldType::Float
    //     );
    //     ds
    // }

    // // Retrieve an unsigned integer field
    // pub(crate) fn get_unsigned_field(&self, ident: &FieldIdent) -> Option<&FieldData<u64>> {
    //     self.unsigned.get(ident)
    // }
    // // Retrieve a signed integer field
    // pub(crate) fn get_signed_field(&self, ident: &FieldIdent) -> Option<&FieldData<i64>> {
    //     self.signed.get(ident)
    // }
    // // Retrieve a string field
    // pub(crate) fn get_text_field(&self, ident: &FieldIdent) -> Option<&FieldData<String>> {
    //     self.text.get(ident)
    // }
    // // Retrieve a boolean field
    // pub(crate) fn get_boolean_field(&self, ident: &FieldIdent) -> Option<&FieldData<bool>> {
    //     self.boolean.get(ident)
    // }
    // // Retrieve a floating-point field
    // pub(crate) fn get_float_field(&self, ident: &FieldIdent) -> Option<&FieldData<f64>> {
    //     self.float.get(ident)
    // }


    pub fn fields<'a>(&'a self) -> impl Iterator<Item=&'a FieldIdent> {
        self.fields.iter().map(|ds_field| &ds_field.ident)
    }

    /// Returns `true` if this `DataStore` contains this field.
    pub fn has_field(&self, ident: &FieldIdent) -> bool {
        self.field_map.contains_key(ident)
    }

    /// Get the field information struct for a given field name
    pub fn get_field_type(&self, ident: &FieldIdent) -> Option<DTypes::DType> {
        self.field_map.get(ident)
            .and_then(|&index| self.fields.get(index).map(|&ref dsfield| dsfield.ty))
    }


    // pub(crate) fn get_field_data(&self, ident: &FieldIdent) -> Option<FieldData> {
    //     self.field_map.get(ident).and_then(|&field_idx| {
    //         match self.fields[field_idx].ty_ident.ty {
    //             FieldType::Unsigned => self.get_unsigned_field(ident)
    //                 .map(|data| FieldData::Unsigned(OwnedOrRef::Ref(data))),
    //             FieldType::Signed => self.get_signed_field(ident)
    //                 .map(|data| FieldData::Signed(OwnedOrRef::Ref(data))),
    //             FieldType::Text => self.get_text_field(ident)
    //                 .map(|data| FieldData::Text(OwnedOrRef::Ref(data))),
    //             FieldType::Boolean => self.get_boolean_field(ident)
    //                 .map(|data| FieldData::Boolean(OwnedOrRef::Ref(data))),
    //             FieldType::Float => self.get_float_field(ident)
    //                 .map(|data| FieldData::Float(OwnedOrRef::Ref(data))),
    //         }
    //     })
    // }

    /// Get the list of field information structs for this data store
    // pub fn fields(&self) -> Vec<&TypedFieldIdent> {
    //     self.fields.iter().map(|&ref s| &s.ty_ident).collect()
    // }
    /// Get the field names in this data store
    // pub fn fieldnames(&self) -> Vec<String> {
    //     self.fields.iter().map(|ref fi| fi.ty_ident.ident.to_string()).collect()
    // }

    /// Check if datastore is "homogenous": all columns (regardless of field type) are the same
    /// length
    // pub fn is_homogeneous(&self) -> bool {
    //     is_hm_homogeneous(&self.unsigned)
    //         .and_then(|x| is_hm_homogeneous_with(&self.signed, x))
    //         .and_then(|x| is_hm_homogeneous_with(&self.text, x))
    //         .and_then(|x| is_hm_homogeneous_with(&self.boolean, x))
    //         .and_then(|x| is_hm_homogeneous_with(&self.float, x))
    //         .is_some()
    // }
    /// Retrieve number of rows for this data store
    pub fn nrows(&self) -> usize
        where DTypes: AssocTypes,
              DTypes::Storage: MaxLen<DTypes>
    {
        self.data.max_len()
        // self.data.values().map(|value| value.nrows()).fold(0, |acc, l| max(acc, l))
        // [max_len(&self.unsigned), max_len(&self.signed), max_len(&self.text),
        //     max_len(&self.boolean), max_len(&self.float)].iter().fold(0, |acc, l| max(acc, *l))
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
        // let ident = ident.into();
        self.field_map
            .get(&ident)
            .ok_or(AgnesError::FieldNotFound(ident.clone()))
            .map(|&field_idx| &self.fields[field_idx])
            .and_then(|ds_field| {
                // if TypeId::of::<T>() != ds_field.ty {
                //     Err(AgnesError::TypeMismatch(format!(
                //         "Unexpected type for field '{}'", ident.clone())))
                // } else {
                    // by construction, td_index is always in range, so unwrap is safe
                    Ok(self.data.select_type().get(ds_field.td_index).unwrap())
                    // Ok(self.data[&ds_field.ty].get(ds_field.td_index))
                // }
            })
            .map(|field| OwnedOrRef::Ref(field) )
    }
}


// impl<'a, T: 'static + DataType> DIter<'a, T> for Selection<'a, DataStore> {
//     type DI = FieldData<T>;
//     fn diter(&'a self) -> Result<DataIterator<'a, T, FieldData<T>>> {
//         self.data.field_map
//             .get(&self.ident)
//             .ok_or(AgnesError::FieldNotFound(self.ident.clone()))
//             .map(|&field_idx| self.data.fields[field_idx])
//             .and_then(|ds_field| self.data.data[&ds_field.ty].data_iter(ds_field.ds_index))
//     }
// }
// impl<'a, T: 'static + DataType> DataIndex<T> for Selection<'a, DataStore> {
//     fn get_data(&self, idx: usize) -> Result<Value<&T>> {
        // self.data.field_map
        //     .get(&self.ident)
        //     .ok_or(AgnesError::FieldNotFound(self.ident.clone()))
        //     .map(|&field_idx| self.data.fields[field_idx])
        //     .and_then(|ds_field| self.data.data[&ds_field.ty].get(ds_field.ds_index))
        //     .and_then(|field| field.get_data(idx))

//     }
//     fn len(&self) -> usize {
//         self.data.field_map
//             .get(&self.ident)
//             .ok_or(AgnesError::FieldNotFound(self.ident.clone()))
//             .map(|&field_idx| self.data.fields[field_idx])
//             .map(|ds_field| self.data.data[&ds_field.ty].nrows())
//             .unwrap()
//     }

// }

// impl ApplyToElem for DataStore {
//     fn apply_to_elem<F: MapFn>(&self, f: &mut F, ident: &FieldIdent, idx: usize)
//         -> Result<F::Output>
//     {
//         self.field_map.get(ident)
//             .ok_or(AgnesError::FieldNotFound(ident.clone()))
//             .and_then(|&field_idx| {
//                 match self.fields[field_idx].ty_ident.ty {
//                     FieldType::Unsigned => self.get_unsigned_field(ident)
//                         .ok_or(AgnesError::FieldNotFound(ident.clone()))
//                         .and_then(|data| {
//                             data.apply(f, idx)
//                         }
//                     ),
//                     FieldType::Signed => self.get_signed_field(ident)
//                         .ok_or(AgnesError::FieldNotFound(ident.clone()))
//                         .and_then(|data| {
//                             data.apply(f, idx)
//                         }
//                     ),
//                     FieldType::Text => self.get_text_field(ident)
//                         .ok_or(AgnesError::FieldNotFound(ident.clone()))
//                         .and_then(|data| {
//                             data.apply(f, idx)
//                         }
//                     ),
//                     FieldType::Boolean => self.get_boolean_field(ident)
//                         .ok_or(AgnesError::FieldNotFound(ident.clone()))
//                         .and_then(|data| {
//                             data.apply(f, idx)
//                         }
//                     ),
//                     FieldType::Float => self.get_float_field(ident)
//                         .ok_or(AgnesError::FieldNotFound(ident.clone()))
//                         .and_then(|data| {
//                             data.apply(f, idx)
//                         }
//                     )
//                 }
//             }
//         )
//     }
// }
// impl FieldApplyTo for DataStore {
//     fn field_apply_to<F: FieldMapFn>(&self, f: &mut F, ident: &FieldIdent)
//         -> Result<F::Output>
//     {
//         self.field_map.get(ident)
//             .ok_or(AgnesError::FieldNotFound(ident.clone()))
//             .and_then(|&field_idx| {
//                 match self.fields[field_idx].ty_ident.ty {
//                     FieldType::Unsigned => self.get_unsigned_field(ident)
//                         .ok_or(AgnesError::FieldNotFound(ident.clone()))
//                         .map(|data| f.apply_unsigned(data)),
//                     FieldType::Signed => self.get_signed_field(ident)
//                         .ok_or(AgnesError::FieldNotFound(ident.clone()))
//                         .map(|data| f.apply_signed(data)),
//                     FieldType::Text => self.get_text_field(ident)
//                         .ok_or(AgnesError::FieldNotFound(ident.clone()))
//                         .map(|data| f.apply_text(data)),
//                     FieldType::Boolean => self.get_boolean_field(ident)
//                         .ok_or(AgnesError::FieldNotFound(ident.clone()))
//                         .map(|data| f.apply_boolean(data)),
//                     FieldType::Float => self.get_float_field(ident)
//                         .ok_or(AgnesError::FieldNotFound(ident.clone()))
//                         .map(|data| f.apply_float(data)),
//                 }
//             })
//     }
// }
// impl<'a> ApplyFieldReduce<'a> for Selection<'a, Arc<DataStore>> {
//     fn apply_field_reduce<F: FieldReduceFn<'a>>(&self, f: &mut F)
//         -> Result<F::Output>
//     {
//         self.data.get_field_data(&self.ident)
//             .ok_or(AgnesError::FieldNotFound(self.ident.clone()))
//             .map(|data| f.reduce(vec![data]))
//     }

// }
// impl<'a> ApplyFieldReduce<'a> for Vec<Selection<'a, Arc<DataStore>>> {
//     fn apply_field_reduce<F: FieldReduceFn<'a>>(&self, f: &mut F)
//         -> Result<F::Output>
//     {
//         self.iter().map(|selection| {
//             selection.data.get_field_data(&selection.ident)
//                 .ok_or(AgnesError::FieldNotFound(selection.ident.clone()))
//         }).collect::<Result<Vec<_>>>()
//             .map(|data_vec| f.reduce(data_vec))
//     }
// }

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
        let ident = ident.into();
        if !self.has_field(&ident) {
            self.add_empty_field::<T>(TFieldIdent::new(ident.clone()))?;
        }
        self.insert(&ident, value.into())
    }
}


// pub trait AddDtData<'a, DTypes>
//     where DTypes: AssociatedValue<'a>
// {
//     fn add(&mut self, ident: FieldIdent, value: DTypes::DtValue) -> Result<()>
//         where DTypes: DTypeList;
// }

// impl<'a, DTypes> AddDtData<'a, DTypes> for DataStore<DTypes>
//     where DTypes: AssociatedValue<'a>
// {
//     fn add(&mut self, ident: FieldIdent, value: DTypes::DtValue) -> Result<()>
//         where DTypes: DTypeList
//     {

//     }
// }

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
        // let ident = ident.into();
        // let &DsField { ty: ds_ty, ds_index, .. } = self.add_field::<T>(TFieldIdent::new(ident));
        // let target_btd = self.data
        //     .entry(ds_ty)
        //     .or_insert(BoxedTypeData::new(TypeData::<T>::new()));
        // for datum in data.drain(..) {
        //     target_btd.insert(ds_index, datum.into())
        //         .expect("Unexpected internal type mismatch error");
        //     // insert_value(&mut self.$hm, ident.clone(), datum);
        // }
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
        // let ident = ident.into();
        // let &DsField { ty: ds_ty, ds_index, .. } = self.add_field::<T>(TFieldIdent::new(ident));
        // let target_btd = self.data
        //     .entry(ds_ty)
        //     .or_insert(BoxedTypeData::new(TypeData::<T>::new()));
        // for datum in iter {
        //     target_btd.insert(ds_index, datum.into())
        //         .expect("Unexpected internal type mismatch error");
        // }
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
        self.into_datastore(ident).map(|ds| DataView::from(ds))
    }

    fn into_dv<I: Into<FieldIdent>>(self, ident: I) -> Result<DataView<DTypes>> where Self: Sized {
        self.into_dataview(ident)
    }
}

// macro_rules! impl_add_data {
//     ($($dtype:ty, $fty:path, $hm:tt);*) => {$(

// impl AddData<$dtype> for DataStore {
//     fn add(&mut self, ident: FieldIdent, value: Value<$dtype>) {
//         insert_value(&mut self.$hm, ident, value);
//     }
// }
// impl AddDataVec<$dtype> for DataStore {
//     fn add_data_vec(&mut self, ident: FieldIdent, mut data: Vec<Value<$dtype>>) {
//         self.add_field(TypedFieldIdent { ident: ident.clone(), ty: $fty });
//         for datum in data.drain(..) {
//             insert_value(&mut self.$hm, ident.clone(), datum);
//         }
//     }
// }
// impl AddDataFromIter<$dtype> for DataStore {
//     fn add_data_from_iter<I: Iterator<Item=Value<$dtype>>>(&mut self, ident: FieldIdent, iter: I)
//     {
//         self.add_field(TypedFieldIdent { ident: ident.clone(), ty: $fty });
//         for datum in iter {
//             insert_value(&mut self.$hm, ident.clone(), datum);
//         }
//     }
// }
// impl<'a> AddDataFromIter<&'a $dtype> for DataStore {
//     fn add_data_from_iter<I: Iterator<Item=Value<&'a $dtype>>>(&mut self, ident: FieldIdent,
//         iter: I)
//     {
//         self.add_field(TypedFieldIdent { ident: ident.clone(), ty: $fty });
//         for datum in iter {
//             insert_value(&mut self.$hm, ident.clone(), datum.cloned());
//         }
//     }
// }

//     )*}
// }
// impl_add_data!(
//     u64,    FieldType::Unsigned, unsigned;
//     i64,    FieldType::Signed,   signed;
//     String, FieldType::Text,     text;
//     bool,   FieldType::Boolean,  boolean;
//     f64,    FieldType::Float,    float
// );

// /// Trait for creating an object from a field identifier and a data structure with that field's
// /// data.
// pub trait FromData<D> {
//     /// Create a new `Self` from this data, using the specified field identifier.
//     fn from_data<I: Into<FieldIdent>>(ident: I, data: D) -> Self;
// }
// impl<'a, DTypes: DTypeList> FromData<FieldData<'a>> for DataStore<DTypes> {
//     fn from_data<I: Into<FieldIdent>>(ident: I, data: FieldData<'a>) -> DataStore<DTypes> {
//         let mut store = DataStore::empty();
//         let ident = ident.into();
//         // FieldData provides Value<&T>...we need to copy into this DataStore
//         macro_rules! copy_data_iter {
//             ($data:expr) => (DataIterator::new($data).map(|Value| Value.map(|val| val.clone())))
//         }
//         match data {
//             FieldData::Unsigned(ref data) =>
//                 store.add_data_from_iter(ident, copy_data_iter!(data)),
//             FieldData::Signed(ref data) =>
//                 store.add_data_from_iter(ident, copy_data_iter!(data)),
//             FieldData::Text(ref data) =>
//                 store.add_data_from_iter(ident, copy_data_iter!(data)),
//             FieldData::Boolean(ref data) =>
//                 store.add_data_from_iter(ident, copy_data_iter!(data)),
//             FieldData::Float(ref data) =>
//                 store.add_data_from_iter(ident, copy_data_iter!(data)),
//         }.unwrap(); // add_data_from_iter only fails if identifier collides, but DataStore was empty
//         store
//     }
// }

// fn max_len<K, T: DataType>(h: &HashMap<K, FieldData<T>>) -> usize where K: Eq + Hash {
//     h.values().fold(0, |acc, v| max(acc, v.len()))
// }
// fn is_hm_homogeneous<K, T: DataType>(h: &HashMap<K, FieldData<T>>) -> Option<usize>
//     where K: Eq + Hash
// {
//     let mut all_same_len = true;
//     let mut target_len = 0;
//     let mut first = true;
//     for (_, v) in h {
//         if first {
//             target_len = v.len();
//             first = false;
//         }
//         all_same_len &= v.len() == target_len;
//     }
//     if all_same_len { Some(target_len) } else { None }
// }
// fn is_hm_homogeneous_with<K, T: DataType>(h: &HashMap<K, FieldData<T>>, value: usize)
//     -> Option<usize> where K: Eq + Hash
// {
//     is_hm_homogeneous(h).and_then(|x| {
//         if x == 0 && value != 0 {
//             Some(value)
//         } else if (value == 0 && x != 0) || x == value {
//             Some(x)
//         } else { None }
//     })
// }
// fn insert_value<T: DataType>(
//     h: &mut HashMap<FieldIdent, FieldData<T>>,
//     k: FieldIdent,
//     v: Value<T>)
// {
//     h.entry(k).or_insert(FieldData::new()).push(v);
// }
