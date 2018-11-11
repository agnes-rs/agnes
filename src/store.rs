use std::fmt::Debug;
use std::marker::PhantomData;
use std::ops::Add;
use std::fmt;

use cons::*;
use fieldlist::{FieldCons, FieldPayloadCons, FieldTypes, Field, FieldPayload, FSelector};
use field::{Value, FieldData};
use select::{SelectField};
use access::{OwnedOrRef};

#[derive(Debug)]
pub struct DataStore<Fields: AssocStorage> {
    data: Fields::Storage,
}

pub type StorageCons<Field, Tail>
    = FieldPayloadCons<Field, FieldData<<Field as FieldTypes>::DType>, Tail>;

pub trait AssocStorage {
    type Storage: Debug;
}
// impl<Field, FIdx, DType, Tail> AssocStorage for StorageCons<Field, FIdx, DType, Tail> {}
impl<Ident, DType, Tail> AssocStorage for FieldCons<Ident, DType, Tail>
    where Tail: AssocStorage,
          Ident: Debug,
          DType: Debug,
{
    type Storage = StorageCons<Field<Ident, DType>, Tail::Storage>;
}
impl AssocStorage for Nil {
    type Storage = Nil;
}


// NEXT: do I need a new selection type for StorageCons?

impl<Fields> DataStore<Fields>
    where Fields: AssocStorage
{
    /// Generate and return an empty data store
    pub fn empty() -> DataStore<Nil> {
        DataStore {
            data: Nil,
        }
    }
}

// ways to add:
// - add_field(FieldData) -> DataStore<...>
// - add_field_from_iter(Iterator<Item=T>) -> DataStore<...>
// - new_field::<Field>() -> DataStore<...>
// - field_mut::<Field>() -> DataIndexMut<Item=T>

pub trait AddField<NewIdent, NewDType> {
    type OutputFields: AssocStorage;

    fn add_field(self, data: FieldData<NewDType>)
        -> DataStore<Self::OutputFields>;
}
impl<PrevFields, NewIdent, NewDType> AddField<NewIdent, NewDType> for DataStore<PrevFields>
    where PrevFields: AssocStorage + FieldTypes,
          NewIdent: Debug,
          NewDType: Debug,
{
    type OutputFields = FieldCons<NewIdent, NewDType, PrevFields>;

    fn add_field(self, data: FieldData<NewDType>)
        -> DataStore<Self::OutputFields>
    {
        DataStore {
            data: StorageCons{
                head: data.into(),
                tail: self.data
            }
        }
    }
}

pub trait AddFieldFromIter<NewIdent, NewDType> {
    type OutputFields: AssocStorage;

    fn add_field_from_iter<IntoIter, Iter>(self, iter: IntoIter)
        -> DataStore<Self::OutputFields>
        where Iter: Iterator<Item=Value<NewDType>>,
              IntoIter: IntoIterator<IntoIter=Iter, Item=Value<NewDType>>;
}
impl<PrevFields, NewIdent, NewDType> AddFieldFromIter<NewIdent, NewDType> for DataStore<PrevFields>
    where PrevFields: AssocStorage,
          NewIdent: Debug, NewDType: Debug,
{
    type OutputFields = FieldCons<NewIdent, NewDType, PrevFields>;

    fn add_field_from_iter<IntoIter, Iter>(self, iter: IntoIter)
        -> DataStore<Self::OutputFields>
        where Iter: Iterator<Item=Value<NewDType>>,
              IntoIter: IntoIterator<IntoIter=Iter, Item=Value<NewDType>>
    {
        DataStore {
            data: StorageCons {
                head: iter.into_iter().collect::<FieldData<NewDType>>().into(),
                tail: self.data
            }
        }
    }
}

pub trait AddEmptyField<NewIdent, NewDType> {
    type OutputFields: AssocStorage;

    fn add_empty_field(self)
        -> DataStore<Self::OutputFields>;
}
impl<PrevFields, NewIdent, NewDType> AddEmptyField<NewIdent, NewDType> for DataStore<PrevFields>
    where PrevFields: AssocStorage,
          NewIdent: Debug, NewDType: Debug,
{
    type OutputFields = FieldCons<NewIdent, NewDType, PrevFields>;

    fn add_empty_field(self)
        -> DataStore<Self::OutputFields>
    {
        DataStore {
            data: StorageCons {
                head: FieldData::default().into(),
                tail: self.data
            }
        }
    }
}

impl<PrevFields> DataStore<PrevFields>
    where PrevFields: AssocStorage + FieldTypes,
{
    pub fn add_field<NewIdent, NewDType>(self, data: FieldData<NewDType>)
        -> DataStore<<Self as AddField<NewIdent, NewDType>>::OutputFields>
        // -> DataStore<StorageCons<Field<NewIdent, Add1<PrevFields::FIdx>, NewDType>, PrevFields>>
        where NewDType: fmt::Debug,
              Self: AddField<NewIdent, NewDType>
    {
        AddField::add_field(self, data)
    }

    pub fn add_field_from_iter<NewIdent, NewDType, IntoIter, Iter>(self, iter: IntoIter)
        // -> DataStore<StorageCons<Field<NewIdent, Add1<PrevFields::FIdx>, NewDType>, PrevFields>>
        -> DataStore<<Self as AddFieldFromIter<NewIdent, NewDType>>::OutputFields>
        where Iter: Iterator<Item=Value<NewDType>>,
              IntoIter: IntoIterator<IntoIter=Iter, Item=Value<NewDType>>,
              NewDType: fmt::Debug + Default + Clone,
              Self: AddFieldFromIter<NewIdent, NewDType>
    {
        AddFieldFromIter::add_field_from_iter(self, iter)
    }

    pub fn add_empty_field<NewIdent, NewDType>(self)
        // -> DataStore<StorageCons<Field<NewIdent, Add1<PrevFields::FIdx>, NewDType>, PrevFields>>
        -> DataStore<<Self as AddEmptyField<NewIdent, NewDType>>::OutputFields>
        where NewDType: fmt::Debug,
              Self: AddEmptyField<NewIdent, NewDType>
    {
        AddEmptyField::add_empty_field(self)
    }
}

impl<Fields> DataStore<Fields>
    where Fields: AssocStorage
{
    pub fn field<'a, Ident, FieldSearcher>(&'a self)
        -> OwnedOrRef<'a, <Fields as FSelector<Ident, FieldSearcher>>::DType>
        where Fields: FSelector<Ident, FieldSearcher>
    {
        self.data.select_field()
    }
}

// impl<'a, Fields, Ident, FIdx> SelectField<'a, Ident>
//     for DataStore<Fields>
//     where Fields: FSelector<Ident, FIdx>
// {
//     type Output = OwnedOrRef<'a, <Fields as FSelector<Ident, FIdx>>::DType>;

//     fn select_field(&'a self) -> Self::Output
//     {
//         self.data.select_field()
//     }
// }






// pub trait AddField<NewField, NewDType, PrevFields>
//     where PrevFields: AssocStorage + FieldIndex,
//           PrevFields::FIdx: Add<B1>,
// {
//     fn add(self, field: FieldData<NewDType>)
//         -> DataStore<FieldCons<NewField, Add1<PrevFields::FIdx>, NewDType, PrevFields>>
//         where NewDType: fmt::Debug;
// }

// impl<NewField, NewDType, PrevFields>
//     AddField<NewField, NewDType, PrevFields>
//     for DataStore<PrevFields>
//     where PrevFields: AssocStorage + FieldIndex,
//           PrevFields::FIdx: Add<B1>
// {
//     fn add(self, field: FieldData<NewDType>)
//         -> DataStore<FieldCons<NewField, Add1<PrevFields::FIdx>, NewDType, PrevFields>>
//         where NewDType: fmt::Debug
//     {
//         DataStore {
//             data: StorageCons {
//                 head: field,
//                 tail: self.data
//             }
//         }
//     }
// }

// impl<Fields> DataStore<Fields>
//     where Fields: AssocStorage,
// {
//     pub fn map<Field, T, F, FOut>(&self, f: F) -> FOut
//         where F: FnOnce(&Vec<Option<T>>) -> FOut,
//               Fields::Storage: Map<Fields, T, F, FOut>
//     {
//         self.data.map(f)
//     }
// }

// pub trait Map<Fields, T, F, FOut>
//     where F: FnOnce(&Vec<Option<T>>) -> FOut
// {
//     fn map(&self, f: F) -> FOut;
// }
// impl<Fields, T, F, FOut, Tail> Map<Fields, T, F, FOut> for StorageCons<Vec<Option<T>>, Tail>
//     where F: FnOnce(&Vec<Option<T>>) -> FOut,
//           Fields: AssocStorage
// {
//     fn map(&self, f: F) -> FOut {
//         f(&self.head)
//     }
// }


#[cfg(test)]
mod tests {

    use std::fmt::Debug;
    use std::path::Path;

    use csv_sniffer::metadata::Metadata;

    use source::csv::{CsvSource, CsvReader, IntoCsvSrcSpec};
    use field::Value;
    use fieldlist::FSelector;
    // use data_types::csv::*;
    use super::DataStore;
    use cons::*;

    fn load_csv_file<Spec>(filename: &str, spec: Spec)
        -> (CsvReader<Spec::CsvSrcSpec>, Metadata)
        where Spec: IntoCsvSrcSpec
        // where CsvSrcSpec: FromSpec<Spec>
        // where Spec: Debug// + FieldSpecs<Types> + AssocFields + AttachSrcPos
    {
        let data_filepath = Path::new(file!()) // start as this file
            .parent().unwrap()                 // navigate up to src directory
            .parent().unwrap()                 // navigate up to root directory
            .join("tests")                     // navigate into integration tests directory            .join("data")                      // navigate into data directory
            .join("data")                      // navigate into data directory
            .join(filename);                   // navigate to target file

        let source = CsvSource::new(data_filepath.into()).unwrap();
        // let csv_rdr = CsvReader::new(&source, spec);
        // csv_rdr.adsjfiaosj();
        (CsvReader::new(&source, spec).unwrap(), source.metadata().clone())
    }

    #[test]
    fn storage_create() {
        let ds = DataStore::<Nil>::empty();
        #[derive(Debug)]
        struct Test;
        let ds = ds.add_field_from_iter::<Test, _, _, _>(
            vec![Value::Exists(4u64), Value::Exists(1), Value::Na, Value::Exists(3),
                 Value::Exists(7), Value::Exists(8), Value::Na]
        );
        println!("{:?}", ds);

        spec![
            let gdp_spec = {
                CountryName("Country Name"): String,
                CountryCode("Country Code"): String,
                Year1983("1983"): f64,
            };
        ];
        // println!("{:?}", gdp_spec);

        // gdp_spec.tail.tail.head.ajdfiaoj();

        // ds.adjiaofj();
        let (mut csv_rdr, _metadata) = load_csv_file("gdp.csv", gdp_spec);
        let ds = csv_rdr.read().unwrap();

        println!("{:?}", ds);
        println!("{:?}", ds.data.select::<CountryName, _>());
        // println!("{:?}", csv_rdr);
        // println!("{:?}", metadata);


    }
}
