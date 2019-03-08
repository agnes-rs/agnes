/*!
Structs and implementation for row-selecting data structure.

A [DataFrame](struct.DataFrame.html) is a reference to an underlying
[DataStore](../store/struct.DataStore.html) along with record-based filtering and sorting details.
*/

#[cfg(feature = "serialize")]
use serde::ser::{Serialize, SerializeSeq, Serializer};
use std::fmt::Debug;
use std::rc::Rc;
use std::sync::Arc;

use access::{self, DataIndex};
use error;
use field::{FieldData, Value};
use label::{ElemOf, LookupElemByLabel, TypeOf, TypeOfElemOf, Typed, Valued};
use select::{FieldSelect, SelectFieldByLabel};
use store::{AssocStorage, DataRef, DataStore, NRows};

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
    /// Returns length (number of rows) in this `DataFrame`.
    pub fn len(&self) -> usize {
        match self.permutation.len() {
            Some(len) => len,
            None => self.store.nrows(),
        }
    }
    /// Returns whether or not this `DataFrame` is empty.
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
    // /// Returns `true` if this `DataFrame` contains this field.
    // pub fn has_field(&self, s: &FieldIdent) -> bool {
    //     self.store.has_field(s)
    // }
    pub(crate) fn update_permutation(&mut self, new_permutation: &[usize]) {
        Rc::make_mut(&mut self.permutation).update(new_permutation);
    }
}

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
#[derive(Debug, Hash, PartialEq, Eq)]
pub struct Framed<T> {
    permutation: Rc<Permutation>,
    data: DataRef<T>,
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
impl<T> From<DataRef<T>> for Framed<T> {
    fn from(orig: DataRef<T>) -> Framed<T> {
        Framed {
            permutation: Rc::new(Permutation::default()),
            data: orig,
        }
    }
}
impl<T> From<FieldData<T>> for Framed<T> {
    fn from(orig: FieldData<T>) -> Framed<T> {
        Framed {
            permutation: Rc::new(Permutation::default()),
            data: orig.into(),
        }
    }
}

impl<T> DataIndex for Framed<T>
where
    T: Debug,
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
    }
}

#[cfg(feature = "serialize")]
impl<T> Serialize for Framed<T>
where
    T: Serialize,
    Self: DataIndex<DType = T>,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut seq = serializer.serialize_seq(Some(self.len()))?;
        for elem in self.iter() {
            seq.serialize_element(&elem)?;
        }
        seq.end()
    }
}

impl<Fields, Label> SelectFieldByLabel<Label> for DataFrame<Fields>
where
    Fields: AssocStorage + Debug,
    Fields::Storage: LookupElemByLabel<Label> + NRows,
    ElemOf<Fields::Storage, Label>: Typed,
    ElemOf<Fields::Storage, Label>: Valued<Value = DataRef<TypeOfElemOf<Fields::Storage, Label>>>,
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

#[cfg(test)]
mod tests {

    use std::path::Path;

    use csv_sniffer::metadata::Metadata;
    use serde_json;

    use super::*;

    use select::FieldSelect;
    use source::csv::{CsvReader, CsvSource, IntoCsvSrcSchema};

    fn load_csv_file<Schema>(
        filename: &str,
        schema: Schema,
    ) -> (CsvReader<Schema::CsvSrcSchema>, Metadata)
    where
        Schema: IntoCsvSrcSchema,
        <Schema as IntoCsvSrcSchema>::CsvSrcSchema: Debug,
    {
        let data_filepath = Path::new(file!()) // start as this file
            .parent()
            .unwrap() // navigate up to src directory
            .parent()
            .unwrap() // navigate up to root directory
            .join("tests") // navigate into integration tests directory
            .join("data") // navigate into data directory
            .join(filename); // navigate to target file

        let source = CsvSource::new(data_filepath).unwrap();
        (
            CsvReader::new(&source, schema).unwrap(),
            source.metadata().clone(),
        )
    }

    tablespace![
        pub table gdp {
            CountryName: String,
            CountryCode: String,
            Year1983: f64
        }
    ];

    #[test]
    fn frame_select() {
        let gdp_schema = schema![
            fieldname gdp::CountryName = "Country Name";
            fieldname gdp::CountryCode = "Country Code";
            fieldname gdp::Year1983 = "1983";
        ];

        let (mut csv_rdr, _metadata) = load_csv_file("gdp.csv", gdp_schema.clone());
        let ds = csv_rdr.read().unwrap();

        let frame = DataFrame::from(ds);
        println!("{:?}", frame.field::<gdp::CountryName>());
    }

    #[test]
    fn framed_serialize() {
        let field: FieldData<f64> = vec![5.0f64, 3.4, -1.3, 5.2, 6.0, -126.9].into();
        let framed: Framed<f64> = field.into();
        assert_eq!(
            serde_json::to_string(&framed).unwrap(),
            "[5.0,3.4,-1.3,5.2,6.0,-126.9]"
        );
        println!("{}", serde_json::to_string(&framed).unwrap());
    }
}
