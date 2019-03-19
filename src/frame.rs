/*!
Structs and implementation for row-selecting data structure.

A [DataFrame](struct.DataFrame.html) is a reference to an underlying
[DataStore](../store/struct.DataStore.html) along with record-based filtering and sorting details.
*/

use std::marker::PhantomData;

#[cfg(feature = "serialize")]
use serde::ser::{Serialize, SerializeSeq, Serializer};
use std::fmt::Debug;
use std::rc::Rc;
use std::sync::Arc;

use typenum::UTerm;

use access::DataIndex;
use cons::Nil;
use error;
use field::{FieldData, Value};
use fieldlist::FieldCons;
use label::*;
use permute::{self, UpdatePermutation};
use select::{FieldSelect, SelectFieldByLabel};
use store::{AssocFrameLookup, AssocStorage, DataRef, DataStore, IntoView, NRows};
use view::{DataView, ViewFrameCons};

type Permutation = permute::Permutation<Vec<usize>>;

/// Type alias for label-only cons-list
pub type StoreFieldCons<L, T> = LCons<L, T>;
/// A marker struct for a frame type (Single, Melt, etc.) and the list of fields used with this
/// type (a [StoreFieldCons](type.StoreFieldCons.html) / [LabelCons](../label/type.LabelCons.html)).
pub struct StoreFieldMarkers<FrameType, StoreFieldList> {
    _marker: PhantomData<(FrameType, StoreFieldList)>,
}
// `FrameLabel` is a label struct. `StoreFields` is a `StoreFieldMarkers` struct.
type FieldLookupCons<FrameLabel, StoreFields, Tail> = LMCons<FrameLabel, StoreFields, Tail>;

/// [StoreFieldMarkers](struct.StoreFieldMarkers.html) `FrameType` for typical single-source fields
pub struct Single;
/// [StoreFieldMarkers](struct.StoreFieldMarkers.html) `FrameType` for multi-source fields
/// constructed by a 'melt' call.
pub struct Melt;

/// Trait for computing the `FrameFields` [FieldLookupCons](type.FieldLookupCons.html) cons-list
/// for a standard [DataFrame](struct.DataFrame.html) (where all labels simple pass through with
/// calls to the underlying [DataStore](../store/struct.DataStore.html)).
pub trait SimpleFrameFields {
    /// The computed `FrameFields` [FieldLookupCons](type.FieldLookupCons.html) cons-list.
    type Fields;
}
impl SimpleFrameFields for Nil {
    type Fields = Nil;
}
impl<Label, DType, Tail> SimpleFrameFields for FieldCons<Label, DType, Tail>
where
    Tail: SimpleFrameFields,
{
    type Fields = FieldLookupCons<
        Label,
        StoreFieldMarkers<Single, Labels![Label]>,
        <Tail as SimpleFrameFields>::Fields,
    >;
}

/// A data frame. A `DataStore` reference along with record-based filtering and sorting details.
/// `FrameFields` is a [FieldLookupCons](type.FieldLookupCons.html) cons-list which maps a single
/// label to one or more `DataStore` labels.
#[derive(Debug, Clone)]
pub struct DataFrame<FrameFields, StoreFields>
where
    StoreFields: AssocStorage,
    StoreFields::Storage: Debug,
{
    permutation: Rc<Permutation>,
    fields: PhantomData<FrameFields>,
    store: Arc<DataStore<StoreFields>>,
}
impl<FrameFields, StoreFields> DataFrame<FrameFields, StoreFields>
where
    StoreFields: AssocStorage,
    DataStore<StoreFields>: NRows,
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
impl<FrameFields, StoreFields> NRows for DataFrame<FrameFields, StoreFields>
where
    StoreFields: AssocStorage,
    DataStore<StoreFields>: NRows,
{
    fn nrows(&self) -> usize {
        self.len()
    }
}
#[cfg(test)]
pub trait StoreRefCount {
    fn store_ref_count(&self) -> usize;
}
#[cfg(test)]
impl<FrameFields, StoreFields> StoreRefCount for DataFrame<FrameFields, StoreFields>
where
    StoreFields: AssocStorage,
{
    fn store_ref_count(&self) -> usize {
        Arc::strong_count(&self.store)
    }
}
impl<FrameFields, StoreFields> UpdatePermutation for DataFrame<FrameFields, StoreFields>
where
    StoreFields: AssocStorage,
{
    fn update_permutation(&mut self, new_permutation: &[usize]) {
        Rc::make_mut(&mut self.permutation).update_indices(new_permutation);
    }
}

impl<StoreFields> From<DataStore<StoreFields>>
    for DataFrame<<StoreFields as SimpleFrameFields>::Fields, StoreFields>
where
    StoreFields: AssocStorage + SimpleFrameFields,
{
    fn from(
        store: DataStore<StoreFields>,
    ) -> DataFrame<<StoreFields as SimpleFrameFields>::Fields, StoreFields> {
        DataFrame {
            permutation: Rc::new(Permutation::default()),
            fields: PhantomData,
            store: Arc::new(store),
        }
    }
}

impl<FrameFields, StoreFields> IntoView for DataFrame<FrameFields, StoreFields>
where
    StoreFields: AssocStorage,
    FrameFields: AssocFrameLookup,
{
    type Labels = <FrameFields as AssocFrameLookup>::Output;
    type Frames = ViewFrameCons<UTerm, Self, Nil>;
    type Output = DataView<Self::Labels, Self::Frames>;

    fn into_view(self) -> Self::Output {
        DataView::new(ViewFrameCons {
            head: self.into(),
            tail: Nil,
        })
    }
}

impl<StoreFields> DataFrame<<StoreFields as SimpleFrameFields>::Fields, StoreFields>
where
    StoreFields: AssocStorage + SimpleFrameFields,
    DataStore<StoreFields>: NRows,
{
    pub(crate) fn from_repeated_store(
        store: DataStore<StoreFields>,
        reps: usize,
    ) -> DataFrame<<StoreFields as SimpleFrameFields>::Fields, StoreFields> {
        DataFrame {
            permutation: {
                //TODO: replace with slice.repeat() call when stabilized
                let mut v = Vec::with_capacity(store.nrows() * reps);
                for _ in 0..reps {
                    v.extend(0..store.nrows());
                }
                Rc::new(v.into())
            },
            fields: PhantomData,
            store: Arc::new(store),
        }
    }
}

/// Allow `DataFrame`s to be pulled from `LVCons` as `Value`s
impl<FrameFields, StoreFields> SelfValued for DataFrame<FrameFields, StoreFields> where
    StoreFields: AssocStorage
{
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
enum FrameKind<DI> {
    Single(DI),
    Melt(Vec<DI>),
}

impl<DI> FrameKind<DI>
where
    DI: DataIndex,
{
    fn nfields(&self) -> usize {
        match *self {
            FrameKind::Single(_) => 1,
            FrameKind::Melt(ref fields) => fields.len(),
        }
    }

    fn nrows(&self) -> usize {
        assert!(!self.is_empty());
        match *self {
            FrameKind::Single(ref field) => field.len(),
            FrameKind::Melt(ref fields) => fields[0].len(),
        }
    }

    fn is_empty(&self) -> bool {
        self.nfields() == 0
    }
}

/// Structure to hold references to a data structure (e.g. DataStore) and a frame used to view
/// that structure. Provides DataIndex for the underlying data structure, as viewed through the
/// frame.
#[derive(Debug, Hash, PartialEq, Eq)]
pub struct Framed<T, DI> {
    permutation: Rc<Permutation>,
    data: FrameKind<DI>,
    _ty: PhantomData<T>,
}
impl<T, DI> Framed<T, DI> {
    /// Create a new framed view of some data, as viewed through a particular `DataFrame`.
    pub fn new(permutation: Rc<Permutation>, data: DI) -> Framed<T, DI> {
        Framed {
            permutation,
            data: FrameKind::Single(data),
            _ty: PhantomData,
        }
    }
}
impl<T, DI> Clone for Framed<T, DI>
where
    DI: Clone,
{
    fn clone(&self) -> Framed<T, DI> {
        Framed {
            permutation: Rc::clone(&self.permutation),
            data: self.data.clone(),
            _ty: PhantomData,
        }
    }
}
impl<T> From<DataRef<T>> for Framed<T, DataRef<T>> {
    fn from(orig: DataRef<T>) -> Framed<T, DataRef<T>> {
        Framed {
            permutation: Rc::new(Permutation::default()),
            data: FrameKind::Single(orig),
            _ty: PhantomData,
        }
    }
}
impl<T> From<FieldData<T>> for Framed<T, DataRef<T>> {
    fn from(orig: FieldData<T>) -> Framed<T, DataRef<T>> {
        Framed {
            permutation: Rc::new(Permutation::default()),
            data: FrameKind::Single(orig.into()),
            _ty: PhantomData,
        }
    }
}

impl<T, DI> DataIndex for Framed<T, DI>
where
    T: Debug,
    DI: DataIndex<DType = T> + Debug,
{
    type DType = T;

    fn get_datum(&self, idx: usize) -> error::Result<Value<&T>> {
        assert!(!self.data.is_empty());
        match self.data {
            FrameKind::Single(ref field) => field.get_datum(self.permutation.map_index(idx)),
            FrameKind::Melt(ref fields) => {
                // when we have multiple fields in this Framed struct, we loop through through the
                // first record in each field, then the second, and so on....
                let nfields = self.data.nfields();
                fields[idx % nfields].get_datum(self.permutation.map_index(idx / nfields))
            }
        }
    }
    fn len(&self) -> usize {
        assert!(!self.data.is_empty());
        // nfields * nrows
        self.data.nfields() * self.permutation.len().unwrap_or(self.data.nrows())
    }
}

#[cfg(feature = "serialize")]
impl<T, DI> Serialize for Framed<T, DI>
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

/// Trait for selecting a field associated with the label `Label` from a type, as mediated through
/// the framing information in `FrameFields`.
pub trait SelectFieldFromStore<FrameFields, Label> {
    /// The resultant data type of the field.
    type DType: Debug;
    /// The field accessor type.
    type Output: DataIndex<DType = Self::DType>;

    /// Returns an accessor (implementing [DataIndex](../access/trait.DataIndex.html)) for the
    /// selected field.
    fn select_field(&self) -> Self::Output;
}

impl<FrameFields, StoreFields, Label, FirstLabel, Tail> SelectFieldFromStore<FrameFields, Label>
    for DataStore<StoreFields>
where
    FrameFields: LookupElemByLabel<Label>,
    ElemOf<FrameFields, Label>:
        Marked<Marker = StoreFieldMarkers<Single, StoreFieldCons<FirstLabel, Tail>>>,
    StoreFields: AssocStorage,
    StoreFields::Storage: LookupElemByLabel<FirstLabel>,
    ElemOf<StoreFields::Storage, FirstLabel>: Typed,
    ElemOf<StoreFields::Storage, FirstLabel>:
        Valued<Value = DataRef<TypeOfElemOf<StoreFields::Storage, FirstLabel>>>,
    TypeOf<ElemOf<StoreFields::Storage, FirstLabel>>: Debug,
{
    type DType = TypeOf<ElemOf<StoreFields::Storage, FirstLabel>>;
    type Output = DataRef<TypeOf<ElemOf<StoreFields::Storage, FirstLabel>>>;

    fn select_field(&self) -> Self::Output {
        DataRef::clone(&self.field::<FirstLabel>())
    }
}

impl<FrameFields, StoreFields, Label> SelectFieldByLabel<Label>
    for DataFrame<FrameFields, StoreFields>
where
    DataStore<StoreFields>: SelectFieldFromStore<FrameFields, Label>,
    StoreFields: AssocStorage,
{
    type Output = Framed<
        <DataStore<StoreFields> as SelectFieldFromStore<FrameFields, Label>>::DType,
        <DataStore<StoreFields> as SelectFieldFromStore<FrameFields, Label>>::Output,
    >;

    fn select_field(&self) -> Self::Output {
        Framed::new(
            Rc::clone(&self.permutation),
            SelectFieldFromStore::<FrameFields, Label>::select_field(&*self.store),
        )
    }
}

impl<FrameFields, StoreFields> FieldSelect for DataFrame<FrameFields, StoreFields> where
    StoreFields: AssocStorage
{
}

#[cfg(test)]
mod tests {

    use std::path::Path;

    use csv_sniffer::metadata::Metadata;
    use serde_json;

    use super::*;

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
        let framed: Framed<f64, _> = field.into();
        assert_eq!(
            serde_json::to_string(&framed).unwrap(),
            "[5.0,3.4,-1.3,5.2,6.0,-126.9]"
        );
        println!("{}", serde_json::to_string(&framed).unwrap());
    }

    tablespace![
        pub table order {
            Name: String,
        }
    ];

    #[test]
    fn repeated() {
        let field: FieldData<String> = vec!["First", "Second", "Third"]
            .iter()
            .map(|&s| s.to_owned())
            .collect();
        let store = DataStore::<Nil>::empty().push_back_field::<order::Name, _>(field);
        let frame = DataFrame::from_repeated_store(store, 5);
        assert_eq!(
            frame.field::<order::Name>().to_vec(),
            vec![
                "First", "Second", "Third", "First", "Second", "Third", "First", "Second", "Third",
                "First", "Second", "Third", "First", "Second", "Third",
            ]
        );

        let dv = frame.into_view();
        println!("{}", dv);
        assert_eq!(
            dv.field::<order::Name>().to_vec(),
            vec![
                "First", "Second", "Third", "First", "Second", "Third", "First", "Second", "Third",
                "First", "Second", "Third", "First", "Second", "Third",
            ]
        );
    }
}
