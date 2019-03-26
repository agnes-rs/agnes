/*!
Structs and implementation for row-selecting data structure.

A [DataFrame](struct.DataFrame.html) is a reference to an underlying
[DataStore](../store/struct.DataStore.html) along with record-based filtering and sorting details.
*/

use std::collections::VecDeque;
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
// `FrameLabel` is a label struct. `StoreDetails` is a `StoreFieldMarkers` struct.
type FieldLookupCons<FrameLabel, StoreDetails, Tail> = LMCons<FrameLabel, StoreDetails, Tail>;

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

    /// Create a new framed view of some data, rotating over data in a `Vec` of `DataIndex` objects.
    pub fn new_melt(permutation: Rc<Permutation>, data: Vec<DI>) -> Framed<T, DI> {
        Framed {
            permutation,
            data: FrameKind::Melt(data),
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

/// Trait for selecting a field associated with the label `Label` from the fields in `StoreFields`.
pub trait SelectAndFrame<Label, StoreFields>
where
    StoreFields: AssocStorage,
{
    /// The resultant data type of the field.
    type DType: Debug;
    /// The field accessor type.
    type Field: DataIndex<DType = Self::DType>;

    /// Returns an [Framed](struct.Framed.html) struct accessing the selected field.
    fn select_and_frame(
        perm: &Rc<Permutation>,
        store: &DataStore<StoreFields>,
    ) -> Framed<Self::DType, Self::Field>;
}

/// Helper trait for selecting and framing fields. Used by
/// [SelectAndFrame](trait.SelectAndFrame.html). `Label` is the label to select, `StoreFields` is
/// the set of fields the data is stored in, and `Match` is whether or not `Label` matches the head
/// label in this type.
pub trait SelectAndFrameMatch<Label, StoreFields, Match>
where
    StoreFields: AssocStorage,
{
    /// The resultant data type of the field.
    type DType: Debug;
    /// The field accessor type.
    type Field: DataIndex<DType = Self::DType>;

    /// Returns an [Framed](struct.Framed.html) struct accessing the selected field.
    fn select_and_frame(
        perm: &Rc<Permutation>,
        store: &DataStore<StoreFields>,
    ) -> Framed<Self::DType, Self::Field>;
}

impl<TargetLabel, FrameLabel, StoreDetails, Tail, StoreFields>
    SelectAndFrame<TargetLabel, StoreFields> for FieldLookupCons<FrameLabel, StoreDetails, Tail>
where
    TargetLabel: LabelEq<FrameLabel>,
    StoreFields: AssocStorage,
    FieldLookupCons<FrameLabel, StoreDetails, Tail>:
        SelectAndFrameMatch<TargetLabel, StoreFields, <TargetLabel as LabelEq<FrameLabel>>::Eq>,
{
    type DType = <FieldLookupCons<FrameLabel, StoreDetails, Tail> as SelectAndFrameMatch<
        TargetLabel,
        StoreFields,
        <TargetLabel as LabelEq<FrameLabel>>::Eq,
    >>::DType;
    type Field = <FieldLookupCons<FrameLabel, StoreDetails, Tail> as SelectAndFrameMatch<
        TargetLabel,
        StoreFields,
        <TargetLabel as LabelEq<FrameLabel>>::Eq,
    >>::Field;

    fn select_and_frame(
        perm: &Rc<Permutation>,
        store: &DataStore<StoreFields>,
    ) -> Framed<Self::DType, Self::Field> {
        <Self as SelectAndFrameMatch<
            TargetLabel,
            StoreFields,
            <TargetLabel as LabelEq<FrameLabel>>::Eq,
        >>::select_and_frame(perm, store)
    }
}

impl<TargetLabel, FrameLabel, StoreFieldList, Tail, StoreFields>
    SelectAndFrameMatch<TargetLabel, StoreFields, True>
    for FieldLookupCons<FrameLabel, StoreFieldMarkers<Single, StoreFieldList>, Tail>
where
    StoreFields: AssocStorage,
    StoreFields::Storage: LookupElemByLabel<TargetLabel>,
    ElemOf<StoreFields::Storage, TargetLabel>: Typed,
    TypeOf<ElemOf<StoreFields::Storage, TargetLabel>>: Debug,
    ElemOf<StoreFields::Storage, TargetLabel>:
        Valued<Value = DataRef<TypeOfElemOf<StoreFields::Storage, TargetLabel>>>,
{
    type DType = TypeOf<ElemOf<StoreFields::Storage, TargetLabel>>;
    type Field = DataRef<TypeOf<ElemOf<StoreFields::Storage, TargetLabel>>>;

    fn select_and_frame(
        perm: &Rc<Permutation>,
        store: &DataStore<StoreFields>,
    ) -> Framed<Self::DType, Self::Field> {
        Framed::new(
            Rc::clone(perm),
            DataRef::clone(&store.field::<TargetLabel>()),
        )
    }
}

impl<TargetLabel, FrameLabel, StoreFieldList, Tail, StoreFields>
    SelectAndFrameMatch<TargetLabel, StoreFields, True>
    for FieldLookupCons<FrameLabel, StoreFieldMarkers<Melt, StoreFieldList>, Tail>
where
    StoreFields: AssocStorage,
    StoreFields::Storage: LookupElemByLabel<TargetLabel>,
    ElemOf<StoreFields::Storage, TargetLabel>: Typed,
    TypeOf<ElemOf<StoreFields::Storage, TargetLabel>>: Debug,
    StoreFieldList: RotateFields<StoreFields, TypeOf<ElemOf<StoreFields::Storage, TargetLabel>>>,
{
    type DType = TypeOf<ElemOf<StoreFields::Storage, TargetLabel>>;
    type Field = DataRef<TypeOf<ElemOf<StoreFields::Storage, TargetLabel>>>;

    fn select_and_frame(
        perm: &Rc<Permutation>,
        store: &DataStore<StoreFields>,
    ) -> Framed<Self::DType, Self::Field> {
        let melt_rotation =
            <StoreFieldList as RotateFields<StoreFields, Self::DType>>::add_to_rotation(store);
        Framed::new_melt(
            Rc::clone(perm),
            melt_rotation.iter().cloned().collect::<Vec<_>>(),
        )
    }
}

impl<TargetLabel, FrameLabel, StoreDetails, Tail, StoreFields>
    SelectAndFrameMatch<TargetLabel, StoreFields, False>
    for FieldLookupCons<FrameLabel, StoreDetails, Tail>
where
    Tail: SelectAndFrame<TargetLabel, StoreFields>,
    StoreFields: AssocStorage,
{
    type DType = <Tail as SelectAndFrame<TargetLabel, StoreFields>>::DType;
    type Field = <Tail as SelectAndFrame<TargetLabel, StoreFields>>::Field;

    fn select_and_frame(
        perm: &Rc<Permutation>,
        store: &DataStore<StoreFields>,
    ) -> Framed<Self::DType, Self::Field> {
        <Tail as SelectAndFrame<TargetLabel, StoreFields>>::select_and_frame(perm, store)
    }
}

/// Trait for generating a of fields to rotate over, producing a collection of
/// [DataRef](../store/struct.DataRef.html) objects.
pub trait RotateFields<StoreFields, DType>
where
    StoreFields: AssocStorage,
{
    /// Add data pointed to by this type to the `DataRef` collection, drawing data from the
    /// provided `DataStore`.
    fn add_to_rotation(store: &DataStore<StoreFields>) -> VecDeque<DataRef<DType>>;
}

impl<StoreFields, DType> RotateFields<StoreFields, DType> for Nil
where
    StoreFields: AssocStorage,
{
    fn add_to_rotation(_store: &DataStore<StoreFields>) -> VecDeque<DataRef<DType>> {
        VecDeque::new()
    }
}
impl<StoreFields, DType, Label, Tail> RotateFields<StoreFields, DType>
    for StoreFieldCons<Label, Tail>
where
    StoreFields: AssocStorage,
    DataStore<StoreFields>: SelectFieldByLabel<Label, Output = DataRef<DType>>,
    Tail: RotateFields<StoreFields, DType>,
    DType: Debug,
{
    fn add_to_rotation(store: &DataStore<StoreFields>) -> VecDeque<DataRef<DType>> {
        let mut v = Tail::add_to_rotation(store);
        v.push_front(store.field::<Label>());
        v
    }
}

impl<FrameFields, StoreFields, Label> SelectFieldByLabel<Label>
    for DataFrame<FrameFields, StoreFields>
where
    FrameFields: SelectAndFrame<Label, StoreFields>,
    StoreFields: AssocStorage,
{
    type Output = Framed<
        <FrameFields as SelectAndFrame<Label, StoreFields>>::DType,
        <FrameFields as SelectAndFrame<Label, StoreFields>>::Field,
    >;

    fn select_field(&self) -> Self::Output {
        <FrameFields as SelectAndFrame<Label, StoreFields>>::select_and_frame(
            &self.permutation,
            &*self.store,
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
            Name1: String,
            Name2: String,
            Name3: String,
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

    #[test]
    fn framed_melt() {
        let store = DataStore::<Nil>::empty().push_back_from_iter::<order::Name1, _, _, _>(
            vec!["First1", "Second1", "Third1"]
                .iter()
                .map(|&s| s.to_owned()),
        );
        let store = store.push_back_from_iter::<order::Name2, _, _, _>(
            vec!["First2", "Second2", "Third2"]
                .iter()
                .map(|&s| s.to_owned()),
        );
        let store = store.push_back_from_iter::<order::Name3, _, _, _>(
            vec!["First3", "Second3", "Third3"]
                .iter()
                .map(|&s| s.to_owned()),
        );

        // let perm_ref = Rc::new(Permutation::default());
        let framed_data = Framed::<String, _>::new_melt(
            Rc::new(Permutation::default()),
            vec![
                store.field::<order::Name1>(),
                store.field::<order::Name2>(),
                store.field::<order::Name3>(),
            ],
        );

        println!("{:?}", framed_data.to_vec());
        assert_eq!(
            framed_data.to_vec(),
            vec![
                "First1", "First2", "First3", "Second1", "Second2", "Second3", "Third1", "Third2",
                "Third3",
            ]
        );
    }
}
