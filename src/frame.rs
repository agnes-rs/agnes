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

use access::{DataIndex, NRows};
use cons::Nil;
use error;
use field::FieldData;
use fieldlist::FieldCons;
use label::*;
use permute::{self, UpdatePermutation};
use select::{FieldSelect, SelectFieldByLabel};
use store::{AssocFrameLookup, AssocStorage, DataRef, DataStore, IntoView};
use value::Value;
use view::{DataView, ViewFrameCons};

type Permutation = permute::Permutation<Vec<usize>>;

/// Type alias for label-only cons-list
pub type StoreFieldCons<L, T> = LCons<L, T>;
/// A marker struct for a frame type (Single, Melt, etc.) and the list of fields used with this
/// type (a [StoreFieldCons](type.StoreFieldCons.html) / [LabelCons](../label/type.LabelCons.html)).
pub struct StoreFieldMarkers<FrameType, StoreFieldList> {
    _marker: PhantomData<(FrameType, StoreFieldList)>,
}
/// Type alias for a cons-list which associated frame labels with their underlying store(s).
/// `FrameLabel` is a label struct. `StoreDetails` is a `StoreFieldMarkers` struct.
pub type FieldLookupCons<FrameLabel, StoreDetails, Tail> = LMCons<FrameLabel, StoreDetails, Tail>;

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
impl<Label, Value, Tail> SimpleFrameFields for LVCons<Label, Value, Tail>
where
    Tail: SimpleFrameFields,
{
    type Fields = FieldLookupCons<
        Label,
        StoreFieldMarkers<Single, Labels![Label]>,
        <Tail as SimpleFrameFields>::Fields,
    >;
}

/// Trait for creating a [DataFrame](struct.DataFrame.html) containing a single field with label
/// `Label` which contains the `String` labels of the implementing type (a cons-list).
pub trait IntoStrFrame<Label> {
    /// Output `DataFrame` type.
    type Output;

    /// Create the `DataFrame` with the a single field labeled `Label` which contains the `String`
    /// labels from this type. The frame will have a number of rows equal to the length of the
    /// implementing cons-list.
    fn into_str_frame() -> Self::Output;
    /// Create the `DataFrame` with the a single field labeled `Label` which contains the `String`
    /// labels from this type, repeated `n` times. The frame will have a number of rows equal to the
    /// length of the implementing cons-list multiplied by `n`.
    fn into_repeated_str_frame(n: usize) -> Self::Output;
}

impl<Label, List> IntoStrFrame<Label> for List
where
    Label: Debug,
    List: StrLabels,
{
    type Output = DataFrame<
        <FieldCons<Label, String, Nil> as SimpleFrameFields>::Fields,
        DataStore<FieldCons<Label, String, Nil>>,
    >;

    fn into_str_frame() -> Self::Output {
        let strs: FieldData<String> = Self::labels().iter().map(|&s| s.to_owned()).collect();
        DataFrame::from(DataStore::<Nil>::empty().push_back_field::<Label, _>(strs))
    }

    fn into_repeated_str_frame(n: usize) -> Self::Output {
        let strs: FieldData<String> = Self::labels().iter().map(|&s| s.to_owned()).collect();
        DataFrame::from_repeated_store(
            DataStore::<Nil>::empty().push_back_field::<Label, _>(strs),
            n,
        )
    }
}

/// Helper trait for [IntoMeltFrame](trait.IntoMeltFrame.html) to generate the `FrameFields` list.
pub trait MeltFrameFields<MeltLabel> {
    /// The computed melt `FrameFields` [FieldLookupCons](type.FieldLookupCons.html) cons-list.
    type Fields;
}
impl<MeltLabel, Labels> MeltFrameFields<MeltLabel> for Labels
where
    Labels: AssocLabels,
{
    type Fields =
        FieldLookupCons<MeltLabel, StoreFieldMarkers<Melt, <Labels as AssocLabels>::Labels>, Nil>;
}

/// A data frame. A reference to the underlying data store along with record-based filtering and
/// sorting details. `FrameFields` is a [FieldLookupCons](type.FieldLookupCons.html) cons-list which
/// maps a single label to one or more underlying store labels.
#[derive(Debug)]
pub struct DataFrame<FrameFields, FramedStore> {
    permutation: Rc<Permutation>,
    fields: PhantomData<FrameFields>,
    store: Arc<FramedStore>,
}

impl<FrameFields, FramedStore> DataFrame<FrameFields, FramedStore>
where
    FramedStore: NRows,
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

impl<FrameFields, FramedStore> NRows for DataFrame<FrameFields, FramedStore>
where
    FramedStore: NRows,
{
    fn nrows(&self) -> usize {
        self.len()
    }
}

impl<FrameFields, FramedStore> Clone for DataFrame<FrameFields, FramedStore> {
    fn clone(&self) -> DataFrame<FrameFields, FramedStore> {
        DataFrame {
            permutation: self.permutation.clone(),
            fields: PhantomData,
            store: Arc::clone(&self.store),
        }
    }
}

#[cfg(test)]
pub trait StoreRefCount {
    fn store_ref_count(&self) -> usize;
}
#[cfg(test)]
impl<FrameFields, FramedStore> StoreRefCount for DataFrame<FrameFields, FramedStore> {
    fn store_ref_count(&self) -> usize {
        Arc::strong_count(&self.store)
    }
}
impl<FrameFields, FramedStore> UpdatePermutation for DataFrame<FrameFields, FramedStore> {
    fn update_permutation(mut self, new_permutation: &[usize]) -> Self {
        let perm = (*self.permutation).clone();
        self.permutation = Rc::new(perm.update_indices(new_permutation));
        self
    }
}

impl<StoreFields> From<DataStore<StoreFields>>
    for DataFrame<<StoreFields as SimpleFrameFields>::Fields, DataStore<StoreFields>>
where
    StoreFields: AssocStorage + SimpleFrameFields,
{
    fn from(
        store: DataStore<StoreFields>,
    ) -> DataFrame<<StoreFields as SimpleFrameFields>::Fields, DataStore<StoreFields>> {
        DataFrame {
            permutation: Rc::new(Permutation::default()),
            fields: PhantomData,
            store: Arc::new(store),
        }
    }
}

impl<Labels, Frames> From<DataView<Labels, Frames>>
    for DataFrame<<Labels as SimpleFrameFields>::Fields, DataView<Labels, Frames>>
where
    Labels: SimpleFrameFields,
{
    fn from(
        view: DataView<Labels, Frames>,
    ) -> DataFrame<<Labels as SimpleFrameFields>::Fields, DataView<Labels, Frames>> {
        DataFrame {
            permutation: Rc::new(Permutation::default()),
            fields: PhantomData,
            store: Arc::new(view),
        }
    }
}

/// Trait for repackaging an data store into a `DataFrame`[struct.DataFrame.html]. The output
/// `DataFrame` should have the same labels as the underlying data store.
pub trait IntoFrame {
    /// The `FrameFields` type parameter for the output `DataFrame`.
    type FrameFields;
    /// The `FramedStore` type parameter for the output `DataFrame`.
    type FramedStore;
    /// The output `DataFrame` (should always be `DataFrame<Self::FrameFields, Self::FramedStore>`).
    type Output; // = DataFrame<Self::FrameFields, Self::FramedStore>

    /// Convert `self` into a [DataFrame](struct.DataFrame.html) object.
    fn into_frame(self) -> Self::Output;
}

impl<Labels, Frames> IntoFrame for DataView<Labels, Frames>
where
    Labels: SimpleFrameFields,
    DataFrame<<Labels as SimpleFrameFields>::Fields, DataView<Labels, Frames>>:
        From<DataView<Labels, Frames>>,
{
    type FrameFields = <Labels as SimpleFrameFields>::Fields;
    type FramedStore = DataView<Labels, Frames>;

    type Output = DataFrame<Self::FrameFields, Self::FramedStore>;

    fn into_frame(self) -> Self::Output {
        self.into()
    }
}

impl<Fields> IntoFrame for DataStore<Fields>
where
    Fields: AssocStorage + SimpleFrameFields,
    DataFrame<<Fields as SimpleFrameFields>::Fields, DataStore<Fields>>: From<DataStore<Fields>>,
{
    type FrameFields = <Fields as SimpleFrameFields>::Fields;
    type FramedStore = DataStore<Fields>;

    type Output = DataFrame<Self::FrameFields, Self::FramedStore>;

    fn into_frame(self) -> Self::Output {
        self.into()
    }
}

/// Trait for repackaging an data store into a `DataFrame`[struct.DataFrame.html] as a melted data
/// structure. The output `DataFrame` will have one label, `MeltLabel`, which rotates over the
/// labels in underlying data store.
pub trait IntoMeltFrame<MeltLabel> {
    /// The `FrameFields` type parameter for the output `DataFrame`.
    type FrameFields;
    /// The `FramedStore` type parameter for the output `DataFrame`.
    type FramedStore;
    /// The output `DataFrame` (should always be `DataFrame<Self::FrameFields, Self::FramedStore>`).
    type Output; // = DataFrame<Self::FrameFields, Self::FramedStore>

    /// Convert `self` into a [DataFrame](struct.DataFrame.html) object.
    fn into_melt_frame(self) -> Self::Output;
}

impl<MeltLabel, Labels, Frames> IntoMeltFrame<MeltLabel> for DataView<Labels, Frames>
where
    Labels: MeltFrameFields<MeltLabel>,
{
    type FrameFields = <Labels as MeltFrameFields<MeltLabel>>::Fields;
    type FramedStore = DataView<Labels, Frames>;

    type Output = DataFrame<Self::FrameFields, Self::FramedStore>;

    fn into_melt_frame(self) -> Self::Output {
        DataFrame {
            permutation: Rc::new(Permutation::default()),
            fields: PhantomData,
            store: Arc::new(self),
        }
    }
}

impl<FrameFields, FramedStore> IntoView for DataFrame<FrameFields, FramedStore>
where
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

impl<StoreFields> DataFrame<<StoreFields as SimpleFrameFields>::Fields, DataStore<StoreFields>>
where
    StoreFields: AssocStorage + SimpleFrameFields,
    DataStore<StoreFields>: NRows,
{
    pub(crate) fn from_repeated_store(
        store: DataStore<StoreFields>,
        reps: usize,
    ) -> DataFrame<<StoreFields as SimpleFrameFields>::Fields, DataStore<StoreFields>> {
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
impl<FrameFields, FramedStore> SelfValued for DataFrame<FrameFields, FramedStore> {}

/// Inner frame kind enum used in a [Framed](struct.Framed.html) struct.
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

/// Trait for selecting a field associated with the label `Label` from the fields in `FramedStore`.
pub trait SelectAndFrame<Label, FramedStore> {
    /// The resultant data type of the field.
    type DType: Debug;
    /// The field accessor type.
    type Field: DataIndex<DType = Self::DType>;

    /// Returns an [Framed](struct.Framed.html) struct accessing the selected field.
    fn select_and_frame(
        perm: &Rc<Permutation>,
        store: &FramedStore,
    ) -> Framed<Self::DType, Self::Field>;
}

/// Helper trait for selecting and framing fields. Used by
/// [SelectAndFrame](trait.SelectAndFrame.html). `Label` is the label to select, `FramedStore` is
/// the struct the data is stored in, and `Match` is whether or not `Label` matches the head
/// label in this type.
pub trait SelectAndFrameMatch<Label, FramedStore, Match> {
    /// The resultant data type of the field.
    type DType: Debug;
    /// The field accessor type.
    type Field: DataIndex<DType = Self::DType>;

    /// Returns an [Framed](struct.Framed.html) struct accessing the selected field.
    fn select_and_frame(
        perm: &Rc<Permutation>,
        store: &FramedStore,
    ) -> Framed<Self::DType, Self::Field>;
}

impl<TargetLabel, FrameLabel, StoreDetails, Tail, FramedStore>
    SelectAndFrame<TargetLabel, FramedStore> for FieldLookupCons<FrameLabel, StoreDetails, Tail>
where
    TargetLabel: LabelEq<FrameLabel>,
    FieldLookupCons<FrameLabel, StoreDetails, Tail>:
        SelectAndFrameMatch<TargetLabel, FramedStore, <TargetLabel as LabelEq<FrameLabel>>::Eq>,
{
    type DType = <FieldLookupCons<FrameLabel, StoreDetails, Tail> as SelectAndFrameMatch<
        TargetLabel,
        FramedStore,
        <TargetLabel as LabelEq<FrameLabel>>::Eq,
    >>::DType;
    type Field = <FieldLookupCons<FrameLabel, StoreDetails, Tail> as SelectAndFrameMatch<
        TargetLabel,
        FramedStore,
        <TargetLabel as LabelEq<FrameLabel>>::Eq,
    >>::Field;

    fn select_and_frame(
        perm: &Rc<Permutation>,
        store: &FramedStore,
    ) -> Framed<Self::DType, Self::Field> {
        <Self as SelectAndFrameMatch<
            TargetLabel,
            FramedStore,
            <TargetLabel as LabelEq<FrameLabel>>::Eq,
        >>::select_and_frame(perm, store)
    }
}

impl<TargetLabel, FrameLabel, StoreFieldList, Tail, FramedStore>
    SelectAndFrameMatch<TargetLabel, FramedStore, True>
    for FieldLookupCons<FrameLabel, StoreFieldMarkers<Single, StoreFieldList>, Tail>
where
    FramedStore: SelectFieldByLabel<TargetLabel>,
    <FramedStore as SelectFieldByLabel<TargetLabel>>::DType: Debug,
{
    type DType = <FramedStore as SelectFieldByLabel<TargetLabel>>::DType;
    type Field = <FramedStore as SelectFieldByLabel<TargetLabel>>::Output;

    fn select_and_frame(
        perm: &Rc<Permutation>,
        store: &FramedStore,
    ) -> Framed<Self::DType, Self::Field> {
        Framed::new(
            Rc::clone(perm),
            SelectFieldByLabel::<TargetLabel>::select_field(store),
        )
    }
}

impl<TargetLabel, FrameLabel, StoreFieldList, Tail, FramedStore>
    SelectAndFrameMatch<TargetLabel, FramedStore, True>
    for FieldLookupCons<FrameLabel, StoreFieldMarkers<Melt, StoreFieldList>, Tail>
where
    StoreFieldList: RotateFields<FramedStore>,
    <StoreFieldList as RotateFields<FramedStore>>::DType: Debug,
    <StoreFieldList as RotateFields<FramedStore>>::Output: Clone,
{
    type DType = <StoreFieldList as RotateFields<FramedStore>>::DType;
    type Field = <StoreFieldList as RotateFields<FramedStore>>::Output;

    fn select_and_frame(
        perm: &Rc<Permutation>,
        store: &FramedStore,
    ) -> Framed<Self::DType, Self::Field> {
        let melt_rotation = <StoreFieldList as RotateFields<FramedStore>>::add_to_rotation(store);
        Framed::new_melt(
            Rc::clone(perm),
            melt_rotation.iter().cloned().collect::<Vec<_>>(),
        )
    }
}

impl<TargetLabel, FrameLabel, StoreDetails, Tail, FramedStore>
    SelectAndFrameMatch<TargetLabel, FramedStore, False>
    for FieldLookupCons<FrameLabel, StoreDetails, Tail>
where
    Tail: SelectAndFrame<TargetLabel, FramedStore>,
{
    type DType = <Tail as SelectAndFrame<TargetLabel, FramedStore>>::DType;
    type Field = <Tail as SelectAndFrame<TargetLabel, FramedStore>>::Field;

    fn select_and_frame(
        perm: &Rc<Permutation>,
        store: &FramedStore,
    ) -> Framed<Self::DType, Self::Field> {
        <Tail as SelectAndFrame<TargetLabel, FramedStore>>::select_and_frame(perm, store)
    }
}

/// Trait for generating a collection of objects implementing
/// [DataIndex](../access/trait.DataIndex.html) with the same underlying type. Used for rotating
/// through source fields in a [Melt](struct.Melt.html)ed field.
pub trait RotateFields<FramedStore> {
    /// Underlying data type of the source fields
    type DType;
    /// Type of the individual `DataIndex`-implementing objects returned.
    type Output: DataIndex<DType = Self::DType>;

    /// Add data pointed to by this type to the data collection, drawing data from the
    /// provided `DataStore`.
    fn add_to_rotation(store: &FramedStore) -> VecDeque<Self::Output>;
}

impl<FramedStore> RotateFields<FramedStore> for Nil {
    type DType = usize;
    type Output = DataRef<usize>;

    fn add_to_rotation(_store: &FramedStore) -> VecDeque<Self::Output> {
        VecDeque::new()
    }
}
impl<FramedStore, Label, Tail> RotateFields<FramedStore> for StoreFieldCons<Label, Tail>
where
    FramedStore: SelectFieldByLabel<Label>,
    Tail: RotateFieldsTerm<
        FramedStore,
        <FramedStore as SelectFieldByLabel<Label>>::DType,
        <FramedStore as SelectFieldByLabel<Label>>::Output,
    >,
{
    type DType = <FramedStore as SelectFieldByLabel<Label>>::DType;
    type Output = <FramedStore as SelectFieldByLabel<Label>>::Output;

    fn add_to_rotation(store: &FramedStore) -> VecDeque<Self::Output> {
        let mut v = Tail::add_to_rotation(store);
        v.push_front(SelectFieldByLabel::<Label>::select_field(store));
        v
    }
}

/// Helper trait for helping terminate a [RotateFields](trait.RotateFields.html) recursion. Handles
/// taking care of the [Nil](../cons/struct.Nil.html) end-case without losing the type information
/// of the returned output collection.
pub trait RotateFieldsTerm<FramedStore, DType, Output> {
    /// Create or add data to the data collection.
    fn add_to_rotation(store: &FramedStore) -> VecDeque<Output>;
}
impl<FramedStore, DType, Output> RotateFieldsTerm<FramedStore, DType, Output> for Nil {
    fn add_to_rotation(_store: &FramedStore) -> VecDeque<Output> {
        VecDeque::new()
    }
}
impl<FramedStore, DType, Output, Label, Tail> RotateFieldsTerm<FramedStore, DType, Output>
    for StoreFieldCons<Label, Tail>
where
    StoreFieldCons<Label, Tail>: RotateFields<FramedStore, DType = DType, Output = Output>,
    Output: DataIndex<DType = DType>,
{
    fn add_to_rotation(store: &FramedStore) -> VecDeque<Output> {
        <StoreFieldCons<Label, Tail> as RotateFields<FramedStore>>::add_to_rotation(store)
    }
}

impl<FrameFields, FramedStore, Label> SelectFieldByLabel<Label>
    for DataFrame<FrameFields, FramedStore>
where
    FrameFields: SelectAndFrame<Label, FramedStore>,
{
    type DType = <FrameFields as SelectAndFrame<Label, FramedStore>>::DType;
    type Output = Framed<Self::DType, <FrameFields as SelectAndFrame<Label, FramedStore>>::Field>;

    fn select_field(&self) -> Self::Output {
        <FrameFields as SelectAndFrame<Label, FramedStore>>::select_and_frame(
            &self.permutation,
            &*self.store,
        )
    }
}

impl<FrameFields, FramedStore> FieldSelect for DataFrame<FrameFields, FramedStore> {}

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

    #[test]
    fn frame_view() {
        let store = DataStore::<Nil>::empty().push_back_from_iter::<order::Name1, _, _, _>(
            vec!["First1", "Second1", "Third1"]
                .iter()
                .map(|&s| s.to_owned()),
        );
        let dv = store.into_view();
        println!("{}", dv.nrows());
        let view_in_frame: DataFrame<_, _> = dv.into();
        println!("{}", view_in_frame.nrows());
        let final_view = view_in_frame.into_view();
        println!("{}", final_view);
    }
}
