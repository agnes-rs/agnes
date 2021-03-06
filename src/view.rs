/*!
Main `DataView` struct and associated implementations.

# Aggregation

There are three types of data aggregation supported by `agnes`:
* Data merging -- combining two `DataView` objects with the same number of records together,
creating a new `DataView` with all the fields of the two source `DataView`s.
* Data appending -- combining two `DataView` objects with the same fields, creating a new `DataView`
object with all of the records of the two source `DataView`s.
* Data joining -- combining two `DataView` objects using specified join, creating a new
`DataView` object with a subset of records from the two source `DataView`s according to the join
parameters.

*/
#[cfg(test)]
use std::collections::VecDeque;
use std::collections::{HashMap, HashSet};
use std::fmt::{self, Debug, Display, Formatter};
use std::hash::{Hash, Hasher};
use std::marker::PhantomData;

use prettytable as pt;
#[cfg(feature = "serialize")]
use serde::ser::{Serialize, SerializeMap, Serializer};

use access::*;
use cons::*;
use error;
use field::FieldData;
use fieldlist::FieldPayloadCons;
#[cfg(test)]
use frame::StoreRefCount;
use frame::{Framed, IntoFrame, IntoMeltFrame, IntoStrFrame};
use join::*;
use label::*;
use partial::{DeriveCapabilities, Func, FuncDefault, Implemented, IsImplemented, PartialMap};
use permute::{
    FilterPerm, SortOrder, SortOrderComparator, SortOrderUnstable, SortOrderUnstableComparator,
    UpdatePermutation,
};
use select::{FieldSelect, SelectFieldByLabel};
use store::{IntoStore, IntoView};
use value::Value;

/// Cons-list of `DataFrame`s held by a `DataView. `FrameIndex` is simply an index used by
/// `FrameLookupCons` to look up `DataFrame`s for a specified `Label`, and `Frame` is the type
/// of the associated `DataFrame`.
pub(crate) type ViewFrameCons<FrameIndex, Frame, Tail> = LVCons<FrameIndex, Frame, Tail>;

/// Cons-list of field labels along with the details necessary to look up that label in a
/// `DataView`'s `ViewFrameCons` cons-list of `DataFrame`s. The `FrameIndex` specifies the index
/// of the `DataFrame` containing the field labeled `Label` in the `ViewFrameCons`, and the
/// `FrameLabel` specifies the potentially-different (since `DataView` supports renaming fields)
/// `Label` within that `DataFrame`.
pub(crate) type FrameLookupCons<Label, FrameIndex, FrameLabel, Tail> =
    LMCons<Label, FrameDetailMarkers<FrameIndex, FrameLabel>, Tail>;

/// A `DataView` is a specific view of data stored inside a `DataStore`. It consists of a list of
/// `DataFrame` objects, which themselves reference individual `DataStore`s.
///
/// The type parameter `Frames` is a `ViewFrameCons` cons-list which contains the `DataFrame`
/// objects referenced by this `DataView`. The type parameter `Labels` is a `FrameLookupCons` which
/// provides lookup functionality from a specific `Label` into the `Frames` cons-list.
#[derive(Debug, Clone, Default)]
pub struct DataView<Labels, Frames> {
    pub(crate) _labels: PhantomData<Labels>,
    pub(crate) frames: Frames,
}

/// Marker struct with the details of where to find a field's data. The `FrameIndex` specifies
/// the index of the [DataFrame](../frame/struct.DataFrame.html) in a
/// [DataView](struct.DataView.html)'s `Frames` cons-list. The `FrameLabel` denotes the label of
/// the field within that frame.
pub struct FrameDetailMarkers<FrameIndex, FrameLabel> {
    _marker: PhantomData<(FrameIndex, FrameLabel)>,
}
/// A trait for providing the associated `FrameIndex` and `FrameLabel` types for a
/// [FrameDetailMarkers](struct.FrameDetailMarkers.html) struct.
pub trait FrameDetails {
    /// The associated frame index.
    type FrameIndex: Identifier;
    /// The associated `Label` within the frame.
    type FrameLabel: Label;
}
impl<FrameIndex, FrameLabel> FrameDetails for FrameDetailMarkers<FrameIndex, FrameLabel>
where
    FrameIndex: Identifier,
    FrameLabel: Label,
{
    type FrameIndex = FrameIndex;
    type FrameLabel = FrameLabel;
}

impl<Labels, Frames> DataView<Labels, Frames> {
    /// Creates a new `DataView` with `frames`.
    pub fn new(frames: Frames) -> DataView<Labels, Frames> {
        DataView {
            _labels: PhantomData,
            frames,
        }
    }
}

impl<Labels, Frames> DataView<Labels, Frames> {
    /// Field names in this data view
    pub fn fieldnames<'a>(&'a self) -> Vec<&'a str>
    where
        Labels: StrLabels,
    {
        <Labels as StrLabels>::labels().into()
    }
}

/// A trait for deriving the [LabelCons](../label/type.LabelCons.html) of field indices of a type.
pub trait FrameIndexList {
    /// The associated `LabelCons` for this type.
    type LabelList;
}

impl FrameIndexList for Nil {
    type LabelList = Nil;
}

impl<Label, FrameIndex, FrameLabel, Tail> FrameIndexList
    for FrameLookupCons<Label, FrameIndex, FrameLabel, Tail>
where
    Tail: FrameIndexList,
{
    type LabelList = LCons<FrameIndex, <Tail as FrameIndexList>::LabelList>;
}

impl<Labels, Frames> DataView<Labels, Frames>
where
    Frames: Clone,
{
    /// Generate a new subview of this `DataView`. LabelList is a
    /// [LabelCons](../label/type.LabelCons.html) list of labels, which can be generated using the
    /// [Labels](../macro.Labels.html) macro.
    pub fn v<LabelList>(&self) -> <Self as Subview<LabelList>>::Output
    where
        Self: Subview<LabelList>,
    {
        Subview::<LabelList>::subview(self)
    }
    /// Generate a new subview of this `DataView`. Equivalent to [v](struct.DataView.html#method.v).
    pub fn subview<LabelList>(&self) -> <Self as Subview<LabelList>>::Output
    where
        Self: Subview<LabelList>,
    {
        Subview::<LabelList>::subview(self)
    }
}

/// Trait for generating a subview of a [DataView](struct.DataView.html). `LabelList` is the fields
/// to keep in the generated `DataView`.
pub trait Subview<LabelList> {
    /// Resulting subview `DataView` type.
    type Output;

    /// Generate a new subview of this `DataView`, resulting in a newly created `DataView` object
    /// only containing the fields matching the labels in `LabelList`.
    fn subview(&self) -> Self::Output;
}

impl<Labels, Frames, LabelList> Subview<LabelList> for DataView<Labels, Frames>
where
    Labels: FrameIndexList + HasLabels<LabelList> + LabelSubset<LabelList>,
    <Labels as LabelSubset<LabelList>>::Output: Reorder<LabelList>,
    Frames: Clone + SubsetClone<<Labels as FrameIndexList>::LabelList>,
{
    type Output = DataView<
        <<Labels as LabelSubset<LabelList>>::Output as Reorder<LabelList>>::Output,
        <Frames as SubsetClone<<Labels as FrameIndexList>::LabelList>>::Output,
    >;

    fn subview(&self) -> Self::Output {
        DataView {
            _labels: PhantomData,
            frames: self.frames.subset_clone(),
        }
    }
}

impl<Labels, Frames> NRows for DataView<Labels, Frames>
where
    Frames: NRows,
{
    fn nrows(&self) -> usize {
        self.frames.nrows()
    }
}

impl<Labels, Frames> DataView<Labels, Frames>
where
    Self: NRows,
{
    /// Number of rows in this data view
    pub fn nrows(&self) -> usize {
        NRows::nrows(self)
    }
}

impl<Labels, Frames> DataView<Labels, Frames>
where
    Labels: Len,
    Frames: Len,
{
    /// Returns `true` if the DataView is empty (has no rows or has no fields)
    pub fn is_empty(&self) -> bool {
        length![Labels] == 0 || Frames::is_empty()
    }
}
impl<Labels, Frames> DataView<Labels, Frames>
where
    Labels: Len,
{
    /// Number of fields in this data view
    pub fn nfields(&self) -> usize {
        length![Labels]
    }
}
impl<Labels, Frames> DataView<Labels, Frames>
where
    Frames: Len,
{
    /// Number of frames this data view covers
    pub fn nframes(&self) -> usize {
        length![Frames]
    }
}

#[cfg(test)]
pub trait StoreRefCounts {
    fn store_ref_counts(&self) -> VecDeque<usize>;
}

#[cfg(test)]
impl StoreRefCounts for Nil {
    fn store_ref_counts(&self) -> VecDeque<usize> {
        VecDeque::new()
    }
}
#[cfg(test)]
impl<FrameIndex, Frame, Tail> StoreRefCounts for ViewFrameCons<FrameIndex, Frame, Tail>
where
    Frame: Valued,
    ValueOf<Frame>: StoreRefCount,
    Tail: StoreRefCounts,
{
    fn store_ref_counts(&self) -> VecDeque<usize> {
        let mut previous = self.tail.store_ref_counts();
        previous.push_front(self.head.value_ref().store_ref_count());
        previous
    }
}

#[cfg(test)]
impl<Labels, Frames> DataView<Labels, Frames>
where
    Frames: StoreRefCounts,
{
    pub fn store_ref_counts(&self) -> VecDeque<usize> {
        Frames::store_ref_counts(&self.frames)
    }
}

/// A trait for finding the associated frame details (implementing
/// [FrameDetails](trait.FrameDetails.html) -- frame index and label within that frame) for
/// for specific label within this type.
pub trait FindFrameDetails<Label>: LookupMarkedElemByLabel<Label> {
    /// The associated frame details for this type.
    type FrameDetails: FrameDetails;
}
impl<Labels, Label> FindFrameDetails<Label> for Labels
where
    Labels: LookupMarkedElemByLabel<Label>,
    MarkerOfElemOf<Labels, Label>: FrameDetails,
{
    type FrameDetails = MarkerOfElemOf<Labels, Label>;
}
/// Type alias for the [FrameDetails](trait.FrameDetails.html)-implementing struct associated with
/// the label `Label` in the label lookup list `Labels`.
pub type FrameDetailsOf<Labels, Label> = <Labels as FindFrameDetails<Label>>::FrameDetails;
/// Type alias for the `FrameIndex` of [FrameDetails](trait.FrameDetails.html)-implementing struct
/// associated with the label `Label` in the label lookup list `Labels`.
pub type FrameIndexOf<Labels, Label> =
    <<Labels as FindFrameDetails<Label>>::FrameDetails as FrameDetails>::FrameIndex;
/// Type alias for the `FrameLLabel` of [FrameDetails](trait.FrameDetails.html)-implementing struct
/// associated with the label `Label` in the label lookup list `Labels`.
pub type FrameLabelOf<Labels, Label> =
    <<Labels as FindFrameDetails<Label>>::FrameDetails as FrameDetails>::FrameLabel;

/// Marker trait for being able to find a frame of label `Label` within label lookup list `Labels`
/// in this type
pub trait FindFrame<Labels, Label>: LookupValuedElemByLabel<FrameIndexOf<Labels, Label>>
where
    Labels: FindFrameDetails<Label>,
{
}
impl<Frames, Labels, Label> FindFrame<Labels, Label> for Frames
where
    Labels: FindFrameDetails<Label>,
    Frames: LookupValuedElemByLabel<FrameIndexOf<Labels, Label>>,
{
}

/// Type alias for the cons-list element within `Frames` associated with a `FrameIndex`.
pub type FrameElemByFrameIndexOf<Frames, FrameIndex> =
    <Frames as LookupValuedElemByLabel<FrameIndex>>::Elem;
/// Type alias for the [DataFrame](../frame/struct.DataFrame.html) within `Frames` associated with
/// a `FrameIndex`.
pub type FrameByFrameIndexOf<Frames, FrameIndex> =
    <FrameElemByFrameIndexOf<Frames, FrameIndex> as Valued>::Value;
/// Type alias for the cons-list element within `Frames` associated with label `Label` in the label
/// lookup list `Labels`.
pub type FrameElemOf<Frames, Labels, Label> =
    FrameElemByFrameIndexOf<Frames, FrameIndexOf<Labels, Label>>;
/// Type alias for the [DataFrame](../frame/struct.DataFrame.html) within `Frames` associated
/// with the label `Label` in the label lookup list `Labels`.
pub type FrameOf<Frames, Labels, Label> = <FrameElemOf<Frames, Labels, Label> as Valued>::Value;

/// Type alias for the field (implementing [DataIndex](../access/trait.DataIndex.html)) within the
/// frames list `Frames` associated with the `FrameIndex` and `FrameLabel`.
pub type FieldFromFrameDetailsOf<Frames, FrameIndex, FrameLabel> =
    <FrameByFrameIndexOf<Frames, FrameIndex> as SelectFieldByLabel<FrameLabel>>::Output;

/// Type alias for the data type of the field (implementing
/// [DataIndex](../access/trait.DataIndex.html)) within the frames list `Frames` associated with
/// the `FrameIndex` and `FrameLabel`.
pub type FieldTypeFromFrameDetailsOf<Frames, FrameIndex, FrameLabel> =
    <FrameByFrameIndexOf<Frames, FrameIndex> as SelectFieldByLabel<FrameLabel>>::DType;

/// Type alias for the field (implementing [DataIndex](../access/trait.DataIndex.html)) within the
/// frames list `Frames` associated with the label `Label` in the label lookup list `Labels`.
pub type FieldOf<Frames, Labels, Label> =
    <FrameOf<Frames, Labels, Label> as SelectFieldByLabel<FrameLabelOf<Labels, Label>>>::Output;
/// Type alias for the data type of the field (implementing
/// [DataIndex](../access/trait.DataIndex.html)) within the frames list `Frames` associated with
/// the label `Label` in the label lookup list `Labels`.
pub type FieldTypeOf<Frames, Labels, Label> =
    <FrameOf<Frames, Labels, Label> as SelectFieldByLabel<FrameLabelOf<Labels, Label>>>::DType;

/// Type alias for the field (implementing [DataIndex](../access/trait.DataIndex.html)) within
/// the [DataView](struct.DataView.html) `View` associated with label `Label`.
pub type VFieldOf<View, Label> = <View as SelectFieldByLabel<Label>>::Output;
/// Type alias for the datta type of the field (implementing
/// [DataIndex](../access/trait.DataIndex.html)) within the [DataView](struct.DataView.html) `View`
/// associated with label `Label`.
pub type VFieldTypeOf<View, Label> = <View as SelectFieldByLabel<Label>>::DType;

/// Trait for selecting a field (implementing [DataIndex](../access/trait.DataIndex.html))
/// associated with the label `Label` from the label lookup list `Labels` from a type.
pub trait SelectFieldFromLabels<Labels, Label> {
    /// Data type of field accessor
    type DType;
    /// Selected field accessor.
    type Output: DataIndex<DType = Self::DType>;

    /// Returns an accessor (implementing [DataIndex](../access/trait.DataIndex.html)) for the
    /// selected field.
    fn select_field(&self) -> Self::Output;
}
impl<Labels, Frames, Label> SelectFieldFromLabels<Labels, Label> for Frames
where
    Labels: FindFrameDetails<Label>,
    Frames: FindFrame<Labels, Label>,
    FrameOf<Frames, Labels, Label>: SelectFieldByLabel<FrameLabelOf<Labels, Label>>,
    FieldOf<Frames, Labels, Label>: SelfValued + Clone,
    FieldTypeOf<Frames, Labels, Label>: fmt::Debug,
{
    type DType = FieldTypeOf<Frames, Labels, Label>;
    type Output = FieldOf<Frames, Labels, Label>;

    fn select_field(&self) -> Self::Output {
        SelectFieldByLabel::<FrameLabelOf<Labels, Label>>::select_field(
            LookupValuedElemByLabel::<FrameIndexOf<Labels, Label>>::elem(self).value_ref(),
        )
        .clone()
    }
}

impl<Labels, Frames, Label> SelectFieldByLabel<Label> for DataView<Labels, Frames>
where
    Frames: SelectFieldFromLabels<Labels, Label>,
{
    type DType = <Frames as SelectFieldFromLabels<Labels, Label>>::DType;
    type Output = <Frames as SelectFieldFromLabels<Labels, Label>>::Output;

    fn select_field(&self) -> Self::Output {
        SelectFieldFromLabels::<Labels, Label>::select_field(&self.frames)
    }
}

impl<Labels, Frames> FieldSelect for DataView<Labels, Frames> {}

/// Type alias for the cons-list of fields implementing [DataIndex](../access/trait.DataIndex.html).
pub type DataIndexCons<Label, DType, DI, Tail> = FieldPayloadCons<Label, DType, DI, Tail>;

/// Trait for finding the associated [DataIndexCons](type.DataIndexCons.html) (cons-list of fields)
/// in a type given labels in a labels list.
pub trait AssocDataIndexCons<Labels> {
    /// Type of associated data index cons-list.
    type Output;
    /// Returns the associated `DataIndexCons`.
    fn assoc_data(&self) -> Self::Output;
}
impl<Frames> AssocDataIndexCons<Nil> for Frames {
    type Output = Nil;
    fn assoc_data(&self) -> Nil {
        Nil
    }
}
impl<Label, FrameIndex, FrameLabel, LookupTail, Frames>
    AssocDataIndexCons<FrameLookupCons<Label, FrameIndex, FrameLabel, LookupTail>> for Frames
where
    Self: SelectFieldFromLabels<FrameLookupCons<Label, FrameIndex, FrameLabel, LookupTail>, Label>,
    Self: AssocDataIndexCons<LookupTail>,
    <Self as SelectFieldFromLabels<
        FrameLookupCons<Label, FrameIndex, FrameLabel, LookupTail>,
        Label,
    >>::Output: Typed,
{
    type Output = DataIndexCons<
        Label,
        TypeOf<
            <Frames as SelectFieldFromLabels<
                FrameLookupCons<Label, FrameIndex, FrameLabel, LookupTail>,
                Label,
            >>::Output,
        >,
        <Frames as SelectFieldFromLabels<
            FrameLookupCons<Label, FrameIndex, FrameLabel, LookupTail>,
            Label,
        >>::Output,
        <Frames as AssocDataIndexCons<LookupTail>>::Output,
    >;
    fn assoc_data(&self) -> Self::Output {
        DataIndexCons {
            head: TypedValue::from(SelectFieldFromLabels::<
                FrameLookupCons<Label, FrameIndex, FrameLabel, LookupTail>,
                Label,
            >::select_field(self))
            .into(),
            tail: AssocDataIndexCons::<LookupTail>::assoc_data(self),
        }
    }
}

/// Type alias for finding the [DataIndexCons](type.DataIndexCons.html) within the frames `Frames`
/// associated with labels `Labels`.
pub type AssocDataIndexConsOf<Labels, Frames> = <Frames as AssocDataIndexCons<Labels>>::Output;

/// Trait for applying a function (implementing [Func](../partial/trait.Func.html)) to all the
/// fields in a data structure. Calls the [call](../partial.trait.Func.html#call) method for each
/// field of this object.
pub trait FieldMap<F> {
    /// Apply this function to every field in this structure.
    fn field_map(&self, f: &mut F);
}

impl<F, Labels, Frames> FieldMap<F> for DataView<Labels, Frames>
where
    Frames: AssocDataIndexCons<Labels>,
    AssocDataIndexConsOf<Labels, Frames>: DeriveCapabilities<F>,
{
    fn field_map(&self, f: &mut F) {
        self.frames.assoc_data().derive().map(f);
    }
}

const MAX_DISP_ROWS: usize = 1000;

impl<Labels, Frames> Display for DataView<Labels, Frames>
where
    Frames: Len + NRows,
    Self: FieldMap<AddCellToRowFn>,
    Labels: StrLabels,
{
    fn fmt(&self, f: &mut Formatter) -> Result<(), fmt::Error> {
        if Frames::is_empty() {
            return write!(f, "Empty DataView");
        }
        let mut table = pt::Table::new();

        let nrows = self.nrows();
        let mut func = AddCellToRowFn {
            rows: vec![pt::row::Row::empty(); nrows.min(MAX_DISP_ROWS)],
        };
        self.field_map(&mut func);
        for row in func.rows.drain(..) {
            table.add_row(row);
        }

        table.set_titles(<Labels as StrLabels>::labels().into());
        table.set_format(*pt::format::consts::FORMAT_NO_BORDER_LINE_SEPARATOR);

        Display::fmt(&table, f)
    }
}

/// Function (implementing [Func](../partial/trait.Func.html)) that adds cells to
/// `prettytable::row::Row`.
pub struct AddCellToRowFn {
    rows: Vec<pt::row::Row>,
}
impl<DType> Func<DType> for AddCellToRowFn
where
    for<'a> Value<&'a DType>: ToString,
{
    type Output = ();
    fn call<DI>(&mut self, data: &DI) -> Self::Output
    where
        DI: DataIndex<DType = DType>,
    {
        debug_assert!(data.len() >= self.rows.len());
        for i in 0..self.rows.len() {
            self.rows[i].add_cell(cell!(data.get_datum(i).unwrap()));
        }
    }
}
impl FuncDefault for AddCellToRowFn {
    type Output = ();
    fn call(&mut self) -> Self::Output {
        for i in 0..self.rows.len() {
            self.rows[i].add_cell(cell!());
        }
    }
}
macro_rules! impl_addcell_is_impl {
    ($($dtype:ty)*) => {$(
        impl IsImplemented<AddCellToRowFn> for $dtype {
            type IsImpl = Implemented;
        }
    )*}
}
impl_addcell_is_impl![String &str f64 f32 u64 u32 i64 i32 bool];

impl<Labels, Frames> DataView<Labels, Frames> {
    /// Construct a new `DataView` with the label `CurrLabel` relabeled with the label `NewLabel`.
    pub fn relabel<CurrLabel, NewLabel>(
        self,
    ) -> DataView<<Labels as Relabel<CurrLabel, NewLabel>>::Output, Frames>
    where
        Labels: Relabel<CurrLabel, NewLabel>,
    {
        DataView {
            _labels: PhantomData,
            frames: self.frames,
        }
    }
}

/// Trait for relabeling the label `TargetLabel` with `NewLabel`.
pub trait Relabel<TargetLabel, NewLabel> {
    /// The output type after relabeling `TargetLabel` to `NewLabel`.
    type Output;
}

impl<TargetLabel, NewLabel, Label, FrameIndex, FrameLabel, Tail> Relabel<TargetLabel, NewLabel>
    for FrameLookupCons<Label, FrameIndex, FrameLabel, Tail>
where
    TargetLabel: LabelEq<Label>,
    FrameLookupCons<Label, FrameIndex, FrameLabel, Tail>:
        RelabelMatch<TargetLabel, NewLabel, <TargetLabel as LabelEq<Label>>::Eq>,
{
    type Output = <FrameLookupCons<Label, FrameIndex, FrameLabel, Tail> as RelabelMatch<
        TargetLabel,
        NewLabel,
        <TargetLabel as LabelEq<Label>>::Eq,
    >>::Output;
}

/// Helper trait for relabeling. Used by [Relabel](trait.Relabel.html). `TargetLabel` is the label
/// to change, `NewLabel` is the desired label to change to, and `Match` is whether or not
/// `TargetLabel` matches the head label in this type.
pub trait RelabelMatch<TargetLabel, NewLabel, Match> {
    /// The output type after relabeling `TargetLabel` to `NewLabel`.
    type Output;
}
// TargetLabel == Label, replace with NewLabel
impl<TargetLabel, NewLabel, Label, FrameIndex, FrameLabel, Tail>
    RelabelMatch<TargetLabel, NewLabel, True>
    for FrameLookupCons<Label, FrameIndex, FrameLabel, Tail>
{
    type Output = FrameLookupCons<NewLabel, FrameIndex, FrameLabel, Tail>;
}
// TargetLabel != Label, recurse
impl<TargetLabel, NewLabel, Label, FrameIndex, FrameLabel, Tail>
    RelabelMatch<TargetLabel, NewLabel, False>
    for FrameLookupCons<Label, FrameIndex, FrameLabel, Tail>
where
    Tail: Relabel<TargetLabel, NewLabel>,
{
    type Output = FrameLookupCons<
        Label,
        FrameIndex,
        FrameLabel,
        <Tail as Relabel<TargetLabel, NewLabel>>::Output,
    >;
}

/// Trait for merging the data from two [DataView](struct.DataView.html)s into one new `DataView`.
/// The two `DataView`s should have the same number of rows, and the resultant `DataView` is one
/// with all the fields of both of the two original `DataView`s.
///
/// This trait does not consume the source `DataView`s: the resultant `DataView` should contain
/// new references to the original field data.
pub trait ViewMerge<Other> {
    /// Resultant `DataView` type.
    type Output;
    /// Merge this `DataView` with another `DataView`. Can fail if the `DataView`s do not have the
    /// same number of rows.
    fn merge(&self, right: &Other) -> error::Result<Self::Output>;
}
impl<Labels, Frames, RLabels, RFrames> ViewMerge<DataView<RLabels, RFrames>>
    for DataView<Labels, Frames>
where
    Self: Merge<RLabels, RFrames>,
    RFrames: NRows,
    Frames: NRows,
    <Self as Merge<RLabels, RFrames>>::OutLabels: IsLabelSet<IsSet = True>,
{
    type Output = DataView<
        <Self as Merge<RLabels, RFrames>>::OutLabels,
        <Self as Merge<RLabels, RFrames>>::OutFrames,
    >;

    fn merge(&self, right: &DataView<RLabels, RFrames>) -> error::Result<Self::Output> {
        if self.nrows() != right.nrows() {
            return Err(error::AgnesError::DimensionMismatch(
                "number of rows mismatch in merge".into(),
            ));
        }
        Ok(Merge::merge(self, right))
    }
}

impl<Labels, Frames> DataView<Labels, Frames> {
    /// Merge this `DataView` with another `DataView` object, creating a new `DataView` with the
    /// same number of rows and all the fields from both source `DataView` objects.
    ///
    /// Fails if the two `DataView`s have different number of rows.
    pub fn merge<RLabels, RFrames>(
        &self,
        right: &DataView<RLabels, RFrames>,
    ) -> error::Result<<Self as ViewMerge<DataView<RLabels, RFrames>>>::Output>
    where
        Self: ViewMerge<DataView<RLabels, RFrames>>,
    {
        ViewMerge::merge(self, right)
    }
}

impl<Labels, Frames> DataView<Labels, Frames> {
    /// Combine two `DataView` objects using specified join, creating a new `DataStore` object with
    /// a subset of records from the two source `DataView`s according to the join parameters.
    ///
    /// Note that since this is creating a new `DataStore` object, it will be allocated new data to
    /// store the contents of the joined `DataView`s.
    pub fn join<Join, RLabels, RFrames>(
        &self,
        right: &DataView<RLabels, RFrames>,
    ) -> <Self as SortMergeJoin<RLabels, RFrames, Join>>::Output
    where
        Self: SortMergeJoin<RLabels, RFrames, Join>,
    {
        SortMergeJoin::join(self, right)
        // match join.predicate {
        //     // TODO: implement hash join
        //     // Predicate::Equal => {
        //     //     hash_join(self, other, join)
        //     // },
        //     _ => {
        //         sort_merge_join(self, other, join)
        //     }
        // }
    }
}

impl<FrameIndex, Frame, Tail> UpdatePermutation for ViewFrameCons<FrameIndex, Frame, Tail>
where
    Frame: Valued<Value = Frame>,
    ValueOf<Frame>: UpdatePermutation,
    Tail: UpdatePermutation,
{
    fn update_permutation(mut self, order: &[usize]) -> Self {
        self.head = Labeled::from(self.head.value().update_permutation(order));
        self.tail = self.tail.update_permutation(order);
        self
    }
}

impl<Labels, Frames> DataView<Labels, Frames>
where
    Frames: UpdatePermutation,
{
    /// Sorts this `DataView` by the provided label. This sort is stable -- it preserves the
    /// original order of equal elements. Consumes the `DataView` and returns a `DataView`
    /// sorted by values from field identified by `Label` in ascending order, with missing (NA)
    /// values at the beginning of the order (considered to be of 'lesser' value than existing
    /// values).
    pub fn sort_by_label<Label>(mut self) -> Self
    where
        Self: SelectFieldByLabel<Label>,
        <Self as SelectFieldByLabel<Label>>::Output: SortOrder,
    {
        // find sort order for this field
        let sorted = self.field::<Label>().sort_order();
        // apply sort order to each frame
        self.frames = self.frames.update_permutation(&sorted);
        self
    }

    /// Sorts this `DataView` by the provided label. This sort is unstable -- it does not
    /// necessarily preserve the original order of equal elements, but may be faster. Consumes the
    /// `DataView` and returns a `DataView sorted by values from field identifier by `Label` in
    /// ascending order, with missing (NA) values at the beginning of the order (considered to be of
    /// 'lesser' value than existing values).
    pub fn sort_unstable_by_label<Label>(mut self) -> Self
    where
        Self: SelectFieldByLabel<Label>,
        <Self as SelectFieldByLabel<Label>>::Output: SortOrderUnstable,
    {
        // find sort order for this field
        let sorted = self.field::<Label>().sort_order_unstable();
        // apply sort order to each frame
        self.frames = self.frames.update_permutation(&sorted);
        self
    }

    /// Sorts this `DataView` by the provided label using a specific comparator. This sort is
    /// stable -- it preserves the original order of equal elements. Consumes the `DataView` and
    /// returns a `DataView sorted by values from field identifier by `Label` in ascending order,
    /// with missing (NA) values at the beginning of the order (considered to be of 'lesser' value
    /// than existing values).
    pub fn sort_by_label_comparator<Label, F>(mut self, compare: F) -> Self
    where
        Self: SelectFieldByLabel<Label>,
        <Self as SelectFieldByLabel<Label>>::Output: SortOrderComparator<F>,
    {
        // find sort order for this field
        let sorted = self.field::<Label>().sort_order_by(compare);
        // apply sort order to each frame
        self.frames = self.frames.update_permutation(&sorted);
        self
    }

    /// Sorts this `DataView` by the provided label using a specific comparator. This sort is
    /// unstable -- it does not necessarily preserve the original order of equal elements, but may
    /// be faster. Consumes the `DataView` and returns a `DataView sorted by values from field
    /// identifier by `Label` in ascending order, with missing (NA) values at the beginning of the
    /// order (considered to be of 'lesser' value than existing values).
    pub fn sort_unstable_by_label_comparator<Label, F>(mut self, compare: F) -> Self
    where
        Self: SelectFieldByLabel<Label>,
        <Self as SelectFieldByLabel<Label>>::Output: SortOrderUnstableComparator<F>,
    {
        // find sort order for this field
        let sorted = self.field::<Label>().sort_order_unstable_by(compare);
        // apply sort order to each frame
        self.frames = self.frames.update_permutation(&sorted);
        self
    }

    /// Filters this `DataView` by `predicate` (a function mapping from `Value<&T>` to `bool` where
    /// `T` is the type of the field with label `Label`). Consumes this `DataView` and returns a new
    /// `DataView` such that only those rows where values within the field with label `Label`
    /// matching `predicate` remain.
    pub fn filter<Label, P>(mut self, predicate: P) -> Self
    where
        Self: SelectFieldByLabel<Label>,
        <Self as SelectFieldByLabel<Label>>::Output: FilterPerm<P>,
    {
        let perm = self.field::<Label>().filter_perm(predicate);
        self.frames = self.frames.update_permutation(&perm);
        self
    }
}

/// Trait for finding a cons-list of fields (implementing
/// [DataIndex](../access/trait.DataIndex.html)) from frames list `Frames` using the `LabelList`
/// list of labels. `LabelList` should consist of labels that exist within `Self` (this trait is
/// implemented by label lookup lists).
pub trait FieldList<LabelList, Frames> {
    /// Resultant cons-list of fields.
    type Output;

    /// Returns the cons-list of fields from the frames list `frames`.
    fn field_list(frames: &Frames) -> Self::Output;
}

impl<LabelList, Frames> FieldList<LabelList, Frames> for Nil {
    type Output = Nil;

    fn field_list(_frames: &Frames) -> Nil {
        Nil
    }
}

impl<LabelList, Frames, Label, FrameIndex, FrameLabel, Tail> FieldList<LabelList, Frames>
    for FrameLookupCons<Label, FrameIndex, FrameLabel, Tail>
where
    LabelList: Member<Label>,
    Self: FieldListPred<LabelList, Frames, <LabelList as Member<Label>>::IsMember>,
{
    type Output =
        <Self as FieldListPred<LabelList, Frames, <LabelList as Member<Label>>::IsMember>>::Output;

    fn field_list(frames: &Frames) -> Self::Output {
        Self::field_list_pred(frames)
    }
}

/// Helper trait for ([FieldList](trait.FieldList.html)). `IsMember` is whether or not the head of
/// `Self` is a member of the list `LabelList`.
pub trait FieldListPred<LabelList, Frames, IsMember> {
    /// The output field list.
    type Output;

    /// Returns the cons-list of fields from `frames`.
    fn field_list_pred(frames: &Frames) -> Self::Output;
}

impl<LabelList, Frames, Label, FrameIndex, FrameLabel, Tail> FieldListPred<LabelList, Frames, True>
    for FrameLookupCons<Label, FrameIndex, FrameLabel, Tail>
where
    Frames: SelectFieldFromLabels<Self, Label>,
    Tail: FieldList<LabelList, Frames>,
{
    type Output = Cons<
        <Frames as SelectFieldFromLabels<
            FrameLookupCons<Label, FrameIndex, FrameLabel, Tail>,
            Label,
        >>::Output,
        <Tail as FieldList<LabelList, Frames>>::Output,
    >;

    fn field_list_pred(frames: &Frames) -> Self::Output {
        Cons {
            head: SelectFieldFromLabels::<Self, Label>::select_field(frames),
            tail: Tail::field_list(frames),
        }
    }
}

impl<LabelList, Frames, Label, FrameIndex, FrameLabel, Tail> FieldListPred<LabelList, Frames, False>
    for FrameLookupCons<Label, FrameIndex, FrameLabel, Tail>
where
    Tail: FieldList<LabelList, Frames>,
{
    type Output = <Tail as FieldList<LabelList, Frames>>::Output;

    fn field_list_pred(frames: &Frames) -> Self::Output {
        Tail::field_list(frames)
    }
}

/// A struct representing a single record across the fields in the field list `Fields`.
#[derive(Debug, Clone)]
pub struct Record<'a, Fields> {
    // a field cons-list (returned from FieldList trait method)
    fields: &'a Fields,
    idx: usize,
}

impl<'a, Fields> Record<'a, Fields> {
    fn new(field_list: &'a Fields, idx: usize) -> Record<'a, Fields> {
        Record {
            fields: field_list,
            idx,
        }
    }
}

/// Trait for computing the hash of a single index (record) within a list of data fields.
pub trait HashIndex {
    /// Compute the hash of the values within this list of data fields with the index `idx`,
    /// updating the hash state.
    fn hash_index<H>(&self, idx: usize, state: &mut H)
    where
        H: Hasher;
}

impl<T, DI> HashIndex for Framed<T, DI>
where
    for<'a> Value<&'a T>: Hash,
    Self: DataIndex<DType = T>,
{
    fn hash_index<H>(&self, idx: usize, state: &mut H)
    where
        H: Hasher,
    {
        self.get_datum(idx).unwrap().hash(state);
    }
}

impl HashIndex for Nil {
    fn hash_index<H>(&self, _idx: usize, _state: &mut H)
    where
        H: Hasher,
    {
    }
}

impl<Head, Tail> HashIndex for Cons<Head, Tail>
where
    Head: HashIndex,
    Tail: HashIndex,
{
    fn hash_index<H>(&self, idx: usize, state: &mut H)
    where
        H: Hasher,
    {
        self.head.hash_index(idx, state);
        self.tail.hash_index(idx, state);
    }
}

impl<'a, Fields> Hash for Record<'a, Fields>
where
    Fields: HashIndex,
{
    fn hash<H>(&self, state: &mut H)
    where
        H: Hasher,
    {
        self.fields.hash_index(self.idx, state)
    }
}

/// Trait for computing equality of a single index (record) within a list of data fields.
pub trait PartialEqIndex {
    /// Returns equality of the values within this list of data fields with the index `idx`.
    fn eq_index(&self, other: &Self, idx: usize) -> bool;
}

impl<T, DI> PartialEqIndex for Framed<T, DI>
where
    for<'a> Value<&'a T>: PartialEq,
    Self: DataIndex<DType = T>,
{
    fn eq_index(&self, other: &Self, idx: usize) -> bool {
        self.get_datum(idx)
            .unwrap()
            .eq(&other.get_datum(idx).unwrap())
    }
}

impl PartialEqIndex for Nil {
    fn eq_index(&self, _other: &Nil, _idx: usize) -> bool {
        true
    }
}

impl<Head, Tail> PartialEqIndex for Cons<Head, Tail>
where
    Head: PartialEqIndex,
    Tail: PartialEqIndex,
{
    fn eq_index(&self, other: &Self, idx: usize) -> bool {
        self.head.eq_index(&other.head, idx) && self.tail.eq_index(&other.tail, idx)
    }
}

impl<'a, Fields> PartialEq for Record<'a, Fields>
where
    Fields: PartialEqIndex,
{
    fn eq(&self, other: &Self) -> bool {
        self.fields.eq_index(other.fields, self.idx)
    }
}

impl<'a, Fields> Eq for Record<'a, Fields> where Self: PartialEq {}

impl<'a> Display for Record<'a, Nil> {
    fn fmt(&self, _f: &mut Formatter) -> Result<(), fmt::Error> {
        Ok(())
    }
}

impl<'a, Head, Tail> Display for Record<'a, Cons<Head, Tail>>
where
    Head: DataIndex,
    <Head as DataIndex>::DType: Display,
    Record<'a, Tail>: Display,
{
    fn fmt(&self, f: &mut Formatter) -> Result<(), fmt::Error> {
        write!(f, "{},", self.fields.head.get_datum(self.idx).unwrap())?;
        Record {
            fields: &self.fields.tail,
            idx: self.idx,
        }
        .fmt(f)
    }
}

impl<Labels, Frames> DataView<Labels, Frames> {
    /// Returns a cons-list of fields (implementing [DataIndex](../access/trait.DataIndex.html))
    /// that match the labels in `LabelList`.
    pub fn field_list<LabelList>(&self) -> <Labels as FieldList<LabelList, Frames>>::Output
    where
        Labels: FieldList<LabelList, Frames>,
    {
        <Labels as FieldList<LabelList, Frames>>::field_list(&self.frames)
    }

    /// Computes the set of unique composite values among the fields in this `DataView` associated
    /// with labels in `LabelList`. Returns the indices of exemplar rows, one index for each unique
    /// value. Taken as a set, the values of the `LabelList`-labeled fields at the indices returned
    /// by this method represent all the possible combinations of values of these fields that exist
    /// in this `DataView`.
    ///
    /// Fields referenced by `LabelList` must implement `Hash`.
    pub fn unique_indices<LabelList>(&self) -> Vec<usize>
    where
        Self: Unique<LabelList>,
    {
        Unique::<LabelList>::unique_indices(self)
    }

    /// Computes the set of unique composite values among the fields in this `DataView` associated
    /// with labels in `LabelList`. Returns a new `DataView` with those specific sets of values. The
    /// returned `DataView` contains the values of the `LabelList`-labeled fields that represent
    /// all the possible combinations of values of these fields that exist in the original
    /// `DataView`.
    ///
    /// Fields referenced by `LabelList` must implement `Hash`.
    pub fn unique_values<LabelList>(&self) -> <Self as Unique<LabelList>>::Output
    where
        Self: Unique<LabelList>,
    {
        Unique::<LabelList>::unique_values(self)
    }
}

/// Trait providing methods for finding the unique indices and values for a
/// [DataView](struct.DataView.html). See the intrinsic methods
/// [unique_indices](struct.DataView.html#method.unique_indices) and
/// [unique_values](struct.DataView.html#method.unique_values) for more details.
pub trait Unique<LabelList> {
    /// Output of the `unique_values` method.
    type Output;
    /// Compute the unique indices for fields with labels in `LabelList`. See the intrinsic method
    /// [unique_indices](struct.DataView.html#method.unique_indices) for more details.
    fn unique_indices(&self) -> Vec<usize>;
    /// Compute the unique values for fields with labels in `LabelList`. See the intrinsic method
    /// [unique_values](struct.DataView.html#method.unique_values) for more details.
    fn unique_values(&self) -> Self::Output;
}

impl<Labels, Frames, LabelList> Unique<LabelList> for DataView<Labels, Frames>
where
    Labels: FieldList<LabelList, Frames>
        + HasLabels<LabelList>
        + LabelSubset<LabelList>
        + FrameIndexList,
    <Labels as FieldList<LabelList, Frames>>::Output: HashIndex + PartialEqIndex,
    <Labels as LabelSubset<LabelList>>::Output: Reorder<LabelList>,
    Frames: NRows + SubsetClone<<Labels as FrameIndexList>::LabelList>,
    <Frames as SubsetClone<<Labels as FrameIndexList>::LabelList>>::Output: UpdatePermutation,
{
    type Output = DataView<
        <<Labels as LabelSubset<LabelList>>::Output as Reorder<LabelList>>::Output,
        <Frames as SubsetClone<<Labels as FrameIndexList>::LabelList>>::Output,
    >;

    fn unique_indices(&self) -> Vec<usize> {
        let fl = self.field_list::<LabelList>();
        let mut indices = vec![];
        let mut set = HashSet::new();
        for i in 0..self.nrows() {
            let record = Record::new(&fl, i);
            if !set.contains(&record) {
                set.insert(record);
                indices.push(i);
            }
        }
        indices
    }

    fn unique_values(&self) -> Self::Output {
        let indices = self.unique_indices::<LabelList>();
        let new_frames = self.frames.subset_clone().update_permutation(&indices);
        DataView {
            _labels: PhantomData,
            frames: new_frames,
        }
    }
}

#[cfg(feature = "serialize")]
impl<Labels, Frames> Serialize for DataView<Labels, Frames>
where
    Labels: Len + SerializeViewField<Frames>,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let map = serializer.serialize_map(Some(self.nfields()))?;
        Labels::serialize_view_field(&self.frames, map)
    }
}

/// Trait for serializing a single field in a view. Used for serializing a
/// [DataView](struct.DataView.html).
#[cfg(feature = "serialize")]
pub trait SerializeViewField<Frames> {
    /// Serialize this single field using data from `frames`, and adding to map `SerializeMap`.
    fn serialize_view_field<M>(frames: &Frames, map: M) -> Result<M::Ok, M::Error>
    where
        M: SerializeMap;
}

#[cfg(feature = "serialize")]
impl<Frames> SerializeViewField<Frames> for Nil {
    fn serialize_view_field<M>(_frames: &Frames, map: M) -> Result<M::Ok, M::Error>
    where
        M: SerializeMap,
    {
        map.end()
    }
}

#[cfg(feature = "serialize")]
impl<Frames, Label, FrameIndex, FrameLabel, Tail> SerializeViewField<Frames>
    for FrameLookupCons<Label, FrameIndex, FrameLabel, Tail>
where
    Frames: SelectFieldFromLabels<Self, Label>,
    <Frames as SelectFieldFromLabels<Self, Label>>::Output: Serialize,
    Label: LabelName,
    Tail: SerializeViewField<Frames>,
{
    fn serialize_view_field<M>(frames: &Frames, mut map: M) -> Result<M::Ok, M::Error>
    where
        M: SerializeMap,
    {
        map.serialize_entry(
            Label::name(),
            &SelectFieldFromLabels::<Self, Label>::select_field(frames),
        )?;
        Tail::serialize_view_field(frames, map)
    }
}

impl<Labels, Frames> DataView<Labels, Frames> {
    /// Creates a new a `DataView` that accesses source data in a different way, viewing the data
    /// as a series of identifier / value pairs instead of a having values in multiple
    /// related fields.
    ///
    /// This is useful when converting a data table in a wide format where several fields represent
    /// different instances of some quantity to a long format where each record only has one
    /// instance of the appropriate value.
    ///
    /// The type parameter `MeltLabels` is a [LabelCons](../label/type.LabelCons.html) list of the
    /// labels of the fields containing the values to 'melt'. `NameLabel` is the desired label for
    /// the new identifier field, which will contain the `String` identifiers for where a record's
    /// value originally came from. `ValueLabel` is the desired label for the new value field, which
    /// will contain the values associated with each of the corresponding `String` identifiers.
    /// `HoldLabels` should be left for the compiler to infer using `_` -- it specifies the
    /// remaining fields that are not affected by this method.
    ///
    /// Since the values from the fields denoted in `MeltLabels` will all be combined into one field
    /// they must be the same data type.
    ///
    /// The resultant `DataView` will be have the following field order: all the fields with labels
    /// in `HoldLabels`, the `NameLabel` field, then the `ValueLabel` field.
    ///
    /// # Example
    /// Let us consider a table of employee salaries with the tablespace:
    /// ```
    /// # #[macro_use] extern crate agnes;
    /// tablespace![
    ///     table salary {
    ///         EmpId: u64,
    ///         Year2010: f64,
    ///         Year2011: f64,
    ///         Year2012: f64,
    ///         Year2013: f64,
    ///         Year2014: f64,
    ///     }
    /// ];
    /// ```
    /// which, when first loaded from the source file, looks like this:
    /// ```text
    ///  EmpId | Year2010 | Year2011 | Year2012 | Year2013 | Year2014
    /// -------+----------+----------+----------+----------+----------
    ///  0     | 1500     | 1600     | 1700     | 1850     | 2000
    ///  1     | 900      | 920      | 940      | 940      | 970
    ///  2     | 600      | 800      | 900      | 1020     | 1100
    /// ```
    /// While this is a valid way to store and present this data, there are definitely cases where
    /// you might want to have the different years separated into different records instead of
    /// having a column for each year. That's what `melt` is for!
    ///
    /// For the first step, we need to create new labels for `melt`'s `NameLabel` and `ValueLabel`
    /// type arguments. The `NameLabel` will be filled in with `String` identifiers for the field
    /// a data point came from, and the `ValueLabel` will be filled with the data values themselves.
    /// We can add these two labels to our previous `tablespace` call.
    ///
    /// Next, after we load the original data, we call `melt`:
    /// ```
    /// # #[macro_use] extern crate agnes;
    /// tablespace![
    ///     table salary {
    ///         EmpId: u64,
    ///         Year2010: f64,
    ///         Year2011: f64,
    ///         Year2012: f64,
    ///         Year2013: f64,
    ///         Year2014: f64,
    ///         SalaryYear: String,
    ///         Salary: f64,
    ///     }
    /// ];
    /// #
    /// # use salary::*;
    /// # use agnes::{store, cons::Nil};
    /// #
    /// fn main() {
    /// #     let orig_table = store::DataStore::<Nil>::empty()
    /// #         .push_back_cloned_from_iter::<EmpId, _, _, _>(&[0u64, 1u64, 2u64])
    /// #         .push_back_cloned_from_iter::<Year2010, _, _, _>(&[1500.0, 900.0, 600.0])
    /// #         .push_back_cloned_from_iter::<Year2011, _, _, _>(&[1600.0, 920.0, 800.0])
    /// #         .push_back_cloned_from_iter::<Year2012, _, _, _>(&[1700.0, 940.0, 900.0])
    /// #         .push_back_cloned_from_iter::<Year2013, _, _, _>(&[1850.0, 940.0, 1020.0])
    /// #         .push_back_cloned_from_iter::<Year2014, _, _, _>(&[2000.0, 970.0, 1100.0])
    /// #         .into_view();
    ///     // <load data into DataView orig_table>
    ///     // quick check to make sure we loaded the right table: with 3 rows, 6 fields
    ///     assert_eq!((orig_table.nrows(), orig_table.nfields()), (3, 6));
    ///
    ///     let melted_table = orig_table.melt::<
    ///         Labels![Year2010, Year2011, Year2012, Year2013, Year2014],
    ///         SalaryYear,
    ///         Salary,
    ///         _,
    ///     >();
    ///
    ///     // melted table should have 15 rows -- 5 for each of our 3 employees -- and 3 fields
    ///     assert_eq!((melted_table.nrows(), melted_table.nfields()), (15, 3));
    ///     assert_eq!(melted_table.fieldnames(), vec!["EmpId", "SalaryYear", "Salary"]);
    ///     println!("{}", melted_table);
    /// }
    /// ```
    /// This call to `melt` transforms the year fields into two new fields: one which contains the
    /// salary year (text) and has the label SalaryYear, and one which contains the salary values
    /// (floating-point) with the label Salary.
    ///
    /// The first type argument is the list of year labels we want to melt, the second is the
    /// new label for the year specifier field, the third is the new label for the year value field,
    /// and we let the compiler compute the list of labels we aren't melting (in this case, the
    /// EmpId field).
    ///
    /// As a result we should have a table with 15 rows, five for each of our three employees, and
    /// three fields: `EmpId`, `SalaryYear`, and `Salary`. This code should output:
    /// ```text
    ///  EmpId | SalaryYear | Salary
    /// -------+------------+--------
    ///  0     | Year2010   | 1500
    ///  0     | Year2011   | 1600
    ///  0     | Year2012   | 1700
    ///  0     | Year2013   | 1850
    ///  0     | Year2014   | 2000
    ///  1     | Year2010   | 900
    ///  1     | Year2011   | 920
    ///  1     | Year2012   | 940
    ///  1     | Year2013   | 940
    ///  1     | Year2014   | 970
    ///  2     | Year2010   | 600
    ///  2     | Year2011   | 800
    ///  2     | Year2012   | 900
    ///  2     | Year2013   | 1020
    ///  2     | Year2014   | 1100
    /// ```
    pub fn melt<MeltLabels, NameLabel, ValueLabel, HoldLabels>(
        &self,
    ) -> <Self as Melt<MeltLabels, NameLabel, ValueLabel, HoldLabels>>::Output
    where
        Self: Melt<MeltLabels, NameLabel, ValueLabel, HoldLabels>,
    {
        Melt::<MeltLabels, NameLabel, ValueLabel, HoldLabels>::melt(self)
    }
}

/// Trait providing the `melt` method for converting wide-format tables into long-format tables.
/// See the intrinsic method [melt](struct.DataView.html#method.melt) for more details.
pub trait Melt<MeltLabels, NameLabel, ValueLabel, HoldLabels> {
    /// Type produced by this melt method.
    type Output;

    /// Perform the 'melt' operation. See the intrinsic method
    /// [melt](struct.DataView.html#method.melt) for more details.
    fn melt(&self) -> Self::Output;
}

// type aliases to hopefully help with readability of Melt trait bounds.
type AsView<Orig> = <Orig as IntoView>::Output;
type AsFrame<Orig> = <Orig as IntoFrame>::Output;
type AsMeltFrame<Orig, ValueLabel> = <Orig as IntoMeltFrame<ValueLabel>>::Output;
type WithFrame<Orig, Added> = <Orig as AddFrame<Added>>::Output;

impl<Frames, Labels, MeltLabels, NameLabel, ValueLabel, HoldLabels>
    Melt<MeltLabels, NameLabel, ValueLabel, HoldLabels> for DataView<Labels, Frames>
where
    Frames: NRows + Clone,
    NameLabel: Debug,
    Labels: SetDiff<MeltLabels, Set = HoldLabels>,
    MeltLabels: Len + IntoStrFrame<NameLabel>,
    <MeltLabels as IntoStrFrame<NameLabel>>::Output: IntoView,
    Self: Subview<HoldLabels>,
    <Self as Subview<HoldLabels>>::Output: IntoFrame,
    <<Self as Subview<HoldLabels>>::Output as IntoFrame>::Output: UpdatePermutation,
    AsView<<MeltLabels as IntoStrFrame<NameLabel>>::Output>:
        AddFrame<<<Self as Subview<HoldLabels>>::Output as IntoFrame>::Output>,
    Self: Subview<MeltLabels>,
    <Self as Subview<MeltLabels>>::Output: IntoMeltFrame<ValueLabel>,
    WithFrame<
        AsView<<MeltLabels as IntoStrFrame<NameLabel>>::Output>,
        AsFrame<<Self as Subview<HoldLabels>>::Output>,
    >: AddFrame<AsMeltFrame<<Self as Subview<MeltLabels>>::Output, ValueLabel>>,
    HoldLabels: AssocLabels,
    <HoldLabels as AssocLabels>::Labels: Append<Labels![NameLabel, ValueLabel]>,
    WithFrame<
        WithFrame<
            AsView<<MeltLabels as IntoStrFrame<NameLabel>>::Output>,
            AsFrame<<Self as Subview<HoldLabels>>::Output>,
        >,
        AsMeltFrame<<Self as Subview<MeltLabels>>::Output, ValueLabel>,
    >: Subview<
        <<HoldLabels as AssocLabels>::Labels as Append<Labels![NameLabel, ValueLabel]>>::Appended,
    >,
{
    type Output = <WithFrame<
        WithFrame<
            AsView<<MeltLabels as IntoStrFrame<NameLabel>>::Output>,
            AsFrame<<Self as Subview<HoldLabels>>::Output>,
        >,
        AsMeltFrame<<Self as Subview<MeltLabels>>::Output, ValueLabel>,
    > as Subview<
        <<HoldLabels as AssocLabels>::Labels as Append<Labels![NameLabel, ValueLabel]>>::Appended,
    >>::Output;

    fn melt(&self) -> Self::Output {
        let premelt_nrows = self.nrows();
        let melt_len = MeltLabels::len();

        // create a new FieldData<String> with the label names from MeltLabels, and convert it into
        // a DataStore. Build a DataFrame around it with an index permutation that repeats the whole
        // list `premelt_nrows` times (e.g. [0,1,2,3,0,1,2,3,0,1,2,3,...,0,1,2,3])
        let melt_label_view = MeltLabels::into_repeated_str_frame(premelt_nrows).into_view();

        // create new frame based on the hold labels, with an index permutation that repeats
        // every element `melt_len` times
        // (e.g. [0,0,0,0,1,1,1,1,...,nrows-1,nrows-1,nrows-1,nrows-1])
        let hold_frame = Subview::<HoldLabels>::subview(self).into_frame();
        let mut hold_permutation = Vec::with_capacity(melt_len * premelt_nrows);
        for i in 0..premelt_nrows {
            for _ in 0..melt_len {
                hold_permutation.push(i);
            }
        }
        let hold_frame = hold_frame.update_permutation(&hold_permutation);
        let label_hold_dv = melt_label_view.add_frame(hold_frame);

        // create a new frame based on the MeltLabels as a LabelSpan-based frame (switches the
        // store field it draws from for each index)
        let melt_frame =
            IntoMeltFrame::<ValueLabel>::into_melt_frame(Subview::<MeltLabels>::subview(self));
        let final_dv = label_hold_dv.add_frame(melt_frame);
        // call subview to reorder fields properly
        final_dv.subview()
    }
}

impl<Labels, Frames> DataView<Labels, Frames> {
    /// Creates a new `DataView` that aggregates values in the `ValueLabel` field, grouping by
    /// records in the `KeyLabels` set of fields, and storing the result in a new field with
    /// label `AggLabel`. The resulting `DataView` will contain the `KeyLabels` fields and the
    /// newly constructed `AggLabel` field.
    ///
    /// For each unique set of key values in `KeyLabels`, this method will find all the records
    /// in the `DataView` which match, initialize an accumulator value with the argument `init`,
    /// and call `AggFunc` for each of the values in the `ValueLabel` field. `AggFunc` takes a
    /// mutable `AggType` value which it updates with the
    /// [Value](../field/enum.Value.html)s of type `DType` from the `ValueLabel` field.
    ///
    /// # Example
    /// Let's start with the data table which contains three fields: an employee ID `EmpId`, an
    /// annual salary `Salary`, and a text field denoting which year this salary took place:
    /// `SalaryYear`. This table (which is the final result of the example for the
    /// [melt](struct.DataView.html#method.melt) documentation) can be represented with the
    /// tablespace:
    /// ```
    /// # #[macro_use] extern crate agnes;
    /// tablespace![
    ///     table salary {
    ///         EmpId: u64,
    ///         SalaryYear: String,
    ///         Salary: f64,
    ///     }
    /// ];
    /// ```
    /// and data:
    /// ```text
    ///  EmpId | SalaryYear | Salary
    /// -------+------------+--------
    ///  0     | Year2010   | 1500
    ///  0     | Year2011   | 1600
    ///  0     | Year2012   | 1700
    ///  0     | Year2013   | 1850
    ///  0     | Year2014   | 2000
    ///  1     | Year2010   | 900
    ///  1     | Year2011   | 920
    ///  1     | Year2012   | 940
    ///  1     | Year2013   | 940
    ///  1     | Year2014   | 970
    ///  2     | Year2010   | 600
    ///  2     | Year2011   | 800
    ///  2     | Year2012   | 900
    ///  2     | Year2013   | 1020
    ///  2     | Year2014   | 1100
    /// ```
    /// For this example, let's compute the total yearly salary being payed out to all employees.
    /// Thus, we want to aggregate over each value in `SalaryYear`, and compute the sum of `Salary`.
    /// Therefore, our `KeyLabels` (our groups) would be `Labels![SalaryYear]` (since we can have
    /// more than one labels as our key, we need to use the label list-making macro
    /// [Labels](../macro.Labels.html)). Our `ValueLabel` (the value being summed) is `Salary`, and
    /// `AggLabel` will be a new label we need to add to our tablespace, which we'll call
    /// `TotalYearlySalary`.
    ///
    /// ```
    /// # #[macro_use] extern crate agnes;
    /// tablespace![
    ///     table salary {
    ///         EmpId: u64,
    ///         SalaryYear: String,
    ///         Salary: f64,
    ///         TotalYearlySalary: f64,
    ///     }
    /// ];
    /// #
    /// # use salary::*;
    /// #
    /// fn main() {
    /// #     let salary_table = table![
    /// #         EmpId = [0u64, 0, 0, 0, 0, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2];
    /// #         SalaryYear = [
    /// #             "Year2010", "Year2011", "Year2012", "Year2013", "Year2014",
    /// #             "Year2010", "Year2011", "Year2012", "Year2013", "Year2014",
    /// #             "Year2010", "Year2011", "Year2012", "Year2013", "Year2014"
    /// #         ];
    /// #         Salary = [
    /// #             1500.0, 1600.0, 1700.0, 1850.0, 2000.0,
    /// #             900.0, 920.0, 940.0, 940.0, 970.0,
    /// #             600.0, 800.0, 900.0, 1020.0, 1100.0
    /// #         ];
    /// #     ];
    ///     // <load data into DataView salary_table>
    ///     // salary table should have 15 rows -- 5 years of data for each of our 3 employees --
    ///     // and 3 fields (employee ID, salary year name, and salary value)
    ///     assert_eq!((salary_table.nrows(), salary_table.nfields()), (15, 3));
    ///     assert_eq!(salary_table.fieldnames(), vec!["EmpId", "SalaryYear", "Salary"]);
    ///     println!("{}", salary_table);
    ///
    ///     // compute the total salary per year, aggregated over employees
    ///     let agg_table = salary_table
    ///         .aggregate::<Labels![SalaryYear], Salary, TotalYearlySalary, _, _, _>(
    ///             0.0,
    ///             |accum, val| {
    ///                 *accum = *accum + val.unwrap_or(&0.0);
    ///             },
    ///         );
    ///
    ///     // we're left with five rows (one for each year of data), and two columns (year name and
    ///     // sum)
    ///     assert_eq!((agg_table.nrows(), agg_table.nfields()), (5, 2));
    ///     println!("{}", agg_table);
    /// }
    /// ```
    /// The call to aggregate takes two arguments: the value used to initialized each of our five
    /// aggregations (each of the five years), and a function which takes a mutable accumulator
    /// and the datum value (a [Value](../field/enum.Value.html) object) and updates the
    /// accumulator by adding the value. We use `unwrap_or` here to treat missing values a `0.0`.
    ///
    /// The resulting printed table should be:
    /// ```text
    ///  SalaryYear | TotalYearlySalary
    /// ------------+-------------------
    ///  Year2010   | 3000
    ///  Year2011   | 3320
    ///  Year2012   | 3540
    ///  Year2013   | 3810
    ///  Year2014   | 4070
    /// ```
    pub fn aggregate<KeyLabels, ValueLabel, AggLabel, DType, AggType, AggFunc>(
        &self,
        init: AggType,
        f: AggFunc,
    ) -> <Self as Aggregate<KeyLabels, ValueLabel, AggLabel, DType, AggType>>::Output
    where
        Self: Aggregate<KeyLabels, ValueLabel, AggLabel, DType, AggType>,
        AggFunc: Fn(&mut AggType, Value<&DType>),
    {
        Aggregate::<KeyLabels, ValueLabel, AggLabel, DType, AggType>::aggregate::<AggFunc>(
            self, init, f,
        )
    }
}

/// Trait providing the `aggregate` method for aggregating values over a specified grouping of
/// records. See the intrinsic method [aggregate](struct.DataView.html#method.aggregate) for more
/// details.
pub trait Aggregate<KeyLabels, ValueLabel, AggLabel, DType, AggType> {
    /// Type produced by this aggregate method.
    type Output;

    /// Perform the 'aggregate' operation. See the intrinsic method
    /// [aggregate](struct.DataView.html#method.aggregate) for more details.
    fn aggregate<AggFunc>(&self, init: AggType, f: AggFunc) -> Self::Output
    where
        AggFunc: Fn(&mut AggType, Value<&DType>);
}

impl<Labels, Frames, KeyLabels, ValueLabel, AggLabel, DType, AggType>
    Aggregate<KeyLabels, ValueLabel, AggLabel, DType, AggType> for DataView<Labels, Frames>
where
    Self: NRows + SelectFieldByLabel<ValueLabel, DType = DType>,
    Labels: FieldList<KeyLabels, Frames> + LabelSubset<KeyLabels> + FrameIndexList,
    <Labels as FieldList<KeyLabels, Frames>>::Output: HashIndex + PartialEqIndex,
    <Labels as LabelSubset<KeyLabels>>::Output: Reorder<KeyLabels>,
    AggType: Clone,
    // AggFunc: Fn(&mut AggType, Value<&<Self as SelectFieldByLabel<ValueLabel>>::DType>),
    FieldData<AggType>: IntoStore<AggLabel>,
    <FieldData<AggType> as IntoStore<AggLabel>>::Output: IntoFrame,
    Frames: NRows + SubsetClone<<Labels as FrameIndexList>::LabelList>,
    <Frames as SubsetClone<<Labels as FrameIndexList>::LabelList>>::Output: UpdatePermutation,
    DataView<
        <<Labels as LabelSubset<KeyLabels>>::Output as Reorder<KeyLabels>>::Output,
        <Frames as SubsetClone<<Labels as FrameIndexList>::LabelList>>::Output,
    >: AddFrame<<<FieldData<AggType> as IntoStore<AggLabel>>::Output as IntoFrame>::Output>,
{
    // output is KeyLabels, then single ValueLabel column
    type Output = <DataView<
        <<Labels as LabelSubset<KeyLabels>>::Output as Reorder<KeyLabels>>::Output,
        <Frames as SubsetClone<<Labels as FrameIndexList>::LabelList>>::Output,
    > as AddFrame<
        <<FieldData<AggType> as IntoStore<AggLabel>>::Output as IntoFrame>::Output,
    >>::Output;

    fn aggregate<AggFunc>(&self, init: AggType, f: AggFunc) -> Self::Output
    where
        AggFunc: Fn(&mut AggType, Value<&DType>),
    {
        let fl = self.field_list::<KeyLabels>();
        let values = self.field::<ValueLabel>();
        let mut map = HashMap::new();
        let mut indices = vec![];
        let mut aggregates = vec![];
        for i in 0..self.nrows() {
            let record = Record::new(&fl, i);
            let aggregates_idx = map.entry(record).or_insert_with(|| {
                indices.push(i);
                aggregates.push(init.clone());
                debug_assert_eq!(indices.len(), aggregates.len());
                indices.len() - 1
            });
            f(
                &mut aggregates[*aggregates_idx],
                values.get_datum(i).unwrap(),
            );
        }
        let agg_data: FieldData<_> = aggregates.into();
        let agg_frame = IntoStore::<AggLabel>::into_store(agg_data).into_frame();

        let record_frames = self.frames.subset_clone().update_permutation(&indices);

        DataView {
            _labels: PhantomData,
            frames: record_frames,
        }
        .add_frame(agg_frame)
    }
}

#[cfg(test)]
mod tests {
    use std::fmt::Debug;
    use std::path::Path;

    use csv_sniffer::metadata::Metadata;

    use super::*;
    use source::csv::{CsvReader, CsvSource, IntoCsvSrcSchema};

    #[cfg(feature = "test-utils")]
    use test_utils::*;

    use access::DataIndex;
    use error::*;

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
            Year1983: f64,
        }
    ];

    #[test]
    fn lookup_field() {
        let gdp_schema = schema![
            fieldname gdp::CountryName = "Country Name";
            fieldname gdp::CountryCode = "Country Code";
            fieldname gdp::Year1983 = "1983";
        ];

        let (mut csv_rdr, _metadata) = load_csv_file("gdp.csv", gdp_schema.clone());
        let ds = csv_rdr.read().unwrap();
        let view = ds.into_view();

        let country_name = view.field::<gdp::CountryName>();
        println!("{:?}", country_name);
    }

    #[test]
    fn generate_dataindex_cons() {
        let gdp_schema = schema![
            fieldname gdp::CountryName = "Country Name";
            fieldname gdp::CountryCode = "Country Code";
            fieldname gdp::Year1983 = "1983";
        ];

        let (mut csv_rdr, _metadata) = load_csv_file("gdp.csv", gdp_schema.clone());
        let ds = csv_rdr.read().unwrap();
        let view = ds.into_view();

        println!("{}", view);
    }

    #[cfg(feature = "test-utils")]
    #[test]
    fn merge() {
        let dv1 = sample_emp_table().into_view();
        let dv2 = sample_emp_table_extra().into_view();

        println!("{}", dv1);
        println!("{}", dv2);

        let merged_dv = dv1.merge(&dv2).unwrap();
        println!("{}", merged_dv);
        assert_eq!(merged_dv.nrows(), 7);
        assert_eq!(merged_dv.nfields(), 6);
        assert_eq!(
            merged_dv.fieldnames(),
            vec![
                "EmpId",
                "DeptId",
                "EmpName",
                "SalaryOffset",
                "DidTraining",
                "VacationHrs"
            ]
        );
    }

    #[cfg(feature = "test-utils")]
    #[test]
    fn merge_dimension_mismatch() {
        let dv1 = sample_emp_table().into_view();
        let dv2 = sample_dept_table().into_view();

        println!("{}", dv1);
        println!("{}", dv2);

        let merge_result = dv1.merge(&dv2);
        match merge_result {
            Ok(_) => {
                panic!("Merge was expected to fail (dimension mismatch), but succeeded");
            }
            Err(AgnesError::DimensionMismatch(_)) => { /* expected */ }
            Err(e) => {
                panic!("Incorrect error: {:?}", e);
            }
        };
    }
    #[cfg(feature = "test-utils")]
    tablespace![
        @continue(typenum::Add1<::test_utils::emp_table::Table>)

        pub table emp_table2 {
            EmpId: u64,
            DeptId: u64,
            EmpName: String,
        }
    ];

    #[cfg(feature = "test-utils")]
    #[test]
    fn merge_different_stores() {
        let dv1 = sample_emp_table().into_view();

        // would NOT COMPILE due to field name collision (see compile-fail/merge_errors test)
        // let merge_result = dv1.merge(&sample_emp_table().into_view());

        // if we use a sample employee table generated in another tablespace, however:
        let ds2: emp_table2::Store = sample_emp_table![];
        let dv2 = ds2.into_view();

        println!("{}", dv1);
        println!("{}", dv2);

        let merged_dv = dv1.merge(&dv2).unwrap();

        println!("{}", merged_dv);
        assert_eq!(merged_dv.nrows(), 7);
        assert_eq!(merged_dv.nfields(), 6);
        assert_eq!(
            merged_dv.fieldnames(),
            vec!["EmpId", "DeptId", "EmpName", "EmpId", "DeptId", "EmpName"]
        );
    }

    #[cfg(feature = "test-utils")]
    tablespace![
        @continue(typenum::Add1<::view::tests::emp_table2::Table>)

        pub table emp_table3 {
            EmployeeId: u64,
            DepartmentId: u64,
            EmployeeName: String,
        }
    ];

    #[cfg(feature = "test-utils")]
    #[test]
    fn relabel() {
        let dv1 = sample_emp_table().into_view();
        let dv2 = sample_emp_table().into_view();

        // much like merge_different_stores, this won't compile
        // let merged_dv = dv1.merge(&dv2).unwrap();
        // if we relabel all the fields in one of the two tables, however, we can go ahead and merge
        let dv1 = dv1.relabel::<emp_table::EmpId, emp_table3::EmployeeId>();
        let dv1 = dv1.relabel::<emp_table::DeptId, emp_table3::DepartmentId>();
        let dv1 = dv1.relabel::<emp_table::EmpName, emp_table3::EmployeeName>();

        let merged_dv = dv1.merge(&dv2).unwrap();
        println!("{}", merged_dv);
        assert_eq!(merged_dv.nrows(), 7);
        assert_eq!(merged_dv.nfields(), 6);
        assert_eq!(
            merged_dv.fieldnames(),
            vec![
                "EmployeeId",
                "DepartmentId",
                "EmployeeName",
                "EmpId",
                "DeptId",
                "EmpName"
            ]
        );
    }

    #[cfg(feature = "test-utils")]
    tablespace![
        @continue(typenum::Add1<::view::tests::emp_table3::Table>)

        pub table emp_table4 {
            EmplId: u64 = {"Employee Id"},
            DeptId: u64 = {"Department Id"},
            EmpName: String = {"Employee Name"},
        }
    ];

    #[cfg(feature = "test-utils")]
    #[test]
    fn name_change() {
        let ds: emp_table4::Store = sample_emp_table![];
        let dv = ds.into_view();

        println!("{}", dv);
        assert_eq!(dv.nrows(), 7);
        assert_eq!(dv.nfields(), 3);
        assert_eq!(
            dv.fieldnames(),
            vec!["Employee Id", "Department Id", "Employee Name"]
        );
    }

    #[cfg(feature = "test-utils")]
    #[test]
    fn fieldnames() {
        let ds = sample_emp_table();
        let dv = ds.into_view();
        assert_eq!(dv.fieldnames(), vec!["EmpId", "DeptId", "EmpName"]);
    }

    #[cfg(feature = "test-utils")]
    #[test]
    fn subview() {
        use test_utils::emp_table::*;
        let ds = sample_emp_table();
        let dv = ds.into_view();
        assert_eq!(dv.fieldnames(), vec!["EmpId", "DeptId", "EmpName"]);
        assert_eq!(dv.store_ref_counts(), vec![1]);
        assert_eq!(dv.nrows(), 7);
        assert_eq!(dv.nfields(), 3);

        let subdv1 = dv.v::<Labels![EmpId]>();
        assert_eq!(subdv1.fieldnames(), vec!["EmpId"]);
        assert_eq!(dv.store_ref_counts(), vec![2]);
        assert_eq!(subdv1.nrows(), 7);
        assert_eq!(subdv1.nfields(), 1);

        let subdv1 = dv.v::<Labels![EmpId]>();
        assert_eq!(subdv1.fieldnames(), vec!["EmpId"]);
        assert_eq!(dv.store_ref_counts(), vec![3]);
        assert_eq!(subdv1.nrows(), 7);
        assert_eq!(subdv1.nfields(), 1);

        let subdv2 = dv.v::<Labels![EmpId, DeptId]>();
        assert_eq!(subdv2.fieldnames(), vec!["EmpId", "DeptId"]);
        assert_eq!(dv.store_ref_counts(), vec![4]);
        assert_eq!(subdv2.nrows(), 7);
        assert_eq!(subdv2.nfields(), 2);

        let subdv2 = dv.v::<Labels![EmpId, DeptId]>();
        assert_eq!(subdv2.fieldnames(), vec!["EmpId", "DeptId"]);
        assert_eq!(dv.store_ref_counts(), vec![5]);
        assert_eq!(subdv2.nrows(), 7);
        assert_eq!(subdv2.nfields(), 2);

        let subdv3 = dv.v::<Labels![EmpId, DeptId, EmpName]>();
        assert_eq!(subdv3.fieldnames(), vec!["EmpId", "DeptId", "EmpName"]);
        assert_eq!(dv.store_ref_counts(), vec![6]);
        assert_eq!(subdv3.nrows(), 7);
        assert_eq!(subdv3.nfields(), 3);

        let subdv3 = dv.v::<Labels![EmpId, DeptId, EmpName]>();
        assert_eq!(subdv3.fieldnames(), vec!["EmpId", "DeptId", "EmpName"]);
        assert_eq!(dv.store_ref_counts(), vec![7]);
        assert_eq!(subdv3.nrows(), 7);
        assert_eq!(subdv3.nfields(), 3);

        // Subview of a subview
        let subdv4 = subdv2.v::<Labels![DeptId]>();
        assert_eq!(subdv4.fieldnames(), vec!["DeptId"]);
        assert_eq!(dv.store_ref_counts(), vec![8]);
        assert_eq!(subdv4.nrows(), 7);
        assert_eq!(subdv4.nfields(), 1);

        let subdv4 = subdv2.v::<Labels![EmpId]>();
        assert_eq!(subdv4.fieldnames(), vec!["EmpId"]);
        assert_eq!(dv.store_ref_counts(), vec![9]);
        assert_eq!(subdv4.nrows(), 7);
        assert_eq!(subdv4.nfields(), 1);
    }

    #[cfg(feature = "test-utils")]
    #[test]
    fn subview_merged() {
        use test_utils::emp_table::*;
        use test_utils::extra_emp::*;

        let dv = sample_merged_emp_table();
        println!("{:?}", dv.store_ref_counts());

        let subdv = dv.v::<Labels![DeptId, DidTraining]>();
        println!("{}", subdv);
        assert_eq!(subdv.fieldnames(), vec!["DeptId", "DidTraining"]);
        assert_eq!(dv.store_ref_counts(), vec![2, 2]);
        assert_eq!(subdv.nrows(), 7);
        assert_eq!(subdv.nfields(), 2);
    }

    #[cfg(feature = "test-utils")]
    #[test]
    fn subview_order() {
        use test_utils::emp_table::*;
        let dv = sample_emp_table().into_view();
        assert_eq!(dv.fieldnames(), vec!["EmpId", "DeptId", "EmpName"]);

        let subdv = dv.v::<Labels![DeptId, EmpId]>();
        assert_eq!(subdv.fieldnames(), vec!["DeptId", "EmpId"]);
    }

    #[cfg(feature = "test-utils")]
    #[test]
    fn subview_no_shared_frame() {
        // test to make sure frames aren't shared between view and subview
        use test_utils::emp_table::*;
        let dv = sample_emp_table().into_view();
        assert_eq!(dv.nrows(), 7);

        let subdv = dv.v::<Labels![DeptId, EmpId]>();
        assert_eq!(subdv.nrows(), 7);

        let newdv = dv.filter::<DeptId, _>(|val: Value<&_>| val.map_or(false, |&v| v == 1));
        assert_eq!(newdv.nrows(), 3);
        assert_eq!(subdv.nrows(), 7);
    }

    //TODO: multi-frame subview tests (which filter out no-longer-needed frames)

    #[cfg(feature = "test-utils")]
    #[test]
    fn sort() {
        use test_utils::emp_table::*;
        use test_utils::extra_emp::*;
        let orig_dv = sample_merged_emp_table();
        assert_eq!(orig_dv.nrows(), 7);

        // sort by name
        let dv1 = orig_dv.clone();
        let dv1 = dv1.sort_by_label::<EmpName>();
        assert_eq!(
            dv1.field::<EmpName>().to_vec(),
            vec!["Ann", "Bob", "Cara", "Jamie", "Louis", "Louise", "Sally"]
        );
        assert_eq!(dv1.field::<EmpId>().to_vec(), vec![10u64, 5, 6, 2, 8, 9, 0]);

        // re-sort by empid
        let dv2 = dv1.clone();
        let dv2 = dv2.sort_by_label::<EmpId>();
        assert_eq!(
            dv2.field::<EmpName>().to_vec(),
            vec!["Sally", "Jamie", "Bob", "Cara", "Louis", "Louise", "Ann"]
        );
        assert_eq!(dv2.field::<EmpId>().to_vec(), vec![0u64, 2, 5, 6, 8, 9, 10]);

        // make sure dv1 is still sorted by EmpName
        assert_eq!(
            dv1.field::<EmpName>().to_vec(),
            vec!["Ann", "Bob", "Cara", "Jamie", "Louis", "Louise", "Sally"]
        );
        assert_eq!(dv1.field::<EmpId>().to_vec(), vec![10u64, 5, 6, 2, 8, 9, 0]);

        // starting with sorted by name, sort by vacation hours
        let dv3 = dv1.clone();
        let dv3 = dv3.sort_by_label_comparator::<VacationHrs, _>(
            |left: Value<&f32>, right: Value<&f32>| left.partial_cmp(&right).unwrap(),
        );
        assert_eq!(
            dv3.field::<EmpName>().to_vec(),
            vec!["Louis", "Louise", "Cara", "Ann", "Sally", "Jamie", "Bob"]
        );
        assert_eq!(dv3.field::<EmpId>().to_vec(), vec![8u64, 9, 6, 10, 0, 2, 5]);
    }

    #[cfg(feature = "test-utils")]
    #[test]
    fn filter() {
        use test_utils::emp_table::*;
        let orig_dv = sample_emp_table().into_view();
        assert_eq!(orig_dv.nrows(), 7);

        // set filtering by department ID
        let dv1 = orig_dv.clone();
        let dv1 = dv1.filter::<DeptId, _>(|val: Value<&u64>| val == valref![1]);
        println!("{}", dv1);
        assert_eq!(dv1.nrows(), 3);
        assert_eq!(
            dv1.field::<EmpName>().to_vec(),
            vec!["Sally", "Bob", "Cara"]
        );

        // filter a second time
        let dv1 = dv1.filter::<EmpId, _>(|val: Value<&u64>| val >= valref![6]);
        assert_eq!(dv1.nrows(), 1);
        assert_eq!(dv1.field::<EmpName>().to_vec(), vec!["Cara"]);

        // that same filter on the original DV has different results
        let dv2 = orig_dv.clone();
        let dv2 = dv2.filter::<EmpId, _>(|val: Value<&u64>| val >= valref![6]);
        assert_eq!(dv2.nrows(), 4);
        assert_eq!(
            dv2.field::<EmpName>().to_vec(),
            vec!["Cara", "Louis", "Louise", "Ann"]
        );

        // let's try filtering by a different department on dv2
        let dv2 = dv2.filter::<DeptId, _>(|val: Value<&u64>| val == valref![4]);
        assert_eq!(dv2.nrows(), 2);
        assert_eq!(dv2.field::<EmpName>().to_vec(), vec!["Louise", "Ann"]);
    }

    #[cfg(feature = "test-utils")]
    #[test]
    fn filter_sort() {
        use test_utils::emp_table::*;
        use test_utils::extra_emp::*;
        let orig_dv = sample_merged_emp_table();
        assert_eq!(orig_dv.nrows(), 7);

        // start by filtering for employees with remaining vacation hours
        let dv1 = orig_dv.clone();
        let dv1 = dv1.filter::<VacationHrs, _>(|val: Value<&f32>| val >= 0.0);
        assert_eq!(dv1.nrows(), 6);
        // only Louis has negative hours, so rest of employees still remain
        assert_eq!(
            dv1.field::<EmpName>().to_vec(),
            vec!["Sally", "Jamie", "Bob", "Cara", "Louise", "Ann"]
        );

        // next, sort by employee name
        let dv2 = dv1.clone();
        let dv2 = dv2.sort_by_label::<EmpName>();
        assert_eq!(
            dv2.field::<EmpName>().to_vec(),
            vec!["Ann", "Bob", "Cara", "Jamie", "Louise", "Sally"]
        );

        // filter by people in department 1
        let dv3 = dv2.clone();
        let dv3 = dv3.filter::<DeptId, _>(|val: Value<&u64>| val == 1);
        assert_eq!(dv3.nrows(), 3);
        // should just be the people in department 1, in employee name order
        assert_eq!(
            dv3.field::<EmpName>().to_vec(),
            vec!["Bob", "Cara", "Sally"]
        );

        // check that dv1 still has the original ordering
        assert_eq!(
            dv1.field::<EmpName>().to_vec(),
            vec!["Sally", "Jamie", "Bob", "Cara", "Louise", "Ann"]
        );

        // ok, now filter dv1 by department 1
        let dv1 = dv1.filter::<DeptId, _>(|val: Value<&u64>| val == 1);
        assert_eq!(dv1.nrows(), 3);
        // should be the people in department 1, but in original name order
        assert_eq!(
            dv1.field::<EmpName>().to_vec(),
            vec!["Sally", "Bob", "Cara"]
        );

        // make sure dv2 hasn't been affected by any of the other changes
        assert_eq!(
            dv2.field::<EmpName>().to_vec(),
            vec!["Ann", "Bob", "Cara", "Jamie", "Louise", "Sally"]
        );
    }

    #[cfg(feature = "test-utils")]
    #[test]
    fn unique_single() {
        let ds = sample_emp_table();
        let dv = ds.into_view();
        println!("{}", dv);
        let uniques = dv.unique_indices::<Labels![emp_table::DeptId]>();
        println!("{:?}", uniques);
        // there are four unique department IDs (1, 2, 3, 4) at indices 0, 1, 4, 5.
        assert_eq!(uniques, vec![0, 1, 4, 5]);
        let dept_ids = dv.field::<emp_table::DeptId>();
        assert_eq![
            uniques
                .iter()
                .map(|&idx| dept_ids.get_datum(idx).unwrap())
                .collect::<Vec<_>>(),
            vec![1, 2, 3, 4]
        ];

        // can also check the unique department values with unique_values
        let unique_deptids = dv.unique_values::<Labels![emp_table::DeptId]>();
        println!("{}", unique_deptids);
        assert_eq!(
            unique_deptids.field::<emp_table::DeptId>().to_vec(),
            vec![1, 2, 3, 4]
        );
    }

    #[cfg(feature = "test-utils")]
    #[test]
    fn unique_composite() {
        let dv = sample_merged_emp_table();
        let uniq_indices =
            dv.unique_indices::<Labels![emp_table::DeptId, extra_emp::DidTraining]>();
        // the only repeat is index 3
        assert_eq!(uniq_indices, vec![0, 1, 2, 4, 5, 6]);

        let uniq_vals = dv.unique_values::<Labels![emp_table::DeptId, extra_emp::DidTraining]>();
        println!("{}", uniq_vals);
        assert_eq!(uniq_vals.fieldnames(), vec!["DeptId", "DidTraining",]);
        assert_eq!(
            uniq_vals.field::<emp_table::DeptId>().to_vec(),
            vec![1u64, 2, 1, 3, 4, 4]
        );
        assert_eq!(
            uniq_vals.field::<extra_emp::DidTraining>().to_vec(),
            vec![false, false, true, true, false, true]
        );

        // check ordering
        let uniq_vals = dv.unique_values::<Labels![extra_emp::DidTraining, emp_table::DeptId]>();
        println!("{}", uniq_vals);
        assert_eq!(uniq_vals.fieldnames(), vec!["DidTraining", "DeptId",]);
    }
}
