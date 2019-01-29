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
use std::collections::{VecDeque, HashSet};
use std::hash::{Hash, Hasher};
use std::fmt::{self, Display, Formatter};
use std::marker::PhantomData;

use prettytable as pt;
#[cfg(serialize)]
use serde::ser::{self, Serialize, SerializeMap, Serializer};

use access::*;
use error;
use frame::{DataFrame, Framed};

#[cfg(serialize)]
use frame::SerializedField;
use field::Value;
use join::*;
use cons::*;
use features::{
    DeriveCapabilities, Func, FuncDefault, Implemented, IsImplemented, PartialMap,
};
use fieldlist::{FieldPayloadCons};
use store::{AssocStorage, NRows};
use label::*;
use select::{FieldSelect, SelectFieldByLabel};

/// Cons-list of `DataFrame`s held by a `DataView. `FrameIndex` is simply an index used by
/// `FrameLookupCons` to look up `DataFrame`s for a specified `Label`, and `FrameFields` is
/// set of fields within the specified `DataFrame`.
pub type ViewFrameCons<FrameIndex, FrameFields, Tail> =
    LVCons<FrameIndex, DataFrame<FrameFields>, Tail>;

/// Cons-list of field labels along with the details necessary to look up that label in a
/// `DataView`'s `ViewFrameCons` cons-list of `DataFrame`s. The `FrameIndex` specifies the index
/// of the `DataFrame` containing the field labeled `Label` in the `ViewFrameCons`, and the
/// `FrameLabel` specifies the potentially-different (since `DataView` supposrt renaming fields)
/// `Label` within that `DataFrame`.
pub type FrameLookupCons<Label, FrameIndex, FrameLabel, Tail> =
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

pub struct FrameDetailMarkers<FrameIndex, FrameLabel> {
    _marker: PhantomData<(FrameIndex, FrameLabel)>,
}
pub trait FrameDetails {
    type FrameIndex: Identifier;
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

/// Allow `DataFrame`s to be pulled from `LVCons` as `Value`s
impl<FrameFields> SelfValued for DataFrame<FrameFields> where FrameFields: AssocStorage {}

impl<FrameIndex, FrameFields, Tail> NRows for ViewFrameCons<FrameIndex, FrameFields, Tail>
where
    FrameFields: AssocStorage,
    DataFrame<FrameFields>: NRows,
{
    fn nrows(&self) -> usize {
        self.head.value_ref().nrows()
    }
}

impl<Labels, Frames> DataView<Labels, Frames> {
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

pub trait FrameIndexList {
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
    /// Generate a new subview of this DataView. LabelList is an LabelCons.
    pub fn v<LabelList>(
        &self,
    ) -> DataView<
        <Labels as LabelSubset<LabelList>>::Output,
        <Frames as SubsetClone<<Labels as FrameIndexList>::LabelList>>::Output,
    >
    where
        Labels: HasLabels<LabelList> + LabelSubset<LabelList> + FrameIndexList,
        Frames: SubsetClone<<Labels as FrameIndexList>::LabelList>,
    {
        DataView {
            _labels: PhantomData,
            frames: self.frames.subset_clone(),
        }
    }
    pub fn subview<LabelList>(
        &self,
    ) -> DataView<
        <Labels as LabelSubset<LabelList>>::Output,
        <Frames as SubsetClone<<Labels as FrameIndexList>::LabelList>>::Output,
    >
    where
        Labels: HasLabels<LabelList> + LabelSubset<LabelList> + FrameIndexList,
        Frames: SubsetClone<<Labels as FrameIndexList>::LabelList>,
    {
        self.v::<LabelList>()
    }
}

impl<Labels, Frames> DataView<Labels, Frames>
where
    Frames: NRows,
{
    /// Number of rows in this data view
    pub fn nrows(&self) -> usize {
        self.frames.nrows()
    }
}

impl<Labels, Frames> DataView<Labels, Frames>
where
    Labels: Len,
    Frames: Len,
{
    /// Returns `true` if the DataView is empty (has no rows or has no fields)
    pub fn is_empty(&self) -> bool {
        length![Labels] == 0 || self.frames.is_empty()
    }
    /// Number of fields in this data view
    pub fn nfields(&self) -> usize {
        length![Labels]
    }
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
impl<FrameIndex, FrameFields, Tail> StoreRefCounts for ViewFrameCons<FrameIndex, FrameFields, Tail>
where
    FrameFields: AssocStorage,
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

pub trait FindFrameDetails<Label>: LookupMarkedElemByLabel<Label> {
    type FrameDetails: FrameDetails;
}
impl<Labels, Label> FindFrameDetails<Label> for Labels
where
    Labels: LookupMarkedElemByLabel<Label>,
    MarkerOfElemOf<Labels, Label>: FrameDetails,
{
    type FrameDetails = MarkerOfElemOf<Labels, Label>;
}
pub type FrameDetailsOf<Labels, Label> = <Labels as FindFrameDetails<Label>>::FrameDetails;
pub type FrameIndexOf<Labels, Label> =
    <<Labels as FindFrameDetails<Label>>::FrameDetails as FrameDetails>::FrameIndex;
pub type FrameLabelOf<Labels, Label> =
    <<Labels as FindFrameDetails<Label>>::FrameDetails as FrameDetails>::FrameLabel;

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
pub type FrameElemByFrameIndexOf<Frames, FrameIndex> =
    <Frames as LookupValuedElemByLabel<FrameIndex>>::Elem;
pub type FrameByFrameIndexOf<Frames, FrameIndex> =
    <FrameElemByFrameIndexOf<Frames, FrameIndex> as Valued>::Value;
pub type FrameElemOf<Frames, Labels, Label> =
    FrameElemByFrameIndexOf<Frames, FrameIndexOf<Labels, Label>>;
pub type FrameOf<Frames, Labels, Label> = <FrameElemOf<Frames, Labels, Label> as Valued>::Value;

pub type FieldFromFrameDetailsOf<Frames, FrameIndex, FrameLabel> =
    <FrameByFrameIndexOf<Frames, FrameIndex> as SelectFieldByLabel<FrameLabel>>::Output;
pub type FieldTypeFromFrameDetailsOf<Frames, FrameIndex, FrameLabel> =
    <FieldFromFrameDetailsOf<Frames, FrameIndex, FrameLabel> as DataIndex>::DType;

pub type FieldOf<Frames, Labels, Label> =
    <FrameOf<Frames, Labels, Label> as SelectFieldByLabel<FrameLabelOf<Labels, Label>>>::Output;
pub type FieldTypeOf<Frames, Labels, Label> = <FieldOf<Frames, Labels, Label> as DataIndex>::DType;

pub type VFieldOf<View, Label> = <View as SelectFieldByLabel<Label>>::Output;
pub type VFieldTypeOf<View, Label> = <VFieldOf<View, Label> as DataIndex>::DType;

pub trait SelectFieldFromLabels<Labels, Label> {
    type Output: DataIndex;
    fn select_field(&self) -> Self::Output;
}
impl<Labels, Frames, Label> SelectFieldFromLabels<Labels, Label> for Frames
where
    Labels: FindFrameDetails<Label>,
    Frames: FindFrame<Labels, Label>,
    FrameOf<Frames, Labels, Label>: SelectFieldByLabel<FrameLabelOf<Labels, Label>>,
    FieldOf<Frames, Labels, Label>: Typed + SelfValued + Clone,
    TypeOf<FieldOf<Frames, Labels, Label>>: fmt::Debug,
{
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
    type Output = <Frames as SelectFieldFromLabels<Labels, Label>>::Output;

    fn select_field(&self) -> Self::Output {
        SelectFieldFromLabels::<Labels, Label>::select_field(&self.frames)
    }
}

impl<Labels, Frames> FieldSelect for DataView<Labels, Frames> {}

pub type DataIndexCons<Label, DType, DI, Tail> = FieldPayloadCons<Label, DType, DI, Tail>;

pub trait AssocDataIndexCons<Labels> {
    type Output;
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

pub type AssocDataIndexConsOf<Labels, Frames> = <Frames as AssocDataIndexCons<Labels>>::Output;

const MAX_DISP_ROWS: usize = 1000;

impl<Labels, Frames> Display for DataView<Labels, Frames>
where
    Frames: Len + NRows + AssocDataIndexCons<Labels>,
    AssocDataIndexConsOf<Labels, Frames>: DeriveCapabilities<AddCellToRowFn>,
    Labels: StrLabels,
{
    fn fmt(&self, f: &mut Formatter) -> Result<(), fmt::Error> {
        if self.frames.is_empty() {
            return write!(f, "Empty DataView");
        }
        let mut table = pt::Table::new();

        let nrows = self.nrows();
        let mut func = AddCellToRowFn {
            rows: vec![pt::row::Row::empty(); nrows.min(MAX_DISP_ROWS)],
        };
        self.frames.assoc_data().derive().map(&mut func);
        for row in func.rows.drain(..) {
            table.add_row(row);
        }

        table.set_titles(<Labels as StrLabels>::labels().into());
        table.set_format(*pt::format::consts::FORMAT_NO_BORDER_LINE_SEPARATOR);

        Display::fmt(&table, f)
    }
}

/// Function (implementing [Func](../features/trait.Func.html)) that adds cells to
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
impl_addcell_is_impl![String f64 f32 u64 u32 i64 i32 bool];

impl<Labels, Frames> DataView<Labels, Frames> {
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

pub trait Relabel<TargetLabel, NewLabel> {
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

pub trait RelabelMatch<TargetLabel, NewLabel, Match> {
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

pub trait ViewMerge<Other> {
    type Output;
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

pub trait UpdatePermutation {
    fn update_permutation(&mut self, _order: &[usize]) {}
}
impl UpdatePermutation for Nil {}
impl<FrameIndex, FrameFields, Tail> UpdatePermutation
    for ViewFrameCons<FrameIndex, FrameFields, Tail>
where
    FrameFields: AssocStorage,
    Tail: UpdatePermutation,
{
    fn update_permutation(&mut self, order: &[usize]) {
        self.head.value_mut().update_permutation(order);
        self.tail.update_permutation(order);
    }
}

// TODO: idea for macro framework for applying function to each value in a cons-list
//
// list_apply![
//     self.frames;
//     |&mut value, order: &[usize]| {
//         value.update_permutation(order);
//         self.tail.update_permutation(order);
//     }
//     |order: &[usize]| {}
// ]

impl<Labels, Frames> DataView<Labels, Frames>
where
    Frames: UpdatePermutation,
{
    /// Sorts this `DataView` by the provided label. Returns the permutation (list of indices in
    /// sorted order) of values in field identified by `ident`.
    ///
    /// The resulting permutation denotes the order of values in ascending order, with missing (NA)
    /// values at the beginning of the order (considered to be of 'lesser' value than existing
    /// values).
    ///
    /// Fails if the field is not found in this `DataView`.
    pub fn sort_by_label<Label>(&mut self) -> Vec<usize>
    where
        Self: SelectFieldByLabel<Label>,
        <Self as SelectFieldByLabel<Label>>::Output: SortOrder,
    {
        // find sort order for this field
        let sorted = self.field::<Label>().sort_order();
        // apply sort order to each frame
        self.frames.update_permutation(&sorted);
        sorted
    }

    pub fn sort_unstable_by_label<Label>(&mut self) -> Vec<usize>
    where
        Self: SelectFieldByLabel<Label>,
        <Self as SelectFieldByLabel<Label>>::Output: SortOrderUnstable,
    {
        // find sort order for this field
        let sorted = self.field::<Label>().sort_order_unstable();
        // apply sort order to each frame
        self.frames.update_permutation(&sorted);
        sorted
    }

    pub fn sort_by_label_comparator<Label, F>(&mut self, compare: F) -> Vec<usize>
    where
        Self: SelectFieldByLabel<Label>,
        <Self as SelectFieldByLabel<Label>>::Output: SortOrderComparator<F>,
    {
        // find sort order for this field
        let sorted = self.field::<Label>().sort_order_by(compare);
        // apply sort order to each frame
        self.frames.update_permutation(&sorted);
        sorted
    }

    pub fn sort_unstable_by_label_comparator<Label, F>(&mut self, compare: F) -> Vec<usize>
    where
        Self: SelectFieldByLabel<Label>,
        <Self as SelectFieldByLabel<Label>>::Output: SortOrderUnstableComparator<F>,
    {
        // find sort order for this field
        let sorted = self.field::<Label>().sort_order_unstable_by(compare);
        // apply sort order to each frame
        self.frames.update_permutation(&sorted);
        sorted
    }

    pub fn filter<Label, P>(&mut self, predicate: P) -> Vec<usize>
    where
        Self: SelectFieldByLabel<Label>,
        <Self as SelectFieldByLabel<Label>>::Output: FilterPerm<P>,
    {
        let perm = self.field::<Label>().filter_perm(predicate);
        self.frames.update_permutation(&perm);
        perm
    }
}

pub trait FieldList<LabelList, Frames> {
    type Output;

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
    type Output = <
        Self as FieldListPred<LabelList, Frames, <LabelList as Member<Label>>::IsMember>
    >::Output;

    fn field_list(frames: &Frames) -> Self::Output {
        Self::field_list_pred(frames)
    }
}


pub trait FieldListPred<LabelList, Frames, IsMember> {
    type Output;

    fn field_list_pred(frames: &Frames) -> Self::Output;
}

impl<LabelList, Frames, Label, FrameIndex, FrameLabel, Tail>
    FieldListPred<LabelList, Frames, True>
    for FrameLookupCons<Label, FrameIndex, FrameLabel, Tail>
where
    Frames: SelectFieldFromLabels<Self, Label>,
    Tail: FieldList<LabelList, Frames>
{
    type Output = Cons<
        <Frames as SelectFieldFromLabels<
            FrameLookupCons<Label, FrameIndex, FrameLabel, Tail>,
            Label
        >>::Output,
        <Tail as FieldList<LabelList, Frames>>::Output
    >;

    fn field_list_pred(frames: &Frames) -> Self::Output {
        Cons {
            head: SelectFieldFromLabels::<Self, Label>::select_field(frames),
            tail: Tail::field_list(frames)
        }
    }
}


impl<LabelList, Frames, Label, FrameIndex, FrameLabel, Tail>
    FieldListPred<LabelList, Frames, False>
    for FrameLookupCons<Label, FrameIndex, FrameLabel, Tail>
where
    Tail: FieldList<LabelList, Frames>
{
    type Output = <Tail as FieldList<LabelList, Frames>>::Output;

    fn field_list_pred(frames: &Frames) -> Self::Output {
        Tail::field_list(frames)
    }
}


#[derive(Debug, Clone)]
pub struct Record<'a, Fields> {
    // a field cons-list (returned from FieldList trait method)
    fields: &'a Fields,
    idx: usize,
}

impl<'a, Fields> Record<'a, Fields> {
    fn new(field_list: &'a Fields, idx: usize) -> Record<'a, Fields> {
        Record { fields: field_list, idx }
    }
}

pub trait HashIndex {
    fn hash_index<H>(&self, idx: usize, state: &mut H)
    where
        H: Hasher;
}

impl<T> HashIndex for Framed<T>
where
    for<'a> Value<&'a T>: Hash,
    Self: DataIndex<DType=T>,
{
    fn hash_index<H>(&self, idx: usize, state: &mut H)
    where
        H: Hasher
    {
        self.get_datum(idx).unwrap().hash(state);
    }
}


impl HashIndex for Nil
{
    fn hash_index<H>(&self, _idx: usize, _state: &mut H)
    where
        H: Hasher
    {}
}

impl<Head, Tail> HashIndex for Cons<Head, Tail>
where
    Head: HashIndex,
    Tail: HashIndex
{
    fn hash_index<H>(&self, idx: usize, state: &mut H)
    where
        H: Hasher
    {
        self.head.hash_index(idx, state);
        self.tail.hash_index(idx, state);
    }
}

impl<'a, Fields> Hash for Record<'a, Fields>
where
    Fields: HashIndex
{
    fn hash<H>(&self, state: &mut H)
    where
        H: Hasher
    {
        self.fields.hash_index(self.idx, state)
    }
}

pub trait PartialEqIndex {
    fn eq_index(&self, other: &Self, idx: usize) -> bool;
}

impl<T> PartialEqIndex for Framed<T>
where
    for<'a> Value<&'a T>: PartialEq,
    Self: DataIndex<DType=T>,
{
    fn eq_index(&self, other: &Self, idx: usize) -> bool {
        self.get_datum(idx).unwrap().eq(&other.get_datum(idx).unwrap())
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
    Tail: PartialEqIndex
{
    fn eq_index(&self, other: &Self, idx: usize) -> bool {
        self.head.eq_index(&other.head, idx) && self.tail.eq_index(&other.tail, idx)
    }
}

impl<'a, Fields> PartialEq for Record<'a, Fields>
where
    Fields: PartialEqIndex
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
    Record<'a, Tail>: Display
{
    fn fmt(&self, f: &mut Formatter) -> Result<(), fmt::Error> {
        write!(f, "{},", self.fields.head.get_datum(self.idx).unwrap())?;
        Record { fields: &self.fields.tail, idx: self.idx }.fmt(f)
    }
}

impl<Labels, Frames> DataView<Labels, Frames> {
    pub fn field_list<LabelList>(&self)
        -> <Labels as FieldList<LabelList, Frames>>::Output
    where
        Labels: FieldList<LabelList, Frames>
    {
        <Labels as FieldList<LabelList, Frames>>::field_list(&self.frames)
    }

    pub fn unique_indices<LabelList>(&self) -> Vec<usize>
    where
        Labels: FieldList<LabelList, Frames>,
        <Labels as FieldList<LabelList, Frames>>::Output: HashIndex + PartialEqIndex,
        Frames: NRows,
    {
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

    pub fn unique_values<LabelList>(
        &self,
    ) -> DataView<
        <Labels as LabelSubset<LabelList>>::Output,
        <Frames as SubsetClone<<Labels as FrameIndexList>::LabelList>>::Output,
    >
    where
        Labels: HasLabels<LabelList> + LabelSubset<LabelList> + FrameIndexList,
        Frames: SubsetClone<<Labels as FrameIndexList>::LabelList>,
        <Frames as SubsetClone<<Labels as FrameIndexList>::LabelList>>::Output: UpdatePermutation,
        Labels: FieldList<LabelList, Frames>,
        <Labels as FieldList<LabelList, Frames>>::Output: HashIndex + PartialEqIndex,
        Frames: NRows,
    {
        let indices = self.unique_indices::<LabelList>();
        let mut new_frames = self.frames.subset_clone();
        new_frames.update_permutation(&indices);
        DataView {
            _labels: PhantomData,
            frames: new_frames,
        }
    }
}

#[cfg(serialize)]
impl<Idents, Frames> Serialize for DataView<Idents, Frames>
// where DTypes: DTypeList,
//       DTypes::Storage: MaxLen<DTypes> + FieldSerialize<DTypes>
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(Some(self.fields.len()))?;
        for field in self.fields.values() {
            map.serialize_entry(
                &field.rident.to_string(),
                &SerializedField::new(field.rident.ident.clone(), &self.frames[field.frame_idx]),
            )?;
        }
        map.end()
    }
}

/// Marker trait to denote an object that serializes into a vector format
#[cfg(serialize)]
pub trait SerializeAsVec: Serialize {}
#[cfg(serialize)]
impl<T> SerializeAsVec for Vec<T> where T: Serialize {}

/// A 'view' into a single field's data in a data frame. This is a specialty view used to serialize
/// a `DataView` as a single sequence instead of as a map.
#[cfg(serialize)]
#[derive(Debug, Clone)]
pub struct FieldView<Fields> {
    frame: DataFrame<Fields>,
    field: RFieldIdent,
}

#[cfg(serialize)]
impl<Fields> Serialize for FieldView<Fields>
// where DTypes: DTypeList,
//       DTypes::Storage: MaxLen<DTypes> + FieldSerialize<DTypes>,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        if self.frame.has_field(&self.field.ident) {
            SerializedField::new(self.field.to_renamed_field_ident(), &self.frame)
                .serialize(serializer)
        } else {
            Err(ser::Error::custom(format!(
                "missing field: {}",
                self.field.to_string()
            )))
        }
    }
}
#[cfg(serialize)]
impl<Fields> SerializeAsVec for FieldView<Fields>
// where DTypes: DTypeList,
//       DTypes::Storage: MaxLen<DTypes> + FieldSerialize<DTypes>
{
}

#[cfg(serialize)]
impl<Idents, Frames> DataView<Idents, Frames>
// where DTypes: DTypeList
{
    /// Create a `FieldView` object from a `DataView` object, if possible. Typically, use this on
    /// `DataView` objects with only a single field; however, if the `DataView` object has multiple
    /// fields, the first one will be used for this `FieldView`. Returns `None` if the `DataView`
    /// has no fields (is empty).
    pub fn as_fieldview(&self) -> Option<FieldView<Fields>> {
        if self.fields.is_empty() {
            None
        } else {
            // self.fields it not empty, so unwrap is safe
            let field = self.fields.values().next().unwrap();

            Some(FieldView {
                frame: self.frames[field.frame_idx].clone(),
                field: field.rident.clone(),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use csv_sniffer::metadata::Metadata;

    use super::*;
    use source::csv::{CsvReader, CsvSource, IntoCsvSrcSpec};

    #[cfg(feature = "test-utils")]
    use test_utils::*;

    use error::*;
    use access::{DataIndex};

    fn load_csv_file<Spec>(filename: &str, spec: Spec) -> (CsvReader<Spec::CsvSrcSpec>, Metadata)
    where
        Spec: IntoCsvSrcSpec,
    {
        let data_filepath = Path::new(file!()) // start as this file
            .parent()
            .unwrap() // navigate up to src directory
            .parent()
            .unwrap() // navigate up to root directory
            .join("tests") // navigate into integration tests directory
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
    fn lookup_field() {
        let gdp_spec = spec![
            fieldname gdp::CountryName = "Country Name";
            fieldname gdp::CountryCode = "Country Code";
            fieldname gdp::Year1983 = "1983";
        ];

        let (mut csv_rdr, _metadata) = load_csv_file("gdp.csv", gdp_spec.clone());
        let ds = csv_rdr.read().unwrap();
        let view = ds.into_view();

        let country_name = view.field::<gdp::CountryName>();
        println!("{:?}", country_name);
    }

    #[test]
    fn generate_dataindex_cons() {
        let gdp_spec = spec![
            fieldname gdp::CountryName = "Country Name";
            fieldname gdp::CountryCode = "Country Code";
            fieldname gdp::Year1983 = "1983";
        ];

        let (mut csv_rdr, _metadata) = load_csv_file("gdp.csv", gdp_spec.clone());
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

    namespace![
        pub namespace emp_table2: emp_table {
            field EmpId: u64;
            field DeptId: u64;
            field EmpName: String;
        }
    ];

    #[cfg(feature = "test-utils")]
    #[test]
    fn merge_different_stores() {
        let dv1 = sample_emp_table().into_view();

        // would NOT COMPILE due to field name collision (see compile-fail/merge_errors test)
        // let merge_result = dv1.merge(&sample_emp_table().into_view());

        // if we use a sample employee table generated in another namespace, however:
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

    namespace![
        pub namespace emp_table3: emp_table2
        {
            field EmployeeId: u64;
            field DepartmentId: u64;
            field EmployeeName: String;
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

    namespace![
        pub namespace emp_table4: emp_table3
        {
            field EmplId: u64 = "Employee Id";
            field DeptId: u64 = "Department Id";
            field EmpName: String = "Employee Name";
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

    //TODO: multi-frame subview tests (which filter out no-longer-needed frames)

    #[cfg(feature = "test-utils")]
    #[test]
    fn sort() {
        use test_utils::emp_table::*;
        use test_utils::extra_emp::*;
        let orig_dv = sample_merged_emp_table();
        assert_eq!(orig_dv.nrows(), 7);

        // sort by name
        let mut dv1 = orig_dv.clone();
        dv1.sort_by_label::<EmpName>();
        assert_eq!(
            dv1.field::<EmpName>().to_vec(),
            vec!["Ann", "Bob", "Cara", "Jamie", "Louis", "Louise", "Sally"]
        );
        assert_eq!(dv1.field::<EmpId>().to_vec(), vec![10u64, 5, 6, 2, 8, 9, 0]);

        // re-sort by empid
        let mut dv2 = dv1.clone();
        dv2.sort_by_label::<EmpId>();
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
        let mut dv3 = dv1.clone();
        dv3.sort_by_label_comparator::<VacationHrs, _>(|left: Value<&f32>, right: Value<&f32>| {
            left.partial_cmp(&right).unwrap()
        });
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
        let mut dv1 = orig_dv.clone();
        dv1.filter::<DeptId, _>(|val: Value<&u64>| val == valref![1]);
        println!("{}", dv1);
        assert_eq!(dv1.nrows(), 3);
        assert_eq!(
            dv1.field::<EmpName>().to_vec(),
            vec!["Sally", "Bob", "Cara"]
        );

        // filter a second time
        dv1.filter::<EmpId, _>(|val: Value<&u64>| val >= valref![6]);
        assert_eq!(dv1.nrows(), 1);
        assert_eq!(dv1.field::<EmpName>().to_vec(), vec!["Cara"]);

        // that same filter on the original DV has different results
        let mut dv2 = orig_dv.clone();
        dv2.filter::<EmpId, _>(|val: Value<&u64>| val >= valref![6]);
        assert_eq!(dv2.nrows(), 4);
        assert_eq!(
            dv2.field::<EmpName>().to_vec(),
            vec!["Cara", "Louis", "Louise", "Ann"]
        );

        // let's try filtering by a different department on dv2
        dv2.filter::<DeptId, _>(|val: Value<&u64>| val == valref![4]);
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
        let mut dv1 = orig_dv.clone();
        dv1.filter::<VacationHrs, _>(|val: Value<&f32>| val >= 0.0);
        assert_eq!(dv1.nrows(), 6);
        // only Louis has negative hours, so rest of employees still remain
        assert_eq!(
            dv1.field::<EmpName>().to_vec(),
            vec!["Sally", "Jamie", "Bob", "Cara", "Louise", "Ann"]
        );

        // next, sort by employee name
        let mut dv2 = dv1.clone();
        dv2.sort_by_label::<EmpName>();
        assert_eq!(
            dv2.field::<EmpName>().to_vec(),
            vec!["Ann", "Bob", "Cara", "Jamie", "Louise", "Sally"]
        );

        // filter by people in department 1
        let mut dv3 = dv2.clone();
        dv3.filter::<DeptId, _>(|val: Value<&u64>| val == 1);
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
        dv1.filter::<DeptId, _>(|val: Value<&u64>| val == 1);
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
    fn record() {
        let ds = sample_emp_table();
        let dv = ds.into_view();
        println!("{}", dv);
        let uniques = dv.unique_indices::<Labels![emp_table::DeptId]>();
        println!("{:?}", uniques);
        // there are four unique department IDs (1, 2, 3, 4) at indices 0, 1, 4, 5.
        assert_eq!(uniques, vec![0, 1, 4, 5]);
        let dept_ids = dv.field::<emp_table::DeptId>();
        assert_eq![
            uniques.iter().map(|&idx| dept_ids.get_datum(idx).unwrap()).collect::<Vec<_>>(),
            vec![1, 2, 3, 4]
        ];

        println!("{}", dv.unique_values::<Labels![emp_table::DeptId]>());
    }

    // #[test]
    // fn tmap_closure() {
    //     let orig_dv = sample_merged_emp_table();

    //     //FIXME: using a closure with tmap currently requires providing the type annotation for
    //     // the value passed into the closure (a DataIndex trait object). I believe this is related
    //     // to this issue: https://github.com/rust-lang/rust/issues/41078.
    //     let has_jamie = orig_dv.tmap(
    //         "EmpName",
    //         |data: &dyn DataIndex<Types, DType=String>|
    //             DataIterator::new(data).any(|emp_name| emp_name == "Jamie".to_string())
    //     ).unwrap();
    //     assert_eq!(has_jamie, true);

    //     let has_james = orig_dv.tmap(
    //         "EmpName",
    //         |data: &dyn DataIndex<Types, DType=String>|
    //             DataIterator::new(data).any(|emp_name| emp_name == "James".to_string())
    //     ).unwrap();
    //     assert_eq!(has_james, false);
    // }

    // #[test]
    // fn tmap_incorrect_field_type() {
    //     let orig_dv = sample_merged_emp_table();

    //     match orig_dv.tmap(
    //         "EmpName",
    //         |data: &dyn DataIndex<Types, DType=u64>|
    //             DataIterator::new(data).any(|emp_id| emp_id == 1)
    //     ) {
    //         Err(AgnesError::IncompatibleTypes { .. }) => {},
    //         Err(_) => { panic!["wrong error when calling tmap() with incorrect type"]; },
    //         Ok(_) => { panic!["expected error when calling tmap() with incorrect type, but \
    //                            received Ok"]; }
    //     }
    // }
}
