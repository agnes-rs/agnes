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
use std::collections::VecDeque;
use std::rc::Rc;
use std::marker::PhantomData;
use std::fmt::{self, Display, Formatter};

#[cfg(serialize)]
use serde::ser::{self, Serialize, Serializer, SerializeMap};
use prettytable as pt;

use access::DataIndex;
use error;
use frame::{DataFrame, Framed, FrameFields, FrameFieldsOf};

#[cfg(serialize)]
use frame::{SerializedField};
// use frame::{DataFrame, FramedMap, FramedTMap, FramedMapExt, Framed, FramedFunc, SerializedField};
// use filter::Filter;
use field::{Value};
use join::{Merge};// use join::{Join, sort_merge_join, compute_merged_frames, compute_merged_field_list, MergedFields,
//     MergeFields};
use fieldlist::{FieldCons, FieldPayloadCons};
use features::{Func, FuncDefault, Implemented, Unimplemented, IsImplemented,
    DeriveCapabilities, PartialMap};
use cons::*;
// use store::{DataStore, CopyIntoFn};
use store::{NRows, AssocStorage};
// use data_types::*;
// use apply::sort::{DtOrd, SortOrderFn};
use select::{SelectFieldByLabel, FieldSelect};
use label::*;

// `Labels` is `FrameLookupCons` cons-list. `Frames` is `ViewFrameCons` cons-list.
#[derive(Debug, Clone, Default)]
pub struct DataView<Labels, Frames>
{
    pub(crate) _labels: PhantomData<Labels>,
    pub(crate) frames: Frames,
}

pub type FrameLookupCons<Label, FrameLabel, Tail> = LMCons<Label, FrameLabel, Tail>;
pub type ViewFrameCons<FrameLabel, FrameFields, Tail>
    = LVCons<FrameLabel, DataFrame<FrameFields>, Tail>;

/// Allow `DataFrame`s to be pulled from `LVCons` as `Value`s
impl<FrameFields> SelfValued for DataFrame<FrameFields>
    where FrameFields: AssocStorage {}



// pub trait TyFrom<T>
// {
//     //FIXME: Should always be 'Self', but default associated types are unstable
//     type Output /* = Self */;
// }
// pub trait TyInto<T>
// {
//     //FIXME: Should always be 'T', but default associated types are unstable
//     type Output /* = T */;
// }
// // TyFrom implies TyInto
// impl<T, U> TyInto<U> for T where U: TyFrom<T>
// {
//     type Output = <U as TyFrom<T>>::Output;
// }
// // TyFrom is reflexive
// impl<T> TyFrom<T> for T
// {
//     type Output = T;
// }

// impl<LblIdx, LblName> TyFrom<Label<LblIdx, LblName>> for LabelCons<Label<LblIdx, LblName>, Nil>
// {
//     type Output = Self;
// }



impl<FrameLabel, FrameFields, Tail> NRows
    for ViewFrameCons<FrameLabel, FrameFields, Tail>
    where FrameFields: AssocStorage,
          DataFrame<FrameFields>: NRows,
{
    fn nrows(&self) -> usize {
        self.head.value_ref().nrows()
    }
}

impl<Labels, Frames> DataView<Labels, Frames>
{
    pub fn new(frames: Frames) -> DataView<Labels, Frames>
    {
        DataView {
            _labels: PhantomData,
            frames,
        }
    }
}

impl<Labels, Frames> DataView<Labels, Frames>
{
    /// Field names in this data view
    pub fn fieldnames<'a>(&'a self) -> Vec<&'a str>
        where Labels: StrLabels
    {
        <Labels as StrLabels>::labels().into()
    }
}

pub trait FieldLabelList
{
    type LabelList;
}

impl FieldLabelList for Nil
{
    type LabelList = Nil;
}

impl<Label, FrameLabel, Tail>
    FieldLabelList
    for FrameLookupCons<Label, FrameLabel, Tail>
    where
        Tail: FieldLabelList
{
    type LabelList = LCons<FrameLabel, <Tail as FieldLabelList>::LabelList>;
}


impl<Labels, Frames> DataView<Labels, Frames>
    where Frames: Clone
{
    /// Generate a new subview of this DataView. LabelList is an LabelCons.
    pub fn v<LabelList>(&self)
        -> DataView<
            <Labels as Filter<LabelList>>::Filtered,
            <Frames as FilterClone<<Labels as FieldLabelList>::LabelList>>::Filtered
        >
        where Labels: HasLabels<LabelList> + Filter<LabelList> + FieldLabelList,
              Frames: FilterClone<<Labels as FieldLabelList>::LabelList>
    {
        DataView {
            _labels: PhantomData,
            frames: self.frames.filter_clone(),
        }
    }
    pub fn subview<LabelList>(&self)
        -> DataView<
            <Labels as Filter<LabelList>>::Filtered,
            <Frames as FilterClone<<Labels as FieldLabelList>::LabelList>>::Filtered
        >
        where Labels: HasLabels<LabelList> + Filter<LabelList> + FieldLabelList,
              Frames: FilterClone<<Labels as FieldLabelList>::LabelList>
    {
        self.v::<LabelList>()
    }
}

impl<Labels, Frames> DataView<Labels, Frames>
    where Frames: NRows
{
    /// Number of rows in this data view
    pub fn nrows(&self) -> usize
    {
        self.frames.nrows()
    }
}

impl<Labels, Frames> DataView<Labels, Frames>
    where Labels: Len, Frames: Len
{
    /// Returns `true` if the DataView is empty (has no rows or has no fields)
    pub fn is_empty(&self) -> bool
    {
        length![Labels] == 0 || self.frames.is_empty()
    }
    /// Number of fields in this data view
    pub fn nfields(&self) -> usize
    {
        length![Labels]
    }
    /// Number of frames this data view covers
    pub fn nframes(&self) -> usize
    {
        length![Frames]
    }
}

#[cfg(test)]
pub trait StoreRefCounts
{
    fn store_ref_counts(&self) -> VecDeque<usize>;
}

#[cfg(test)]
impl StoreRefCounts for Nil
{
    fn store_ref_counts(&self) -> VecDeque<usize> { VecDeque::new() }
}
#[cfg(test)]
impl<FrameLabel, FrameFields, Tail> StoreRefCounts
    for ViewFrameCons<FrameLabel, FrameFields, Tail>
    where
        FrameFields: AssocStorage,
        Tail: StoreRefCounts
{
    fn store_ref_counts(&self) -> VecDeque<usize>
    {
        let mut previous = self.tail.store_ref_counts();
        previous.push_front(self.head.value_ref().store_ref_count());
        previous
    }
}

#[cfg(test)]
impl<Labels, Frames> DataView<Labels, Frames>
    where Frames: StoreRefCounts
{
    pub fn store_ref_counts(&self) -> VecDeque<usize>
    {
        Frames::store_ref_counts(&self.frames)
    }
}

pub trait FindFrameLabel<Label>:
    LookupMarkedElemByLabel<Label>
{
    type FrameLabel;
}
impl<Labels, Label> FindFrameLabel<Label>
    for Labels
    where
        Labels: LookupMarkedElemByLabel<Label>,
{
    type FrameLabel = MarkerOfElemOf<Labels, Label>;
}
pub type FrameLabelOf<Labels, Label> = <Labels as FindFrameLabel<Label>>::FrameLabel;

pub trait FindFrame<Labels, Label>:
    LookupValuedElemByLabel<FrameLabelOf<Labels, Label>>
    where
        Labels: FindFrameLabel<Label>
{}
impl<Frames, Labels, Label> FindFrame<Labels, Label>
    for Frames
    where
        Labels: FindFrameLabel<Label>,
        Frames: LookupValuedElemByLabel<FrameLabelOf<Labels, Label>>,
{}
pub type FrameOf<Frames, Labels, Label> =
    <<Frames as LookupValuedElemByLabel<FrameLabelOf<Labels, Label>>>::Elem as Valued>::Value;

pub type FieldOf<Frames, Labels, Label> =
    <FrameOf<Frames, Labels, Label> as SelectFieldByLabel<Label>>::Output;

pub trait SelectFieldFromLabels<Labels, Label>
{
    type Output: DataIndex;
    fn select_field(&self) -> Self::Output;
}
impl<Labels, Frames, Label> SelectFieldFromLabels<Labels, Label>
    for Frames
    where
        Labels: FindFrameLabel<Label>,
        Frames: FindFrame<Labels, Label>,
        FrameOf<Frames, Labels, Label>: SelectFieldByLabel<Label>,
        FieldOf<Frames, Labels, Label>: Typed + SelfValued + Clone,
        TypeOf<FieldOf<Frames, Labels, Label>>: fmt::Debug,
{
    type Output = FieldOf<Frames, Labels, Label>;

    fn select_field(&self) -> Self::Output
    {
        SelectFieldByLabel::<Label>::select_field(
            LookupValuedElemByLabel::<FrameLabelOf<Labels, Label>>::elem(self).value_ref()
        ).clone()
    }
}

impl<Labels, Frames, Label> SelectFieldByLabel<Label>
    for DataView<Labels, Frames>
    where
          Frames: SelectFieldFromLabels<Labels, Label>,
{
    type Output = <Frames as SelectFieldFromLabels<Labels, Label>>::Output;

    fn select_field(&self) -> Self::Output
    {
        SelectFieldFromLabels::<Labels, Label>::select_field(&self.frames)
    }
}

impl<Labels, Frames> FieldSelect for DataView<Labels, Frames> {}

// pub trait AssocFieldCons<Frames>
// {
//     type Output;
// }

// impl<'a, Frames> AssocFieldCons<Frames> for Nil
// {
//     type Output = Nil;
// }
// impl<Label, FrameLabel, LookupTail, Frames> AssocFieldCons<Frames>
//     for FrameLookupCons<Label, FrameLabel, LookupTail>
//     where Frames: LookupElemByLabel<FrameLabel>,
//           ElemOf<Frames, FrameLabel>: Valued,
//           ValueOfElemOf<Frames, FrameLabel>: FrameFields,
//           FrameFieldsOf<ValueOfElemOf<Frames, FrameLabel>>: LookupElemByLabel<Label>,
//           ElemOf<FrameFieldsOf<ValueOfElemOf<Frames, FrameLabel>>, Label>: Marked,
//           LookupTail: AssocFieldCons<Frames>,
//           // Frames: LookupFrameByLabel<Label>,
//           // <Frames as LookupFrameByLabel<Label>>::Frame: LookupElemByLabel<Label>,
//           // <<Frames as LookupFrameByLabel<Label>>::Frame as LookupElemByLabel<Label>>::Elem:
//           //   Typed
// {
//     type Output = FieldCons<
//         Label,
//         MarkerOf<ElemOf<FrameFieldsOf<ValueOfElemOf<Frames, FrameLabel>>, Label>>,
//         <LookupTail as AssocFieldCons<Frames>>::Output
//     >;
//     // type Output = FieldCons<
//     //     Label,
//     //     TypeOf<ElemOf<<Frames as LookupFrameByLabel<Label>>::Frame, Label>>,
//     //     <LookupTail as AssocFieldCons<Frames>>::Output
//     // >;
// }

// pub type AssocFieldsOf<L, F> = <L as AssocFieldCons<F>>::Output;

pub type DataIndexCons<Label, DType, DI, Tail> = FieldPayloadCons<Label, DType, DI, Tail>;

pub trait AssocDataIndexCons<Labels>
{
    type Output;
    fn assoc_data(&self) -> Self::Output;
}
impl<Frames> AssocDataIndexCons<Nil> for Frames
{
    type Output = Nil;
    fn assoc_data(&self) -> Nil { Nil }
}
impl<Label, FrameLabel, LookupTail, Frames>
    AssocDataIndexCons<FrameLookupCons<Label, FrameLabel, LookupTail>>
    for Frames
    where
          Self: SelectFieldFromLabels<FrameLookupCons<Label, FrameLabel, LookupTail>, Label>
            + AssocDataIndexCons<LookupTail>,
          <Self as SelectFieldFromLabels<FrameLookupCons<Label, FrameLabel, LookupTail>, Label>>
            ::Output: Typed
{
    type Output = DataIndexCons<
        Label,
        TypeOf<
            <Frames as SelectFieldFromLabels<FrameLookupCons<Label, FrameLabel, LookupTail>, Label>>
                ::Output
        >,
        <Frames as SelectFieldFromLabels<FrameLookupCons<Label, FrameLabel, LookupTail>, Label>>
            ::Output,
        <Frames as AssocDataIndexCons<LookupTail>>::Output
    >;
    fn assoc_data(&self) -> Self::Output
    {
        DataIndexCons
        {
            head: TypedValue::from(
                SelectFieldFromLabels::<FrameLookupCons<Label, FrameLabel, LookupTail>, Label>
                    ::select_field(self)
            ).into(),
            tail: AssocDataIndexCons::<LookupTail>::assoc_data(self)
        }
    }
}

pub type AssocDataIndexConsOf<Frames, Labels> = <Frames as AssocDataIndexCons<Labels>>::Output;


// pub trait AssocPartialMappable<Labels, F>
// {
//     type Output: DeriveCapabilities<F>;
//     fn fields_data(&self) -> Self::Output;
// }
// impl<'a, Frames, F> AssocPartialMappable<Nil, F> for Frames
// {
//     type Output = Nil;
//     fn fields_data(&self) -> Nil { Nil }
// }
// impl<'a, Label, FrameLabel, LookupTail, Frames, F>
//     AssocPartialMappable<FrameLookupCons<Label, FrameLabel, LookupTail>, F>
//     for Frames
//     where
//           Frames: LookupFrameByFrameLabel<FrameLabel> + AssocPartialMappable<LookupTail, F>,
//           FrameByFrameLabelOf<Frames, FrameLabel>: SelectFieldByLabel<Label> + FieldSelect,
//           <FrameByFrameLabelOf<Frames, FrameLabel> as SelectFieldByLabel<Label>>::Output:
//             'a + Typed + SelfValued,
//           DataIndexCons<
//             Label,
//             TypeOf<
//                 <FrameByFrameLabelOf<Frames, FrameLabel> as SelectFieldByLabel<Label>>
//                     ::Output
//             >,
//             <FrameByFrameLabelOf<Frames, FrameLabel> as SelectFieldByLabel<Label>>::Output,
//             <Frames as AssocPartialMappable<LookupTail, F>>::Output
//           >: DeriveCapabilities<F>
//             // + DataIndex<DType=TypeOf<
//             //     <FrameByFrameLabelOf<Frames, FrameLabel>
//             //         as SelectFieldByLabel<Label>>::Output
//             // >>
//           ,

//           // Frames: LookupElemByLabel<FrameLabel> + AssocPartialMappable<LookupTail>,
//           // ElemOf<Frames, FrameLabel>: Valued,
//           // ValueOfElemOf<Frames, FrameLabel>: FrameFields + SelectFieldByLabel<Label>
//           //   + FieldSelect,
//           // FrameFieldsOf<ValueOfElemOf<Frames, FrameLabel>>: LookupElemByLabel<Label>,
//           // ElemOf<FrameFieldsOf<ValueOfElemOf<Frames, FrameLabel>>, Label>: 'a + Typed,
// {
//     type Output = DataIndexCons<
//         Label,
//         // TypeOf<ElemOf<FrameFieldsOf<ValueOfElemOf<Frames, FrameLabel>>, Label>>,
//         TypeOf<
//             <FrameByFrameLabelOf<Frames, FrameLabel> as SelectFieldByLabel<Label>>::Output
//         >,
//         <FrameByFrameLabelOf<Frames, FrameLabel> as SelectFieldByLabel<Label>>::Output,
//         <Frames as AssocPartialMappable<LookupTail, F>>::Output
//     >;
//     fn fields_data(&self) -> Self::Output
//     {
//         DataIndexCons
//         {
//             head: TypedValue::from(
//                 LookupFrameByFrameLabel::<FrameLabel>::select_frame(self).field::<Label>()
//             ).into(),
//             tail: AssocPartialMappable::<LookupTail, F>::fields_data(self),
//         }
//     }
// }

// impl<Labels, Frames> DataView<Labels, Frames>
// {
//     fn assoc_fields_data<'a, F>(&'a self) -> <Frames as AssocPartialMappable<'a, Labels, F>>::Output
//         where Frames: AssocPartialMappable<'a, Labels, F>
//     {
//         AssocPartialMappable::<Labels, F>::fields_data(&self.frames)
//     }
// }

// pub type AssocPartialMappableOf<'a, Fields, Labels, F> =
//     <Fields as AssocPartialMappable<'a, Labels, F>>::Output;

// impl Display
//     for Nil
// {
//     fn fmt(&self, f: &mut Formatter) -> Result<(), fmt::Error>
//     {
//         writeln!(f, "Nil")
//     }
// }
// impl<'a, Label, DType, DI, Feature, Impl, Tail>
//     Display
//     for StorageCapabilitiesCons<'a, Label, DType, DI, Feature, Impl, Tail>
//     where Self: PartialMap<AddCellToRowFn>,
//           DI: DataIndex<DType=DType>,
// {
//     fn fmt(&self, f: &mut Formatter) -> Result<(), fmt::Error>
//     {

//     }
// }
// impl<Label, DType, DI, Tail>
//     Display
//     for DataIndexCons<Label, DType, DI, Tail>
//     where for<'a> Self: DeriveCapabilities<'a, DisplayFeat>,
//         for<'a> <Self as DeriveCapabilities<'a, DisplayFeat>>::Output: Display,
//         // DType: Debug,
//         DI: DataIndex<DType=DType> + SelfValued
// {
//     fn fmt(&self, f: &mut Formatter) -> Result<(), fmt::Error>
//     {
//         write!(f, "{}", self.derive())
//     }
// }

const MAX_DISP_ROWS: usize = 1000;

// pub trait CapableOf<Labels, F>:
//     AssocPartialMappable<Labels, F>
//     where <Self as AssocPartialMappable<Labels, F>>::Output: DeriveCapabilities<F>
// {}
// impl<Frames, Labels, F> CapableOf<Labels, F>
//     for Frames
//     where Frames: AssocPartialMappable<Labels, F>,
//           <Frames as AssocPartialMappable<Labels, F>>::Output: DeriveCapabilities<F>
// {}

// pub type FindFieldFromFrames<Frames, Labels, Label> =
    // <Frames as FindFieldFromFrames<Labels, Label>>::Output;

// pub type FindField<'a, Frames, FrameLabel, Label> =
//     <FrameByFrameLabelOf<'a, Frames, FrameLabel> as SelectFieldByLabel<Label>>::Output;
// pub type FindFieldType<'a, Frames, FrameLabel, Label> =
//     TypeOf<FindField<'a, Frames, FrameLabel, Label>>;

// pub trait PMap<'a, Labels, F>
// {
//     type Output;
//     fn pmap(&'a self, f: &mut F) -> Self::Output;
// }
// impl<'a, Frames, Impl, F> PMap<'a, Nil, F> for (Frames, Impl)
// {
//     type Output = Nil;
//     fn pmap(&self, _f: &mut F) -> Nil { Nil }
// }
// impl<'a, Frames, Label, FrameLabel, LookupTail, F>
//     PMap<'a, FrameLookupCons<Label, FrameLabel, LookupTail>, F>
//     for (Frames, Implemented)
//     where Frames: PMap<'a, LookupTail, F>,
//           Frames: LookupFrameByFrameLabel<'a, FrameLabel>,
//           FrameByFrameLabelOf<'a, Frames, FrameLabel>: SelectFieldByLabel<Label> + FieldSelect,
//           FindField<'a, Frames, FrameLabel, Label>: Typed + SelfValued,
//           FindField<'a, Frames, FrameLabel, Label>:
//             DataIndex<DType=FindFieldType<'a, Frames, FrameLabel, Label>>,
//           FindFieldType<'a, Frames, FrameLabel, Label>: IsImplemented<F, IsImpl=Implemented>,
//           F: Func<FindFieldType<'a, Frames, FrameLabel, Label>>
// {
//     type Output = ();
//     fn pmap(&self, f: &mut F) -> ()
//     {
//         f.call(&self.0.select_frame().field::<Label>());
//     }
// }
// impl<'a, Frames, Label, FrameLabel, LookupTail, F>
//     PMap<'a, FrameLookupCons<Label, FrameLabel, LookupTail>, F>
//     for (Frames, Unimplemented)
//     where Frames: PMap<'a, LookupTail, F>,
//           Frames: LookupFrameByFrameLabel<'a, FrameLabel>,
//           FrameByFrameLabelOf<'a, Frames, FrameLabel>: SelectFieldByLabel<Label> + FieldSelect,
//           FindField<'a, Frames, FrameLabel, Label>: Typed + SelfValued,
//           FindField<'a, Frames, FrameLabel, Label>:
//             DataIndex<DType=FindFieldType<'a, Frames, FrameLabel, Label>>,
//           FindFieldType<'a, Frames, FrameLabel, Label>: IsImplemented<F, IsImpl=Unimplemented>,
//           F: FuncDefault
// {
//     type Output = ();
//     fn pmap(&self, f: &mut F) -> ()
//     {
//         f.call();
//     }
// }

impl<Labels, Frames>
    Display
    for DataView<Labels, Frames>
    where Frames: Len + NRows + AssocDataIndexCons<Labels>,
          AssocDataIndexConsOf<Frames, Labels>: DeriveCapabilities<AddCellToRowFn>,
          Labels: StrLabels,
{
    fn fmt(&self, f: &mut Formatter) -> Result<(), fmt::Error> {
        if self.frames.is_empty() {
            return write!(f, "Empty DataView");
        }
        let mut table = pt::Table::new();

        let nrows = self.nrows();
        let mut func = AddCellToRowFn {
            rows: vec![pt::row::Row::empty(); nrows.min(MAX_DISP_ROWS)]
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

/// Function (implementing [Func](../data_types/trait.Func.html)) that adds cells to
/// `prettytable::row::Row`.
pub struct AddCellToRowFn
{
    rows: Vec<pt::row::Row>,
}
impl<DType> Func<DType> for AddCellToRowFn
    where for<'a> Value<&'a DType>: ToString,
{
    type Output = ();
    fn call<DI>(&mut self, data: &DI) -> Self::Output
        where DI: DataIndex<DType=DType>
    {
        debug_assert!(data.len() >= self.rows.len());
        for i in 0..self.rows.len() {
            self.rows[i].add_cell(cell!(data.get_datum(i).unwrap()));
        }
    }
}
impl FuncDefault for AddCellToRowFn
{
    type Output = ();
    fn call(&mut self) -> Self::Output
    {
        for i in 0..self.rows.len() {
            self.rows[i].add_cell(cell!());
        }
    }
}
impl IsImplemented<AddCellToRowFn> for String {
    type IsImpl = Implemented;
}
impl IsImplemented<AddCellToRowFn> for f64 {
    type IsImpl = Implemented;
}
impl IsImplemented<AddCellToRowFn> for f32 {
    type IsImpl = Implemented;
}
impl IsImplemented<AddCellToRowFn> for u64 {
    type IsImpl = Implemented;
}
impl IsImplemented<AddCellToRowFn> for i64 {
    type IsImpl = Implemented;
}
impl IsImplemented<AddCellToRowFn> for bool {
    type IsImpl = Implemented;
}

impl<Labels, Frames> DataView<Labels, Frames>
{
    /// merge this `DataView` with another `DataView` object, creating a new `DataView` with the
    /// same number of rows and all the fields from both source `DataView` objects.
    pub fn merge<RLabels, RFrames>(&self, right: &DataView<RLabels, RFrames>)
        -> error::Result<DataView<
            <Self as Merge<RLabels, RFrames>>::OutLabels,
            <Self as Merge<RLabels, RFrames>>::OutFrames,
        >>
        where
            Self: Merge<RLabels, RFrames>,
            RFrames: NRows,
            Frames: NRows,
            <Self as Merge<RLabels, RFrames>>::OutLabels: IsLabelSet,
        // where DTypes::Storage: MaxLen<DTypes>
    {
        if self.nrows() != right.nrows() {
            return Err(error::AgnesError::DimensionMismatch(
                "number of rows mismatch in merge".into()));
        }
        Ok(Merge::merge(self, right))

        // // compute merged stores (and mapping from 'other' store index references to combined
        // // store vector)
        // let (new_frames, other_store_indices) = compute_merged_frames(self, other);

        // // compute merged field list
        // let MergedFields { mut new_fields, .. } =
        //     compute_merged_field_list(self, other, &other_store_indices, None)?;
        // let new_fields = IndexMap::from_iter(new_fields.drain(..));
        // Ok(DataView {
        //     frames: new_frames,
        //     fields: new_fields
        // })
    }
}


























// #[derive(Debug, Clone)]

// impl<Ident, FrameIdx, Tail> IdentFrameIdxCons<Ident, FrameIdx, Tail>
// {
//     fn select_idents<IdentList, SearcherPool>(&self)
//         -> <Self as SelectIdents<IdentList, SearcherPool>>::Output
//         where Self: SelectIdents<IdentList, SearcherPool>
//     {
//         SelectIdents::select_ident(self)
//     }
// }

// pub trait SelectIdents<IdentList, SearcherPool>
// {
//     type Output;
// }
// impl<T> SelectIdents<Nil, Nil> for T {
//     type Output = Nil;
// }
// impl<Ident, IdTail, SIdent, SFrameIdx, STail, Searcher, SearchTail>
//     SelectIdents<IdentCons<Ident, IdTail>, Cons<Searcher, SearchTail>>
//     for IdentFrameIdxCons<SIdent, SFrameIdx, STail>
//     where IdentFrameIdxCons<SIdent, SFrameIdx, STail>: FrameIdxSelector<Ident, Searcher>,
//           STail: SelectIdents<IdTail, SearchTail>
// {
//     type Output = IdentFrameIdxCons<
//         Ident,
//         <IdentFrameIdxCons<SIdent, SFrameIdx, STail>
//             as FrameIdxSelector<Ident, Searcher>>::FrameIdx,
//         <STail as SelectIdents<IdTail, SearchTail>>::Output
//     >;
// }

// /// Trait to retrieve the Fields con-list for a particular FrameIdx.
// pub trait FrameSelector<FrameIdx, Searcher> {
//     type FrameFields: AssocStorage;
//     fn select_frame(&self) -> &DataFrame<Self::FrameFields>;
// }
// impl<TargetFrameIdx, NonTargetFrameIdx, TargetInTail, FrameFields, Tail>
//     FrameSelector<TargetFrameIdx, NoMatch<TargetInTail>>
//     for FrameCons<FrameFields, NonTargetFrameIdx, Tail>
//     where Tail: FrameSelector<TargetFrameIdx, TargetInTail>,
//           FrameFields: AssocStorage
// {
//     type FrameFields = <Tail as FrameSelector<TargetFrameIdx, TargetInTail>>::FrameFields;

//     fn select_frame(&self) -> &DataFrame<Self::FrameFields>
//     {
//         self.tail.select_frame()
//     }
// }
// impl<TargetFrameIdx, FrameFields, Tail>
//     FrameSelector<TargetFrameIdx, Match>
//     for FrameCons<FrameFields, TargetFrameIdx, Tail>
//     where FrameFields: AssocStorage
// {
//     type FrameFields = FrameFields;

//     fn select_frame(&self) -> &DataFrame<FrameFields>
//     {
//         &self.head.frame
//     }
// }

// pub trait AssocFieldCons<FrameS, FieldS>
// {
//     type Fields;
// }
// impl<FrameS, FieldS, Frames>
//     AssocFieldCons<FrameS, FieldS>
//     for (Nil, Frames)
// {
//     type Fields = Nil;
// }
// impl<Frames, Ident, FIdx, ICTail, FrameS, FieldS>
//     AssocFieldCons<FrameS, FieldS>
//     for (IdentFrameIdxCons<Ident, FIdx, ICTail>, Frames)
//     where Frames: FrameSelector<
//             FIdx,
//             FrameS
//           >,
//           Frames::FrameFields: FSelector<Ident, FieldS>,
//           (ICTail, Frames): AssocFieldCons<FrameS, FieldS>
// {
//     type Fields = FieldCons<
//         Ident,
//         <Frames::FrameFields as FSelector<Ident, FieldS>>::DType,
//         <(ICTail, Frames) as AssocFieldCons<FrameS, FieldS>>::Fields
//     >;
// }

// impl<FrameS, FieldS, Idents, Frames>
//     AssocFieldCons<FrameS, FieldS>
//     for DataView<Idents, Frames>
//     where (Idents, Frames): AssocFieldCons<FrameS, FieldS>
// {
//     type Fields = <(Idents, Frames) as AssocFieldCons<FrameS, FieldS>>::Fields;
// }

// // Idents is a 'IdentFrameIdxCons', Frames is a 'FrameCons'.
// #[derive(Debug, Clone, Default)]
// pub struct DataView<Idents, Frames>
// {
//     /// A cons-list of field identifiers and their associated frame indices
//     frame_indices: PhantomData<Idents>,
//     /// A cons-list of DataFrames
//     frames: Frames,
// }


// impl<Idents, Frames> DataView<Idents, Frames>
//     // where DTypes: DTypeList
// {
//     /// Generate a new subview of this DataView. IdentList is an IdentCons.
//     pub fn v<IdentList, SearcherPool>(&self)
//         -> DataView<<Idents as SelectIdents<IdentList, SearcherPool>>::Output, Frames>
//         where Idents: SelectIdents<IdentList, SearcherPool>
//     {
//         // select_idents builds a new IdentFrameIdxCons sublist from a IdentFrameIdxCons only
//         // containins the idents specified in the IdentList.
//         DataView {
//             // frame_indices: self.frame_indices.select_idents::<IdentList>(),
//             frame_indices: PhantomData,
//             frames: self.frames,
//         }

//         // let mut sub_fields = IndexMap::new();
//         // for ident in &s.into_field_list() {
//         //     if let Some(field) = self.fields.get(ident) {
//         //         sub_fields.insert(ident.clone(), field.clone());
//         //     }
//         // }
//         // DataView {
//         //     frames: self.frames.clone(),
//         //     fields: sub_fields,
//         // }
//     }
//     pub fn subview<IdentList, SearcherPool>(&self)
//         -> DataView<<Idents as SelectIdents<IdentList, SearcherPool>>::Output, Frames>
//         where Idents: SelectIdents<IdentList, SearcherPool>
//     {
//         self.v::<IdentList, SearcherPool>()
//     }
//     // /// Generate a new subview of this DataView, generating an error if a specified field does
//     // /// not exist.
//     // pub fn subview<L: IntoFieldList>(&self, s: L) -> error::Result<DataView<Fields>> {
//     //     let mut sub_fields = IndexMap::new();
//     //     for ident in &s.into_field_list() {
//     //         if let Some(field) = self.fields.get(ident) {
//     //             sub_fields.insert(ident.clone(), field.clone());
//     //         } else {
//     //             return Err(error::AgnesError::FieldNotFound(ident.clone()));
//     //         }
//     //     }
//     //     Ok(DataView {
//     //         frames: self.frames.clone(),
//     //         fields: sub_fields,
//     //     })
//     // }
//     /// Number of rows in this data view
//     pub fn nrows(&self) -> usize
//         where Frames: NRows,
//         // where DTypes::Storage: MaxLen<DTypes>
//     {
//         self.frames.nrows()
//         // if self.frames.is_empty() { 0 } else { self.frames[0].nrows() }
//     }
//     /// Returns `true` if the DataView is empty (has no rows or has no fields)
//     pub fn is_empty(&self) -> bool
//         where Frames: Len
//         // where DTypes::Storage: MaxLen<DTypes>
//     {
//         self.frames.is_empty()
//         // self.nrows() == 0
//     }
//     /// Number of fields in this data view
//     pub fn nfields(&self) -> usize
//         where Idents: Len
//     {
//         Idents::LEN
//         // self.fields.len()
//     }
// }

    // /// Field names in this data view
    // pub fn fieldnames(&self) -> Vec<&FieldIdent> {
    //     self.fields.keys().collect()
    // }
    // /// Return the field type for specified field
    // pub(crate) fn get_field_type(&self, ident: &FieldIdent) -> Option<DTypes::DType> {
    //     self.fields.get(ident).and_then(|view_field: &ViewField| {
    //         self.frames[view_field.frame_idx].get_field_type(&view_field.rident.ident)
    //     })
    // }

    // /// Returns `true` if this `DataView` contains this field.
    // pub fn has_field(&self, s: &FieldIdent) -> bool {
    //     self.fields.contains_key(s)
    // }

    // /// Rename a field of this DataView.
    // pub fn rename<T, U>(&mut self, orig: T, new: U) -> error::Result<()> where
    //     T: Into<FieldIdent>,
    //     U: Into<FieldIdent>
    // {
    //     let (orig, new) = (orig.into(), new.into());
    //     if self.fields.contains_key(&new) {
    //         return Err(error::AgnesError::FieldCollision(vec![new]));
    //     }
    //     let new_vf = if let Some(ref orig_vf) = self.fields.get(&orig) {
    //         ViewField {
    //             rident: RFieldIdent {
    //                 ident: orig_vf.rident.ident.clone(),
    //                 rename: Some(new.to_string())
    //             },
    //             frame_idx: orig_vf.frame_idx,
    //         }
    //     } else {
    //         return Err(error::AgnesError::FieldNotFound(orig));
    //     };
    //     self.fields.insert(new_vf.rident.to_renamed_field_ident(), new_vf);
    //     self.fields.swap_remove(&orig);
    //     Ok(())
    // }

    // /// Merge this `DataView` with another `DataView` object, creating a new `DataView` with the
    // /// same number of rows and all the fields from both source `DataView` objects.
    // pub fn merge<OtherFields>(&self, other: &DataView<OtherFields>)
    //     -> error::Result<DataView<OtherFields>>
    //     // where DTypes::Storage: MaxLen<DTypes>
    // {
    //     if self.nrows() != other.nrows() {
    //         return Err(error::AgnesError::DimensionMismatch(
    //             "number of rows mismatch in merge".into()));
    //     }

    //     // compute merged stores (and mapping from 'other' store index references to combined
    //     // store vector)
    //     let (new_frames, other_store_indices) = compute_merged_frames(self, other);

    //     // compute merged field list
    //     let MergedFields { mut new_fields, .. } =
    //         compute_merged_field_list(self, other, &other_store_indices, None)?;
    //     let new_fields = IndexMap::from_iter(new_fields.drain(..));
    //     Ok(DataView {
    //         frames: new_frames,
    //         fields: new_fields
    //     })
    // }

    // /// Combine two `DataView` objects using specified join, creating a new `DataStore` object with
    // /// a subset of records from the two source `DataView`s according to the join parameters.
    // ///
    // /// Note that since this is creating a new `DataStore` object, it will be allocated new data to
    // /// store the contents of the joined `DataView`s.
    // pub fn join<'b, RIdents, RFrames>(
    //     &'b self, other: &'b DataView<RIdents, RFrames>, join: &Join
    // )
    //     -> error::Result<DataStore<<(Frames, RFrames) as MergeFields>::OutFields>>
    //     // where T: 'static + DataType<DTypes> + DtOrd + PartialEq + Default,
    //     //       DTypes: 'b,
    //     //       DTypes::Storage: MaxLen<DTypes> + TypeSelector<DTypes, T> + CreateStorage
    //     //               + for<'c> FramedMapExt<DTypes, CopyIntoFn<'c, DTypes>, ()>
    // {
    //     match join.predicate {
    //         // TODO: implement hash join
    //         // Predicate::Equal => {
    //         //     hash_join(self, other, join)
    //         // },
    //         _ => {
    //             sort_merge_join(self, other, join)
    //         }
    //     }
    // }

    // /// Returns an iterator over the fields (as `FieldIdent`s of this DataView.
    // pub fn idents(&self) -> Keys<FieldIdent, ViewField> {
    //     self.fields.keys()
    // }

    // /// Applies the provided `Func` to the data in the specified field. This `Func` must be
    // /// implemented for all types in `DTypes`.
    // ///
    // /// Fails if the specified identifier is not found in this `DataView`.
    // pub fn map<F, FOut, I>(&self, ident: I, f: F)
    //     -> error::Result<FOut>
    //     where DTypes::Storage: FramedMap<DTypes, F, FOut>,
    //           I: Into<FieldIdent>
    // {
    //     let ident = ident.into();
    //     self.fields.get(&ident)
    //         .ok_or_else(|| error::AgnesError::FieldNotFound(ident))
    //         .and_then(|view_field: &ViewField| {
    //             self.frames[view_field.frame_idx].map(&view_field.rident.ident, f)
    //         })
    // }

    // /// Applies the provided `Func` to the data in the specified field. This `Func` must be
    // /// implemented for type `T`.
    // ///
    // /// Fails if the specified identifier is not found in this `DataView` or the incorrect type `T`
    // /// is used.
    // pub fn tmap<T, F, I>(&self, ident: I, f: F)
    //     -> error::Result<F::Output>
    //     where F: Func<DTypes, T>,
    //           T: DataType<DTypes>,
    //           DTypes::Storage: MaxLen<DTypes> + FramedTMap<DTypes, T, F>,
    //           I: Into<FieldIdent>,
    // {
    //     let ident = ident.into();
    //     self.fields.get(&ident)
    //         .ok_or_else(|| error::AgnesError::FieldNotFound(ident))
    //         .and_then(|view_field: &ViewField| {
    //             self.frames[view_field.frame_idx].tmap(&view_field.rident.ident, f)
    //         })
    // }

    // /// Applies the provided `FuncExt` to the data in the specified field. This `FuncExt` must be
    // /// implemented for all types in `DTypes`.
    // ///
    // /// Fails if the specified identifier is not found in this `DataView`.
    // pub fn map_ext<F, FOut, I>(&self, ident: I, f: F)
    //     -> error::Result<FOut>
    //     where DTypes::Storage: FramedMapExt<DTypes, F, FOut>,
    //           I: Into<FieldIdent>
    // {
    //     let ident = ident.into();
    //     self.fields.get(&ident)
    //         .ok_or_else(|| error::AgnesError::FieldNotFound(ident))
    //         .and_then(|view_field: &ViewField| {
    //             self.frames[view_field.frame_idx].map_ext(&view_field.rident.ident, f)
    //         })
    // }

    // /// Applies the provided `FuncPartial` to the data in the specified field.
    // ///
    // /// Fails if the specified identifier is not found in this `DataView`.
    // pub fn map_partial<F, I>(&self, ident: I, f: F)
    //     -> error::Result<Option<F::Output>>
    //     where DTypes::Storage: MapPartial<DTypes, F> + MaxLen<DTypes>,
    //           F: FuncPartial<DTypes>,
    //           I: Into<FieldIdent>
    // {
    //     let ident = ident.into();
    //     self.fields.get(&ident)
    //         .ok_or_else(|| error::AgnesError::FieldNotFound(ident))
    //         .and_then(|view_field: &ViewField| {
    //             self.frames[view_field.frame_idx].map_partial(&view_field.rident.ident, f)
    //         })
    // }

    // /// Returns the permutation (list of indices in sorted order) of values in field identified
    // /// by `ident`.
    // ///
    // /// The resulting permutation denotes the order of values in ascending order, with missing (NA)
    // /// values at the beginning of the order (considered to be of 'lesser' value than existing
    // /// values).
    // ///
    // /// Fails if the field is not found in this `DataView`.
    // pub fn sort_by<'a>(&'a mut self, ident: &FieldIdent) -> error::Result<Vec<usize>>
    //     where DTypes::Storage: FramedMap<DTypes, SortOrderFn, Vec<usize>>,
    // {
    //     match self.fields.get(ident) {
    //         Some(view_field) => {
    //             // filter on frame index this field belongs to
    //             let sorted = self.frames[view_field.frame_idx].sort_by(&view_field.rident.ident)?;
    //             // apply same filter to rest of frames
    //             for frame_idx in 0..self.frames.len() {
    //                 if frame_idx != view_field.frame_idx {
    //                     self.frames[frame_idx].update_permutation(&sorted);
    //                 }
    //             }
    //             Ok(sorted)
    //         },
    //         None => Err(error::AgnesError::FieldNotFound(ident.clone()))
    //     }
    // }


// impl<Idents, Frames> DataView<Idents, Frames>
// {
//     pub fn field<'a, Ident, FrameSearcher, Searcher>(&'a self)
//         -> Framed<
//             'a,
//             <Frames as FrameSelector<Ident, FrameSearcher>>::FrameFields,
//             <<Frames as FrameSelector<Ident, FrameSearcher>>::FrameFields
//                 as FSelector<Ident, Searcher>>::DType
//         >
//         where Frames: FrameSelector<Ident, FrameSearcher>,
//               <Frames as FrameSelector<Ident, FrameSearcher>>::FrameFields:
//                 FSelector<Ident, Searcher>
//     {
//         self.frames.select_frame().field::<Ident, _>()
//     }

// }

// impl<Idents, Frames> FSelect for DataView<Idents, Frames>
// {}

// impl<'a, Idents, Frames, Ident, Searcher, FrameSearcher> SelectField<'a, Ident>
//     for DataView<Idents, Frames>
//     where Frames: FrameSelector<Ident, FrameSearcher>,
//           <Frames as FrameSelector<Ident, FrameSearcher>>::FrameFields: FSelector<Ident, Searcher>
//           // T: 'static + DataType<DTypes>,
//           // DTypes: 'a + DTypeList,
//           // DTypes::Storage: MaxLen<DTypes>
// {
//     type Output = Framed<
//         'a,
//         Frames::FrameFields,
//         <<Frames as FrameSelector<Ident, FrameSearcher>>::FrameFields
//             as FSelector<Ident, Searcher>>::DType
//     >;

//     fn select_field(&'a self) -> Self::Output
//         // where DTypes: AssocTypes,
//         //       DTypes::Storage: TypeSelector<DTypes, T>
//     {
//         self.select_frame::<Ident, _>().field::<Ident, _>()
//         // <Frames as FrameSelector<Ident, Searcher>>::FrameIdx
//         // <Idents as FrameIdxSelector<Ident, _>>::FrameIdx
//         // self.frames[self.frame_indices.select::<Ident, _>()]
//             // .field::<Ident, _>()

//         // self.fields.get(&ident)
//         //     .ok_or_else(|| error::AgnesError::FieldNotFound(ident.clone()))
//         //     .and_then(|view_field: &ViewField| {
//         //         self.frames[view_field.frame_idx].select(view_field.rident.ident.clone())
//         //     })
//     }
// }

// impl<Fields> Filter for DataView<Fields>
//     where DataFrame<Fields>: Filter<Fields> + FSelect<Fields>,
//           // DTypes: DTypeList,
//           // DTypes::Storage: MaxLen<DTypes> + TypeSelector<DTypes, T>,
//           // T: 'static + DataType<DTypes>
// {
//     fn filter<Ident, FIdx, F>(&mut self, pred: F)
//         -> Vec<usize>
//         where Fields: FSelector<Ident, FIdx>,
//               F: Fn(&<Fields as FSelector<Ident, FIdx>>::DType) -> bool
//     {
//         let this_frame_idx = self.frame_indices.select::<Ident, _>();
//         let perm = self.frame[this_frame_idx].filter(pred);
//         for frame_idx in 0..self.frames.len() {
//             if frame_idx != this_frame_idx {
//                 self.frames[frame_idx].update_permutation(&perm);
//             }
//         }
//         perm
//         // let ident = ident.into();
//         // match self.fields.get(&ident) {
//         //     Some(view_field) => {
//         //         // filter on frame index this field belongs to
//         //         let filter = self.frames[view_field.frame_idx].filter(
//         //             &view_field.rident.ident, pred)?;
//         //         // apply same filter to rest of frames
//         //         for frame_idx in 0..self.frames.len() {
//         //             if frame_idx != view_field.frame_idx {
//         //                 self.frames[frame_idx].update_permutation(&filter);
//         //             }
//         //         }
//         //         Ok(filter)
//         //     },
//         //     None => Err(error::AgnesError::FieldNotFound(ident.clone()))
//         // }
//     }
// }

// impl<Idents, Fields> From<DataStore<Fields>> for DataView<Idents, FrameCons<Fields, Frame0, Nil>>
//     where Fields: AssocStorage,
// {
//     fn from(store: DataStore<Fields>) -> DataView<Idents, FrameCons<Fields, Frame0, Nil>> {

//         // let mut fields = IndexMap::new();
//         // for ident in store.fields() {
//         //     fields.insert(ident.clone(), ViewField {
//         //         rident: RFieldIdent {
//         //             ident: ident.clone(),
//         //             rename: None
//         //         },
//         //         frame_idx: 0,
//         //     });
//         // }

//         DataView {
//             frames: FrameCons {
//                 head: Frame {
//                     frame: store.into(),
//                     _frame_idx: PhantomData,
//                 },
//                 tail: Nil
//             },
//             frame_indices: PhantomData
//         }
//         // DataView {
//         //     frames: vec![store.into()],
//         //     frame_indices: <Fields as AttachPayload<GenerateViewCons, _>>::attach_payload(),
//         // }
//     }
// }

// impl<Idents, Frames> DataView<Idents, Frames>
// {
//     fn add_to_rows<FrameSearcher, FieldSearcher>(rows: &mut Vec<pt::row::Row>)
//         where Self: AddToRows<FrameSearcher, FieldSearcher>
//     {
//         AddToRows::add_to_rows_(rows)
//     }
// }
// trait AddToRows<FrameSearcher, FieldSearcher> {
//     fn add_to_rows_(rows: &mut Vec<pt::row::Row>);
// }
// impl<Idents, Frames, FrameSearcher, FieldSearcher>
//     AddToRows<FrameSearcher, FieldSearcher>
//     for DataView<Idents, Frames>
//     where DataView<Idents, Frames>: AssocFieldCons<FrameSearcher, FieldSearcher>
// {
//     fn add_to_rows_(rows: &mut Vec<pt::row::Row>) {
//         <Self as AssocFieldCons<_, _>>::Fields::adifjoa();
//     }
// }

// const MAX_DISP_ROWS: usize = 1000;
// // impl<Idents, Frames> Display for DataView<Idents, Frames>
// //     where Frames: Len + NRows,
//     // where DTypes: DTypeList,
//     //       DTypes::Storage: MaxLen<DTypes>
//     //               + for<'a, 'b> Map<DTypes, FramedFunc<'a, DTypes, AddCellToRowFn<'b>>, ()>
// impl<Idents, Frames>
//     Display
//     for DataView<Idents, Frames>
//     where Frames: Len + NRows,
// {
//     fn fmt(&self, f: &mut Formatter) -> Result<(), fmt::Error> {
//         if self.frames.is_empty() {
//             return write!(f, "Empty DataView");
//         }
//         // if self.frames.is_empty() || self.fields.is_empty() {
//         //     return write!(f, "Empty DataView");
//         // }
//         let nrows = self.frames.nrows();
//         let mut rows = vec![pt::row::Row::empty(); nrows.min(MAX_DISP_ROWS)];
//         Self::add_to_rows::<_, _>(&mut rows);
//         // <Self as AssocFieldCons<_, _>>::Fields::aidjofa();

//         partial_map![Self::Fields, AddCellToRowFn];
//         for view_field in self.fields.values() {
//             match self.frames[view_field.frame_idx].map(
//                 &view_field.rident.ident,
//                 AddCellToRowFn {
//                     rows: &mut rows,
//                 },
//             ) {
//                 Ok(_) => {},
//                 Err(e) => { return write!(f, "view display error: {}", e); },
//             }
//         }
//         let mut table = pt::Table::new();
//         table.set_titles(self.fields.keys().into());
//         for row in rows.drain(..) {
//             table.add_row(row);
//         }
//         table.set_format(*pt::format::consts::FORMAT_NO_BORDER_LINE_SEPARATOR);
//         Display::fmt(&table, f)
//     }
// }


// /// Function (implementing [Func](../data_types/trait.Func.html)) that adds cells to
// /// `prettytable::row::Row`.
// pub struct AddCellToRowFn<'a, 'b, Idents, Frames>
//     where Idents: 'b,
//           Frames: 'b,
// {
//     rows: &'a mut Vec<pt::row::Row>,
//     dv: &'b DataView<Idents, Frames>
// }
// impl<'a, 'b, DType, Idents, Frames> Func<DType> for AddCellToRowFn<'a, 'b, Idents, Frames>
// {
//     type Output = ();
//     fn call<Field>(&self) -> Self::Output
//         where Field: FieldTypes<DType=DType>
//     {
//         let type_data = self.dv.field::<Field::Ident, _, _>();
//         for i in 0..type_data.len().min(MAX_DISP_ROWS) {
//             self.rows[i].add_cell(cell!(type_data.get_datum(i).unwrap()));
//         }
//     }
// }
// impl<'a, 'b, Idents, Frames> FuncDefault for AddCellToRowFn<'a, 'b, Idents, Frames>
// {
//     type Output = ();
//     fn call<Field>(&self) -> Self::Output
//     {
//         let type_data = self.dv.field::<Field::Ident, _>();
//         for i in 0..type_data.len().min(MAX_DISP_ROWS) {
//             self.rows[i].add_cell(cell!());
//         }
//     }
// }
// impl<'a, 'b, Idents, Frames> ReqFeature for AddCellToRowFn<'a, 'b, Idents, Frames>
// {
//     type Feature = DisplayFeat;
// }

#[cfg(serialize)]
impl<Idents, Frames> Serialize for DataView<Idents, Frames>
    // where DTypes: DTypeList,
    //       DTypes::Storage: MaxLen<DTypes> + FieldSerialize<DTypes>
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error> where S: Serializer {
        let mut map = serializer.serialize_map(Some(self.fields.len()))?;
        for field in self.fields.values() {
            map.serialize_entry(&field.rident.to_string(), &SerializedField::new(
                field.rident.ident.clone(),
                &self.frames[field.frame_idx]
            ))?;
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
pub struct FieldView<Fields>
    // where DTypes: DTypeList
{
    frame: DataFrame<Fields>,
    field: RFieldIdent,
}

#[cfg(serialize)]
impl<Fields> Serialize for FieldView<Fields>
    // where DTypes: DTypeList,
    //       DTypes::Storage: MaxLen<DTypes> + FieldSerialize<DTypes>,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where S: Serializer {
        if self.frame.has_field(&self.field.ident) {
            SerializedField::new(
                self.field.to_renamed_field_ident(),
                &self.frame
            ).serialize(serializer)
        } else {
            Err(ser::Error::custom(format!("missing field: {}", self.field.to_string())))
        }
    }
}
#[cfg(serialize)]
impl<Fields> SerializeAsVec for FieldView<Fields>
    // where DTypes: DTypeList,
    //       DTypes::Storage: MaxLen<DTypes> + FieldSerialize<DTypes>
{}

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

// /// Conversion trait for converting into a vector of FieldIdents. Used for indexing into a
// /// `DataView`.
// pub trait IntoFieldList {
//     /// Convert into a `Vec<FieldIdents>`
//     fn into_field_list(self) -> Vec<FieldIdent>;
// }

// impl IntoFieldList for FieldIdent {
//     fn into_field_list(self) -> Vec<FieldIdent> {
//         vec![self]
//     }
// }
// impl<'a> IntoFieldList for &'a FieldIdent {
//     fn into_field_list(self) -> Vec<FieldIdent> {
//         vec![self.clone()]
//     }
// }
// impl IntoFieldList for Vec<FieldIdent> {
//     fn into_field_list(self) -> Vec<FieldIdent> {
//         self
//     }
// }
// impl<'a> IntoFieldList for Vec<&'a FieldIdent> {
//     fn into_field_list(self) -> Vec<FieldIdent> {
//         #[allow(unknown_lints, map_clone)]
//         self.iter().map(|&fi| fi.clone()).collect()
//     }
// }

// impl<'a> IntoFieldList for &'a str {
//     fn into_field_list(self) -> Vec<FieldIdent> {
//         vec![FieldIdent::Name(self.to_string())]
//     }
// }
// impl<'a> IntoFieldList for Vec<&'a str> {
//     fn into_field_list(mut self) -> Vec<FieldIdent> {
//         self.drain(..).map(|s| FieldIdent::Name(s.to_string())).collect()
//     }
// }
// macro_rules! impl_into_field_list_str_arr {
//     ($($val:expr),*) => {$(
//         impl<'a> IntoFieldList for [&'a str; $val] {
//             fn into_field_list(self) -> Vec<FieldIdent> {
//                 self.iter().map(|s| FieldIdent::Name(s.to_string())).collect()
//             }
//         }
//     )*}
// }
// impl_into_field_list_str_arr!(1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
//     11, 12, 13, 14, 15, 16, 17, 18, 19, 20);

// impl IntoFieldList for String {
//     fn into_field_list(self) -> Vec<FieldIdent> {
//         vec![FieldIdent::Name(self)]
//     }
// }
// impl IntoFieldList for Vec<String> {
//     fn into_field_list(mut self) -> Vec<FieldIdent> {
//         self.drain(..).map(FieldIdent::Name).collect()
//     }
// }
// macro_rules! impl_into_field_list_string_arr {
//     ($($val:expr),*) => {$(
//         impl IntoFieldList for [String; $val] {
//             fn into_field_list(self) -> Vec<FieldIdent> {
//                 // clone necessary since we're moving to the heap
//                 self.iter().map(|s| FieldIdent::Name(s.clone())).collect()
//             }
//         }
//     )*}
// }
// impl_into_field_list_string_arr!(1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
//     11, 12, 13, 14, 15, 16, 17, 18, 19, 20);

#[cfg(test)]
mod tests {
    // use test_utils::*;

    use std::path::Path;

    use typenum::uint::UTerm;
    use csv_sniffer::metadata::Metadata;

    use source::csv::{CsvSource, CsvReader, IntoCsvSrcSpec};
    use super::*;

    #[cfg(feature = "test-utils")]
    use test_utils::*;

    // use super::{DataView, Filter};
    use error::*;
    // use data_types::standard::*;
    use access::{DataIndex, DataIterator};

    fn load_csv_file<Spec>(filename: &str, spec: Spec)
        -> (CsvReader<Spec::CsvSrcSpec>, Metadata)
        where Spec: IntoCsvSrcSpec
    {
        let data_filepath = Path::new(file!()) // start as this file
            .parent().unwrap()                 // navigate up to src directory
            .parent().unwrap()                 // navigate up to root directory
            .join("tests")                     // navigate into integration tests directory            .join("data")                      // navigate into data directory
            .join("data")                      // navigate into data directory
            .join(filename);                   // navigate to target file

        let source = CsvSource::new(data_filepath.into()).unwrap();
        (CsvReader::new(&source, spec).unwrap(), source.metadata().clone())
    }

    #[test]
    fn lookup_field()
    {
        spec![
            let gdp_spec = {
                CountryName("Country Name"): String,
                CountryCode("Country Code"): String,
                Year1983("1983"): f64,
            };
        ];

        let (mut csv_rdr, _metadata) = load_csv_file("gdp.csv", gdp_spec.clone());
        let ds = csv_rdr.read().unwrap();
        let view = ds.into_view();

        // println!("{:?}", SelectFieldByLabel::<CountryName::Label>::select_field(&view));
        let country_name = view.field::<CountryName>();
        println!("{:?}", country_name);
    }

    #[test]
    fn generate_dataindex_cons()
    {
        spec![
            let gdp_spec = {
                CountryName("Country Name"): String,
                CountryCode("Country Code"): String,
                Year1983("1983"): f64,
            };
        ];

        let (mut csv_rdr, _metadata) = load_csv_file("gdp.csv", gdp_spec.clone());
        let ds = csv_rdr.read().unwrap();
        let view = ds.into_view();

        println!("{}", view);
    }

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
        assert_eq!(merged_dv.fieldnames(), vec!["EmpId", "DeptId", "EmpName", "SalaryOffset",
            "DidTraining", "VacationHrs"]);
    }

    #[test]
    fn merge_dimension_mismatch() {
        let dv1 = sample_emp_table().into_view();
        let dv2 = sample_dept_table().into_view();

        println!("{}", dv1);
        println!("{}", dv2);

        let merge_result = dv1.merge(&dv2);
        match merge_result {
            Ok(_) => { panic!("Merge was expected to fail (dimension mismatch), but succeeded"); },
            Err(AgnesError::DimensionMismatch(_)) => { /* expected */ },
            Err(e) => { panic!("Incorrect error: {:?}", e); },
        };
    }

    #[test]
    fn merge_field_collision() {
        let dv1 = sample_emp_table().into_view();
        let dv2 = sample_emp_table().into_view();

        println!("{}", dv1);
        println!("{}", dv2);

        // let merge_result = dv1.merge(&dv2);
    }

    // #[test]
    // fn rename() {
    //     let ds = sample_emp_table();
    //     let mut dv: DataView = ds.into();
    //     // println!("{}", dv);
    //     assert_field_lists_match(dv.fieldnames(), vec!["EmpId", "DeptId", "EmpName"]);
    //     dv.rename("DeptId", "Department Id").expect("rename failed");
    //     // println!("{}", dv);
    //     assert_field_lists_match(dv.fieldnames(), vec!["EmpId", "Department Id", "EmpName"]);
    //     dv.rename("Department Id", "DeptId").expect("rename failed");
    //     // println!("{}", dv);
    //     assert_field_lists_match(dv.fieldnames(), vec!["EmpId", "DeptId", "EmpName"]);
    // }

    // #[test]
    // fn rename_field_collision() {
    //     let ds = sample_emp_table();
    //     let mut dv: DataView = ds.into();
    //     // println!("{}", dv);
    //     assert_field_lists_match(dv.fieldnames(), vec!["EmpId", "DeptId", "EmpName"]);
    //     match dv.rename("DeptId", "EmpId") {
    //         Ok(_) => { panic!("Rename expected to fail (field collision), but succeeded"); },
    //         Err(AgnesError::FieldCollision(fields)) => {
    //             assert_eq!(fields, vec!["EmpId"]
    //                 .iter().map(|&s| FieldIdent::Name(s.into())).collect::<Vec<_>>());
    //         },
    //         Err(e) => { panic!("Incorrect error: {:?}", e); }
    //     }
    //     // println!("{}", dv);
    // }

    // #[test]
    // fn rename_field_not_found() {
    //     let ds = sample_emp_table();
    //     let mut dv: DataView = ds.into();
    //     // println!("{}", dv);
    //     assert_field_lists_match(dv.fieldnames(), vec!["EmpId", "DeptId", "EmpName"]);
    //     match dv.rename("Department Id", "DepartmentId") {
    //         Ok(_) => { panic!("Rename expected to fail (field not found), but succeeded"); },
    //         Err(AgnesError::FieldNotFound(field)) => {
    //             assert_eq!(field, FieldIdent::Name("Department Id".to_string()));
    //         },
    //         Err(e) => { panic!("Incorrect error: {:?}", e); }
    //     }
    //     // println!("{}", dv);
    // }

    #[test]
    #[cfg(feature = "test-utils")]
    fn fieldnames()
    {
        let ds = sample_emp_table();
        let dv = ds.into_view();
        assert_eq!(dv.fieldnames(), vec!["EmpId", "DeptId", "EmpName"]);
    }

    #[test]
    #[cfg(feature = "test-utils")]
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

    // #[test]
    // fn filter() {
    //     let ds = sample_emp_table();
    //     let orig_dv: DataView = ds.into();
    //     assert_eq!(orig_dv.nrows(), 7);

    //     // set filtering by department ID
    //     let mut dv1 = orig_dv.clone();
    //     dv1.filter("DeptId", |val: &u64| *val == 1).unwrap();
    //     println!("{}", dv1);
    //     assert_eq!(dv1.nrows(), 3);
    //     text::assert_dv_sorted_eq(&dv1, &"EmpName".into(), vec!["Sally", "Bob", "Cara"]);

    //     // filter a second time
    //     dv1.filter("EmpId", |val: &u64| *val >= 6).unwrap();
    //     assert_eq!(dv1.nrows(), 1);
    //     text::assert_dv_sorted_eq(&dv1, &"EmpName".into(), vec!["Cara"]);

    //     // that same filter on the original DV has different results
    //     let mut dv2 = orig_dv.clone();
    //     dv2.filter("EmpId", |val: &u64| *val >= 6).unwrap();
    //     assert_eq!(dv2.nrows(), 4);
    //     text::assert_dv_sorted_eq(&dv2, &"EmpName".into(), vec!["Cara", "Louis", "Louise", "Ann"]);

    //     // let's try filtering by a different department on dv2
    //     dv2.filter("DeptId", |val: &u64| *val == 4).unwrap();
    //     assert_eq!(dv2.nrows(), 2);
    //     text::assert_dv_sorted_eq(&dv2, &"EmpName".into(), vec!["Louise", "Ann"]);
    // }

    // #[test]
    // fn sort() {
    //     let orig_dv = sample_merged_emp_table();
    //     assert_eq!(orig_dv.nrows(), 7);

    //     // sort by name
    //     let mut dv1 = orig_dv.clone();
    //     dv1.sort_by(&"EmpName".into()).unwrap();
    //     text::assert_dv_eq_vec(&dv1, &"EmpName".into(),
    //         vec!["Ann", "Bob", "Cara", "Jamie", "Louis", "Louise", "Sally"]
    //     );
    //     unsigned::assert_dv_eq_vec(&dv1, &"EmpId".into(),
    //         vec![10u64, 5, 6, 2, 8, 9, 0]);

    //     // re-sort by empid
    //     let mut dv2 = dv1.clone();
    //     dv2.sort_by(&"EmpId".into()).unwrap();
    //     text::assert_dv_eq_vec(&dv2, &"EmpName".into(),
    //         vec!["Sally", "Jamie", "Bob", "Cara", "Louis", "Louise", "Ann"]
    //     );
    //     unsigned::assert_dv_eq_vec(&dv2, &"EmpId".into(),
    //         vec![0u64, 2, 5, 6, 8, 9, 10]);

    //     // make sure dv1 is still sorted by EmpName
    //     text::assert_dv_eq_vec(&dv1, &"EmpName".into(),
    //         vec!["Ann", "Bob", "Cara", "Jamie", "Louis", "Louise", "Sally"]
    //     );
    //     unsigned::assert_dv_eq_vec(&dv1, &"EmpId".into(),
    //         vec![10u64, 5, 6, 2, 8, 9, 0]);

    //     // starting with sorted by name, sort by vacation hours
    //     let mut dv3 = dv1.clone();
    //     dv3.sort_by(&"VacationHrs".into()).unwrap();
    //     text::assert_dv_eq_vec(&dv3, &"EmpName".into(),
    //         vec!["Louis", "Louise", "Cara", "Ann", "Sally", "Jamie", "Bob"]
    //     );
    //     unsigned::assert_dv_eq_vec(&dv3, &"EmpId".into(),
    //         vec![8u64, 9, 6, 10, 0, 2, 5]);
    // }

    // #[test]
    // fn filter_sort() {
    //     let orig_dv = sample_merged_emp_table();
    //     assert_eq!(orig_dv.nrows(), 7);

    //     // start by filtering for employees with remaining vacation hours
    //     let mut dv1 = orig_dv.clone();
    //     dv1.filter("VacationHrs", |&val: &f64| val >= 0.0).unwrap();
    //     assert_eq!(dv1.nrows(), 6);
    //     // only Louis has negative hours, so rest of employees still remain
    //     text::assert_dv_eq_vec(&dv1, &"EmpName".into(),
    //         vec!["Sally", "Jamie", "Bob", "Cara", "Louise", "Ann"]
    //     );

    //     // next, sort by employee name
    //     let mut dv2 = dv1.clone();
    //     dv2.sort_by(&"EmpName".into()).unwrap();
    //     text::assert_dv_eq_vec(&dv2, &"EmpName".into(),
    //         vec!["Ann", "Bob", "Cara", "Jamie", "Louise", "Sally"]
    //     );

    //     // filter by people in department 1
    //     let mut dv3 = dv2.clone();
    //     dv3.filter("DeptId", |&val: &u64| val == 1).unwrap();
    //     assert_eq!(dv3.nrows(), 3);
    //     // should just be the people in department 1, in employee name order
    //     text::assert_dv_eq_vec(&dv3, &"EmpName".into(), vec!["Bob", "Cara", "Sally"]);

    //     // check that dv1 still has the original ordering
    //     text::assert_dv_eq_vec(&dv1, &"EmpName".into(),
    //         vec!["Sally", "Jamie", "Bob", "Cara", "Louise", "Ann"]
    //     );

    //     // ok, now filter dv1 by department 1
    //     dv1.filter("DeptId", |&val: &u64| val == 1).unwrap();
    //     assert_eq!(dv1.nrows(), 3);
    //     // should be the people in department 1, but in original name order
    //     text::assert_dv_eq_vec(&dv1, &"EmpName".into(), vec!["Sally", "Bob", "Cara"]);

    //     // make sure dv2 hasn't been affected by any of the other changes
    //     text::assert_dv_eq_vec(&dv2, &"EmpName".into(),
    //         vec!["Ann", "Bob", "Cara", "Jamie", "Louise", "Sally"]
    //     );
    // }

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
