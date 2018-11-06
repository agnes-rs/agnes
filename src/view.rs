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
use std::marker::PhantomData;
use std::fmt::{self, Display, Formatter};
use std::iter::FromIterator;

use indexmap::IndexMap;
use indexmap::map::Keys;
use serde::ser::{self, Serialize, Serializer, SerializeMap};
use prettytable as pt;

use access::DataIndex;
use frame::{DataFrame, Framed, SerializedField};
// use frame::{DataFrame, FramedMap, FramedTMap, FramedMapExt, Framed, FramedFunc, SerializedField};
// use filter::Filter;
use field::{Value};
// use join::{Join, sort_merge_join, compute_merged_frames, compute_merged_field_list, MergedFields,
//     MergeFields};
use field::{FieldIdent, RFieldIdent};
use fieldlist::{FSelector, FieldCons, FieldPayloadCons, Field, PayloadGenerator, AttachPayload,
    FieldTypes, Next, AssocFieldCons};
use features::{Func, FuncDefault, ReqFeature, DisplayFeat};
use cons::*;
use error;
// use store::{DataStore, CopyIntoFn};
use store::{DataStore, AssocStorage};
// use data_types::*;
// use apply::sort::{DtOrd, SortOrderFn};
use select::{SelectField, FSelect};

/// A field in a `DataView`. Contains the (possibly-renamed) field identifier and the store index
/// with the underlying data.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ViewField {
    /// The field identifier, along with renaming information (if exists)
    pub rident: RFieldIdent,
    /// Frame index of the underlying data
    pub frame_idx: usize,
}

pub type FrameIndex = usize;

pub type ViewCons<Field, Tail> = FieldPayloadCons<Field, FrameIndex, Tail>;

pub trait AssocFrame {
    type Frame;
}
impl<Ident, FIdx, DType, Tail> AssocFrame for FieldCons<Ident, FIdx, DType, Tail>
    where Tail: AssocFrame
{
    type Frame = ViewCons<Field<Ident, FIdx, DType>, Tail::Frame>;
}
impl AssocFrame for Nil {
    type Frame = Nil;
}


// /// A 'view' into a data store. The primary struct for viewing and manipulating data.
// #[derive(Debug, Clone, Default)]
// pub struct DataView<Fields: AssocFrame>
//     // where DTypes: DTypeList
// {
//     pub(crate) frames: Vec<DataFrame<Fields>>,
//     frame_indices: Fields::AssocFrame,
//     // pub(crate) fields: IndexMap<FieldIdent, ViewField>,
// }

#[derive(Debug, Clone)]
pub struct Frame<FrameFields, FrameIdx> {
    frame: DataFrame<FrameFields>,
    _frame_idx: PhantomData<FrameIdx>
}

impl<FrameFields, FrameIdx> AssocFieldCons for Frame<FrameFields, FrameIdx> {
    type Fields = FrameFields;
}

pub type FrameCons<FrameFields, FrameIdx, Tail> = Cons<Frame<FrameFields, FrameIdx>, Tail>;

#[derive(Debug, Clone)]
pub struct IdentFrame<Ident, FIdx, FrameIdx> {
    _ident: PhantomData<Ident>,
    _fidx: PhantomData<FIdx>,
    _frameidx: PhantomData<FrameIdx>
}
pub type IdentFrameIdxCons<Ident, FIdx, FrameIdx, Tail>
    = Cons<IdentFrame<Ident, FIdx, FrameIdx>, Tail>;
pub type IdentCons<Ident, Tail> = Cons<PhantomData<Ident>, Tail>;

/// Trait for finding the associated FrameIdx for a field Ident.
pub trait FrameIdx<Ident, FrameIdx> {
    type FrameIdx;
}
impl<TargetIdent, NonTargetIdent, NonTargetFIdx, TargetFrameIdx, Tail>
    FrameIdx<TargetIdent, TargetFrameIdx>
    for IdentFrameIdxCons<NonTargetIdent, NonTargetFIdx, Next<TargetFrameIdx>, Tail>
    where Tail: FrameIdx<TargetIdent, TargetFrameIdx>
{
    type FrameIdx = Tail::FrameIdx;
}
impl<TargetIdent, TargetFIdx, TargetFrameIdx, Tail> FrameIdx<TargetIdent, TargetFrameIdx>
    for IdentFrameIdxCons<TargetIdent, TargetFIdx, TargetFrameIdx, Tail>
{
    type FrameIdx = TargetFrameIdx;
}

impl<Ident, FIdx, FrameIdx, Tail> IdentFrameIdxCons<Ident, FIdx, FrameIdx, Tail>
{
    fn select_idents<IdentList>(&self)
        -> <Self as SelectIdents<IdentList>>::Output
        where Self: SelectIdents<IdentList>
    {
        SelectIdents::select_ident(self)
    }
}

pub trait SelectIdents<IdentList>
{
    type Output;
    // fn select_idents(&self) -> Self::Output;
}

impl<IdentList> SelectIdents<IdentList> for Nil
{
    type Output = Nil;
    // fn select_idents(&self) -> Self::Output { Nil }
}
impl<TargetIdent, NonTargetIdent, FrameIdx, Tail, ICTail>
    SelectIdents<IdentCons<TargetIdent, ICTail>>
    for IdentFrameIdxCons<NonTargetIdent, FrameIdx, Tail>
    where Tail: SelectIdents<IdentCons<TargetIdent, ICTail>>
{
    type Output = <Tail as SelectIdents<IdentCons<TargetIdent, ICTail>>>::Output;

    // fn select_idents(&self) -> Self::Output
    // {
    //     self.tail.select_idents()
    // }
}
impl<TargetIdent, FrameIdx, Tail, ICTail>
    SelectIdents<IdentCons<TargetIdent, ICTail>>
    for IdentFrameIdxCons<TargetIdent, FrameIdx, Tail>
{
    type Output = IdentFrameIdxCons<TargetIdent, FrameIdx, <Tail as SelectIdents<ICTail>>::Output>;

    // fn select_idents(&self) -> Self::Output
    // {
    //     IdentFrameIdxCons {
    //         head: IdentFrame {
    //             _ident: PhantomData,
    //             _frame: PhantomData
    //         },
    //         tail: self.tail
    //     }
    // }
}

pub trait FrameSelector<FrameIdx>
{
    type FrameFields;
}

impl<Ident, FrameIdx, Tail, Frames, FIdx> AssocFieldCons
    for (IdentFrameIdxCons<Ident, FIdx, FrameIdx, Tail>, Frames)
    where Frames: FrameSelector<FrameIdx>,
          Frames::FrameFields: FSelector<Ident, FIdx>,
{
    type Fields = FieldCons<
        Ident,
        FIdx,
        <Frames::FrameFields as FSelector<Ident, FIdx>>::DType,
        <(Tail, Frames) as AssocFieldCons>::Fields
    >;

}

#[macro_export]
macro_rules! Idents {
    (@idents()) => { Nil };
    (@idents($fident:ty, $($rest:ty,)*)) => {
        IdentCons {
            head: PhantomData::<$fident>,
            tail: Idents![@idents($($rest)*)]
        }
    };
    ($($fident:ty),*$(,)*) => {
        Idents![@idents($($fident,)*)]
    }
}

// Idents is a 'IdentFrameIdxCons', Frames is a 'FrameCons'.
#[derive(Debug, Clone, Default)]
pub struct DataView<Idents, Frames> {
    /// cons-list of field identifiers and their associated frame indices
    frame_indices: PhantomData<Idents>,
    /// A cons-list of DataFrames
    frames: Frames,
}


impl<Idents, Frames> DataView<Idents, Frames>
    // where DTypes: DTypeList
{
    /// Generate a new subview of this DataView. IdentList is an IdentCons.
    pub fn v<IdentList>(&self)
        -> DataView<<Idents as SelectIdents<IdentList>>::Output, Frames>
    {
        // select_idents builds a new IdentFrameIdxCons sublist from a IdentFrameIdxCons only
        // containins the idents specified in the IdentList.
        DataView {
            // frame_indices: self.frame_indices.select_idents::<IdentList>(),
            frame_indices: PhantomData,
            fields: self.frames,
        }

        // let mut sub_fields = IndexMap::new();
        // for ident in &s.into_field_list() {
        //     if let Some(field) = self.fields.get(ident) {
        //         sub_fields.insert(ident.clone(), field.clone());
        //     }
        // }
        // DataView {
        //     frames: self.frames.clone(),
        //     fields: sub_fields,
        // }
    }
    pub fn subview<IdentList>(&self) -> DataView<Idents, Frames> {
        self.v::<IdentList>()
    }
    // /// Generate a new subview of this DataView, generating an error if a specified field does
    // /// not exist.
    // pub fn subview<L: IntoFieldList>(&self, s: L) -> error::Result<DataView<Fields>> {
    //     let mut sub_fields = IndexMap::new();
    //     for ident in &s.into_field_list() {
    //         if let Some(field) = self.fields.get(ident) {
    //             sub_fields.insert(ident.clone(), field.clone());
    //         } else {
    //             return Err(error::AgnesError::FieldNotFound(ident.clone()));
    //         }
    //     }
    //     Ok(DataView {
    //         frames: self.frames.clone(),
    //         fields: sub_fields,
    //     })
    // }
    /// Number of rows in this data view
    pub fn nrows(&self) -> usize
        // where DTypes::Storage: MaxLen<DTypes>
    {
        if self.frames.is_empty() { 0 } else { self.frames[0].nrows() }
    }
    /// Returns `true` if the DataView is empty (has no rows or has no fields)
    pub fn is_empty(&self) -> bool
        // where DTypes::Storage: MaxLen<DTypes>
    {
        self.nrows() == 0
    }
    /// Number of fields in this data view
    pub fn nfields(&self) -> usize {
        self.fields.len()
    }
    /// Field names in this data view
    pub fn fieldnames(&self) -> Vec<&FieldIdent> {
        self.fields.keys().collect()
    }
    // /// Return the field type for specified field
    // pub(crate) fn get_field_type(&self, ident: &FieldIdent) -> Option<DTypes::DType> {
    //     self.fields.get(ident).and_then(|view_field: &ViewField| {
    //         self.frames[view_field.frame_idx].get_field_type(&view_field.rident.ident)
    //     })
    // }
    /// Returns `true` if this `DataView` contains this field.
    pub fn has_field(&self, s: &FieldIdent) -> bool {
        self.fields.contains_key(s)
    }

    /// Rename a field of this DataView.
    pub fn rename<T, U>(&mut self, orig: T, new: U) -> error::Result<()> where
        T: Into<FieldIdent>,
        U: Into<FieldIdent>
    {
        let (orig, new) = (orig.into(), new.into());
        if self.fields.contains_key(&new) {
            return Err(error::AgnesError::FieldCollision(vec![new]));
        }
        let new_vf = if let Some(ref orig_vf) = self.fields.get(&orig) {
            ViewField {
                rident: RFieldIdent {
                    ident: orig_vf.rident.ident.clone(),
                    rename: Some(new.to_string())
                },
                frame_idx: orig_vf.frame_idx,
            }
        } else {
            return Err(error::AgnesError::FieldNotFound(orig));
        };
        self.fields.insert(new_vf.rident.to_renamed_field_ident(), new_vf);
        self.fields.swap_remove(&orig);
        Ok(())
    }

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

    /// Returns an iterator over the fields (as `FieldIdent`s of this DataView.
    pub fn idents(&self) -> Keys<FieldIdent, ViewField> {
        self.fields.keys()
    }

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
}

impl<Idents, Frames> FSelect for DataView<Idents, Frames>
{}

impl<'a, Idents, Frames, Ident, FIdx> SelectField<'a, Ident, FIdx>
    for DataView<Idents, Frames>
    where
          Fields: FSelector<Ident, FIdx>
          // T: 'static + DataType<DTypes>,
          // DTypes: 'a + DTypeList,
          // DTypes::Storage: MaxLen<DTypes>
{
    type Output = Framed<'a, Fields::DType>;

    fn select_field(&'a self)
        -> Framed<'a, Field::DType>
        // where DTypes: AssocTypes,
        //       DTypes::Storage: TypeSelector<DTypes, T>
    {
        self.frames[self.frame_indices.select::<Ident, _>()]
            .field::<Ident, _>()

        // self.fields.get(&ident)
        //     .ok_or_else(|| error::AgnesError::FieldNotFound(ident.clone()))
        //     .and_then(|view_field: &ViewField| {
        //         self.frames[view_field.frame_idx].select(view_field.rident.ident.clone())
        //     })
    }
}

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

impl<Fields> From<DataStore<Fields>> for DataView<Fields>
    where Fields: AssocStorage + AssocFrame,
{
    fn from(store: DataStore<Fields>) -> DataView<Fields> {

        // let mut fields = IndexMap::new();
        // for ident in store.fields() {
        //     fields.insert(ident.clone(), ViewField {
        //         rident: RFieldIdent {
        //             ident: ident.clone(),
        //             rename: None
        //         },
        //         frame_idx: 0,
        //     });
        // }

        struct GenerateViewCons {}
        impl<DType> PayloadGenerator<DType> for GenerateViewCons {
            type Payload = FrameIndex;
            fn generate() -> FrameIndex {
                0usize
            }
        }

        DataView {
            frames: vec![store.into()],
            frame_indices: <Fields as AttachPayload<GenerateViewCons, _>>::attach_payload(),
        }
    }
}

const MAX_DISP_ROWS: usize = 1000;
impl<Fields> Display for DataView<Fields>
    // where DTypes: DTypeList,
    //       DTypes::Storage: MaxLen<DTypes>
    //               + for<'a, 'b> Map<DTypes, FramedFunc<'a, DTypes, AddCellToRowFn<'b>>, ()>
{
    fn fmt(&self, f: &mut Formatter) -> Result<(), fmt::Error> {
        if self.frames.is_empty() || self.fields.is_empty() {
            return write!(f, "Empty DataView");
        }
        let nrows = self.frames[0].nrows();
        let mut rows = vec![pt::row::Row::empty(); nrows.min(MAX_DISP_ROWS)];

        partial_map![Fields, AddCellToRowFn];
        for view_field in self.fields.values() {
            match self.frames[view_field.frame_idx].map(
                &view_field.rident.ident,
                AddCellToRowFn {
                    rows: &mut rows,
                },
            ) {
                Ok(_) => {},
                Err(e) => { return write!(f, "view display error: {}", e); },
            }
        }
        let mut table = pt::Table::new();
        table.set_titles(self.fields.keys().into());
        for row in rows.drain(..) {
            table.add_row(row);
        }
        table.set_format(*pt::format::consts::FORMAT_NO_BORDER_LINE_SEPARATOR);
        Display::fmt(&table, f)
    }
}


/// Function (implementing [Func](../data_types/trait.Func.html)) that adds cells to
/// `prettytable::row::Row`.
pub struct AddCellToRowFn<'a, 'b, Fields> {
    rows: &'a mut Vec<pt::row::Row>,
    dv: &'b DataView<Fields>
}
impl<'a, 'b, DType, Fields> Func<DType> for AddCellToRowFn<'a, 'b, Fields>
{
    type Output = ();
    fn call<Field>(&self) -> Self::Output
        where Field: FieldTypes<DType=DType>
    {
        let type_data = self.dv.field::<Field::Ident, _>();
        for i in 0..type_data.len().min(MAX_DISP_ROWS) {
            self.rows[i].add_cell(cell!(type_data.get_datum(i).unwrap()));
        }
    }
}
impl<'a, 'b, Fields> FuncDefault for AddCellToRowFn<'a, 'b, Fields>
{
    type Output = ();
    fn call<Field>(&self) -> Self::Output
    {
        let type_data = self.dv.field::<Field::Ident, _>();
        for i in 0..type_data.len().min(MAX_DISP_ROWS) {
            self.rows[i].add_cell(cell!());
        }
    }
}
impl<'a> ReqFeature for AddCellToRowFn<'a>
{
    type Feature = DisplayFeat;
}

impl<Fields> Serialize for DataView<Fields>
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
pub trait SerializeAsVec: Serialize {}
impl<T> SerializeAsVec for Vec<T> where T: Serialize {}

/// A 'view' into a single field's data in a data frame. This is a specialty view used to serialize
/// a `DataView` as a single sequence instead of as a map.
#[derive(Debug, Clone)]
pub struct FieldView<Fields>
    // where DTypes: DTypeList
{
    frame: DataFrame<Fields>,
    field: RFieldIdent,
}

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
impl<Fields> SerializeAsVec for FieldView<Fields>
    // where DTypes: DTypeList,
    //       DTypes::Storage: MaxLen<DTypes> + FieldSerialize<DTypes>
{}

impl<Fields> DataView<Fields>
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

/// Conversion trait for converting into a vector of FieldIdents. Used for indexing into a
/// `DataView`.
pub trait IntoFieldList {
    /// Convert into a `Vec<FieldIdents>`
    fn into_field_list(self) -> Vec<FieldIdent>;
}

impl IntoFieldList for FieldIdent {
    fn into_field_list(self) -> Vec<FieldIdent> {
        vec![self]
    }
}
impl<'a> IntoFieldList for &'a FieldIdent {
    fn into_field_list(self) -> Vec<FieldIdent> {
        vec![self.clone()]
    }
}
impl IntoFieldList for Vec<FieldIdent> {
    fn into_field_list(self) -> Vec<FieldIdent> {
        self
    }
}
impl<'a> IntoFieldList for Vec<&'a FieldIdent> {
    fn into_field_list(self) -> Vec<FieldIdent> {
        #[allow(unknown_lints, map_clone)]
        self.iter().map(|&fi| fi.clone()).collect()
    }
}

impl<'a> IntoFieldList for &'a str {
    fn into_field_list(self) -> Vec<FieldIdent> {
        vec![FieldIdent::Name(self.to_string())]
    }
}
impl<'a> IntoFieldList for Vec<&'a str> {
    fn into_field_list(mut self) -> Vec<FieldIdent> {
        self.drain(..).map(|s| FieldIdent::Name(s.to_string())).collect()
    }
}
macro_rules! impl_into_field_list_str_arr {
    ($($val:expr),*) => {$(
        impl<'a> IntoFieldList for [&'a str; $val] {
            fn into_field_list(self) -> Vec<FieldIdent> {
                self.iter().map(|s| FieldIdent::Name(s.to_string())).collect()
            }
        }
    )*}
}
impl_into_field_list_str_arr!(1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
    11, 12, 13, 14, 15, 16, 17, 18, 19, 20);

impl IntoFieldList for String {
    fn into_field_list(self) -> Vec<FieldIdent> {
        vec![FieldIdent::Name(self)]
    }
}
impl IntoFieldList for Vec<String> {
    fn into_field_list(mut self) -> Vec<FieldIdent> {
        self.drain(..).map(FieldIdent::Name).collect()
    }
}
macro_rules! impl_into_field_list_string_arr {
    ($($val:expr),*) => {$(
        impl IntoFieldList for [String; $val] {
            fn into_field_list(self) -> Vec<FieldIdent> {
                // clone necessary since we're moving to the heap
                self.iter().map(|s| FieldIdent::Name(s.clone())).collect()
            }
        }
    )*}
}
impl_into_field_list_string_arr!(1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
    11, 12, 13, 14, 15, 16, 17, 18, 19, 20);

#[cfg(test)]
mod tests {
    // use test_utils::*;

    // use super::{DataView, Filter};
    use error::*;
    // use data_types::standard::*;
    use access::{DataIndex, DataIterator};

    // #[test]
    // fn merge() {
    //     let ds1 = sample_emp_table();
    //     let ds2 = sample_emp_table_extra();

    //     let (dv1, dv2): (DataView, DataView) = (ds1.into(), ds2.into());
    //     println!("{}", dv1);
    //     println!("{}", dv2);
    //     let merged_dv: DataView = dv1.merge(&dv2).expect("merge failed");
    //     println!("{}", merged_dv);
    //     assert_eq!(merged_dv.nrows(), 7);
    //     assert_eq!(merged_dv.nfields(), 6);
    //     for (left, right) in merged_dv.fieldnames().iter()
    //         .zip(vec!["EmpId", "DeptId", "EmpName", "SalaryOffset", "DidTraining", "VacationHrs"]
    //                 .iter().map(|&s| FieldIdent::Name(s.into())))
    //     {
    //         assert_eq!(left, &&right);
    //     }
    // }

    // #[test]
    // fn merge_dimension_mismatch() {
    //     let ds1 = sample_emp_table();
    //     let ds2 = sample_dept_table();

    //     let (dv1, dv2): (DataView, DataView) = (ds1.into(), ds2.into());
    //     println!("{}", dv1);
    //     println!("{}", dv2);
    //     match dv1.merge(&dv2) {
    //         Ok(_) => { panic!("Merge was expected to fail (dimension mismatch), but succeeded"); },
    //         Err(AgnesError::DimensionMismatch(_)) => { /* expected */ },
    //         Err(e) => { panic!("Incorrect error: {:?}", e); },
    //     };
    // }

    // #[test]
    // fn merge_field_collision() {
    //     let ds1 = sample_emp_table();
    //     let ds2 = sample_emp_table();

    //     let (dv1, dv2): (DataView, DataView) = (ds1.into(), ds2.into());
    //     println!("{}", dv1);
    //     println!("{}", dv2);
    //     match dv1.merge(&dv2) {
    //         Ok(_) => { panic!("Merge expected to fail (field collision), but succeeded"); },
    //         Err(AgnesError::FieldCollision(fields)) => {
    //             assert_eq!(fields, vec!["EmpId", "DeptId", "EmpName"]
    //                 .iter().map(|&s| FieldIdent::Name(s.into())).collect::<Vec<_>>());
    //         },
    //         Err(e) => { panic!("Incorrect error: {:?}", e); }
    //     }
    // }

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

    // #[test]
    // fn subview() {
    //     let ds = sample_emp_table();
    //     let dv: DataView = ds.into();
    //     assert_eq!(dv.frames[0].store_ref_count(), 1);
    //     assert_field_lists_match(dv.fieldnames(), vec!["EmpId", "DeptId", "EmpName"]);

    //     let subdv1 = dv.v("EmpId");
    //     assert_eq!(dv.frames[0].store_ref_count(), 2);
    //     assert_eq!(subdv1.nrows(), 7);
    //     assert_eq!(subdv1.nfields(), 1);
    //     let subdv1 = dv.subview("EmpId").expect("subview failed");
    //     assert_eq!(dv.frames[0].store_ref_count(), 3);
    //     assert_eq!(subdv1.nrows(), 7);
    //     assert_eq!(subdv1.nfields(), 1);

    //     let subdv2 = dv.v(vec!["EmpId", "DeptId"]);
    //     assert_eq!(dv.frames[0].store_ref_count(), 4);
    //     assert_eq!(subdv2.nrows(), 7);
    //     assert_eq!(subdv2.nfields(), 2);
    //     let subdv2 = dv.subview(vec!["EmpId", "DeptId"]).expect("subview failed");
    //     assert_eq!(dv.frames[0].store_ref_count(), 5);
    //     assert_eq!(subdv2.nrows(), 7);
    //     assert_eq!(subdv2.nfields(), 2);

    //     let subdv3 = dv.v(vec!["EmpId", "DeptId", "EmpName"]);
    //     assert_eq!(dv.frames[0].store_ref_count(), 6);
    //     assert_eq!(subdv3.nrows(), 7);
    //     assert_eq!(subdv3.nfields(), 3);
    //     let subdv3 = dv.subview(vec!["EmpId", "DeptId", "EmpName"]).expect("subview failed");
    //     assert_eq!(dv.frames[0].store_ref_count(), 7);
    //     assert_eq!(subdv3.nrows(), 7);
    //     assert_eq!(subdv3.nfields(), 3);

    //     // Subview of a subview
    //     let subdv4 = subdv2.v("DeptId");
    //     assert_eq!(dv.frames[0].store_ref_count(), 8);
    //     assert_eq!(subdv4.nrows(), 7);
    //     assert_eq!(subdv4.nfields(), 1);
    //     let subdv4 = subdv2.subview("DeptId").expect("subview failed");
    //     assert_eq!(dv.frames[0].store_ref_count(), 9);
    //     assert_eq!(subdv4.nrows(), 7);
    //     assert_eq!(subdv4.nfields(), 1);
    // }

    // #[test]
    // fn subview_fail() {
    //     let ds = sample_emp_table();
    //     let dv: DataView = ds.into();
    //     assert_eq!(dv.frames[0].store_ref_count(), 1);
    //     assert_field_lists_match(dv.fieldnames(), vec!["EmpId", "DeptId", "EmpName"]);

    //     // "Employee Name" does not exist
    //     let subdv1 = dv.v(vec!["EmpId", "DeptId", "Employee Name"]);
    //     assert_eq!(dv.frames[0].store_ref_count(), 2);
    //     assert_eq!(subdv1.nrows(), 7);
    //     assert_eq!(subdv1.nfields(), 2);
    //     match dv.subview(vec!["EmpId", "DeptId", "Employee Name"]) {
    //         Ok(_) => { panic!("expected error (field not found), but succeeded"); },
    //         Err(AgnesError::FieldNotFound(field)) => {
    //             assert_eq!(field, FieldIdent::Name("Employee Name".into()));
    //         },
    //         Err(e) => { panic!("Incorrect error: {:?}", e); }
    //     }

    //     let subdv2 = dv.v("Nonexistant");
    //     assert_eq!(dv.frames[0].store_ref_count(), 3);
    //     assert_eq!(subdv2.nrows(), 7); // still 7 rows, just no fields
    //     assert_eq!(subdv2.nfields(), 0);
    //     match dv.subview(vec!["Nonexistant"]) {
    //         Ok(_) => { panic!("expected error (field not found), but succeeded"); },
    //         Err(AgnesError::FieldNotFound(field)) => {
    //             assert_eq!(field, FieldIdent::Name("Nonexistant".into()));
    //         },
    //         Err(e) => { panic!("Incorrect error: {:?}", e); }
    //     }
    // }

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
