/*!
Structs and implementation for Frame-level data structure. A `DataFrame` is a reference to an
underlying data store, along with record-based filtering and sorting details.
*/
use std::rc::Rc;
use std::marker::PhantomData;
use serde::{Serialize, Serializer};
use serde::ser::{self, SerializeSeq};

use store::DataStore;
use field::{DataType, FieldIdent, FieldType};
use apply::*;
use error;
use masked::MaybeNa;

/// A data frame. A `DataStore` reference along with record-based filtering and sorting details.
#[derive(Debug, Clone)]
pub struct DataFrame {
    permutation: Option<Vec<usize>>,
    store: Rc<DataStore>,
}
impl DataFrame {
    /// Number of rows that pass the filter in this frame.
    pub fn nrows(&self) -> usize {
        match self.permutation {
            Some(ref perm) => perm.len(),
            None => self.store.nrows()
        }
    }
    #[cfg(test)]
    pub(crate) fn store_ref_count(&self) -> usize {
        Rc::strong_count(&self.store)
    }
    /// Get the field type of a particular field in the underlying `DataStore`.
    pub fn get_field_type(&self, ident: &FieldIdent) -> Option<FieldType> {
        self.store.get_field_type(ident)
    }
    pub(crate) fn has_same_store(&self, other: &DataFrame) -> bool {
        Rc::ptr_eq(&self.store, &other.store)
    }
    fn map_index(&self, requested: usize) -> usize {
        match self.permutation {
            Some(ref perm) => perm[requested],
            None => requested
        }
    }
    /// Returns `true` if this `DataFrame` contains this field.
    pub fn has_field(&self, s: &FieldIdent) -> bool {
        self.store.has_field(s)
    }
    pub(crate) fn update_permutation(&mut self, new_permutation: &Vec<usize>) {
        // check if we already have a permutation
        self.permutation = match self.permutation {
            Some(ref prev_perm) => {
                // we already have a permutation, map the filter indices through it
                Some(new_permutation.iter().map(|&new_idx| prev_perm[new_idx]).collect())
            },
            None => Some(new_permutation.clone())
        };
    }
}

/// Trait that provides a function for filtering a data structure's contents.
pub trait Filter<T> {
    /// Filter the contents of this data structure by applying the supplied predicate on the
    /// specified field.
    fn filter<F: Fn(&T) -> bool>(&mut self, ident: &FieldIdent, pred: F)
        -> error::Result<Vec<usize>>;
}
macro_rules! impl_filter {
    ($($dtype:tt)*) => {$(

impl Filter<$dtype> for DataFrame {
    fn filter<F: Fn(&$dtype) -> bool>(&mut self, ident: &FieldIdent, pred: F)
        -> error::Result<Vec<usize>>
    {
        let filter = self.get_filter(pred, ident)?;
        self.update_permutation(&filter);
        Ok(filter)
    }
}

    )*}
}
impl_filter!(u64 i64 String bool f64);

/// Trait that provides a function for sorting a data structure's contents.
pub trait SortBy {
    /// Sort the contents of this data structure (ascending) by the specified field.
    fn sort_by(&mut self, ident: &FieldIdent) -> error::Result<Vec<usize>>;
}
impl SortBy for DataFrame {
    fn sort_by(&mut self, ident: &FieldIdent) -> error::Result<Vec<usize>> {
        let sort_order = self.sort_order_by(ident)?;
        self.update_permutation(&sort_order);
        Ok(sort_order)
    }
}

impl ApplyTo for DataFrame {
    fn apply_to<F: MapFn>(&self, f: &mut F, ident: &FieldIdent)
        -> error::Result<Vec<F::Output>>
    {
        (0..self.nrows()).map(|idx| {
            self.store.apply_to_elem(f, &ident, self.map_index(idx))
        }).collect()
    }
}
impl ApplyToElem for DataFrame {
    fn apply_to_elem<F: MapFn>(&self, f: &mut F, ident: &FieldIdent, idx: usize)
        -> error::Result<F::Output>
    {
        self.store.apply_to_elem(f, &ident, self.map_index(idx))
    }
}
impl FieldApplyTo for DataFrame {
    fn field_apply_to<F: FieldMapFn>(&self, f: &mut F, ident: &FieldIdent)
        -> error::Result<F::Output>
    {
        self.store.field_apply_to(&mut FrameFieldMapFn { frame: &self, field_fn: f }, &ident)
    }
}

impl<'a, 'b> ApplyFieldReduce<'a> for Selection<'a, 'b, DataFrame> {
    fn apply_field_reduce<F: FieldReduceFn<'a>>(&self, f: &mut F)
        -> error::Result<F::Output>
    {
        self.data.store.select(self.ident)
            .apply_field_reduce(&mut FrameFieldReduceFn {
                frames: vec![&self.data],
                reduce_fn: f,
            })
    }
}
impl<'a, 'b> ApplyFieldReduce<'a> for Vec<Selection<'a, 'b, DataFrame>> {
    fn apply_field_reduce<F: FieldReduceFn<'a>>(&self, f: &mut F)
        -> error::Result<F::Output>
    {
        let frames = self.iter().map(|selection| selection.data).collect::<Vec<_>>();
        self.iter().map(|selection| {
            selection.data.store.select(selection.ident)
        }).collect::<Vec<_>>().apply_field_reduce(&mut FrameFieldReduceFn {
            frames: frames,
            reduce_fn: f,
        })
    }
}

impl From<DataStore> for DataFrame {
    fn from(store: DataStore) -> DataFrame {
        DataFrame {
            permutation: None,
            store: Rc::new(store),
        }
    }
}

// Structure to hold references to a data structure (e.g. DataStore) and a frame used to view
// that structure. Provides DataIndex for the underlying data structure, as view through the frame.
struct Framed<'a, 'b, T: 'b + DataType> {
    frame: &'a DataFrame,
    data: OwnedOrRef<'b, T>,
    dtype: PhantomData<T>,
}
impl<'a, 'b, T: DataType> Framed<'a, 'b, T> {
    fn new(frame: &'a DataFrame, data: OwnedOrRef<'b, T>) -> Framed<'a, 'b, T> {
        Framed { frame, data, dtype: PhantomData }
    }
}
impl<'a, 'b, T: DataType> DataIndex<T> for Framed<'a, 'b, T> {
    fn get_data(&self, idx: usize) -> error::Result<MaybeNa<&T>> {
        self.data.get_data(self.frame.map_index(idx))
    }
    fn len(&self) -> usize {
        self.frame.nrows()
    }

}

struct FrameFieldMapFn<'a, 'b, F: 'b + FieldMapFn> {
    frame: &'a DataFrame,
    field_fn: &'b mut F,
}
impl<'a, 'b, F: 'b + FieldMapFn> FieldMapFn for FrameFieldMapFn<'a, 'b, F> {
    type Output = F::Output;
    fn apply_unsigned<T: DataIndex<u64>>(&mut self, field: &T) -> F::Output {
        self.field_fn.apply_unsigned(&Framed::new(self.frame, OwnedOrRef::Ref(field)))
    }
    fn apply_signed<T: DataIndex<i64>>(&mut self, field: &T) -> F::Output {
        self.field_fn.apply_signed(&Framed::new(self.frame, OwnedOrRef::Ref(field)))
    }
    fn apply_text<T: DataIndex<String>>(&mut self, field: &T) -> F::Output {
        self.field_fn.apply_text(&Framed::new(self.frame, OwnedOrRef::Ref(field)))
    }
    fn apply_boolean<T: DataIndex<bool>>(&mut self, field: &T) -> F::Output {
        self.field_fn.apply_boolean(&Framed::new(self.frame, OwnedOrRef::Ref(field)))
    }
    fn apply_float<T: DataIndex<f64>>(&mut self, field: &T) -> F::Output {
        self.field_fn.apply_float(&Framed::new(self.frame, OwnedOrRef::Ref(field)))
    }
}

struct FrameFieldReduceFn<'a, 'b, F: 'b + FieldReduceFn<'a>> {
    frames: Vec<&'a DataFrame>,
    reduce_fn: &'b mut F,
}
impl<'a, 'b, F: FieldReduceFn<'a>> FieldReduceFn<'a> for FrameFieldReduceFn<'a, 'b, F>
{
    type Output = F::Output;
    fn reduce(&mut self, mut fields: Vec<ReduceDataIndex<'a>>) -> F::Output {
        let data_vec = fields.drain(..).zip(self.frames.iter()).map(|(field, frame)| {
            let field: ReduceDataIndex<'a> = field;
            match field {
                ReduceDataIndex::Unsigned(field) =>
                    ReduceDataIndex::Unsigned(OwnedOrRef::Owned(Box::new(
                        Framed::new(frame, field)))),
                ReduceDataIndex::Signed(field) =>
                    ReduceDataIndex::Signed(OwnedOrRef::Owned(Box::new(
                        Framed::new(frame, field)))),
                ReduceDataIndex::Text(field) =>
                    ReduceDataIndex::Text(OwnedOrRef::Owned(Box::new(
                        Framed::new(frame, field)))),
                ReduceDataIndex::Boolean(field) =>
                    ReduceDataIndex::Boolean(OwnedOrRef::Owned(Box::new(
                        Framed::new(frame, field)))),
                ReduceDataIndex::Float(field) =>
                    ReduceDataIndex::Float(OwnedOrRef::Owned(Box::new(
                        Framed::new(frame, field)))),
            }
        }
        ).collect::<Vec<ReduceDataIndex<'a>>>();
        self.reduce_fn.reduce(data_vec)
    }
}

pub(crate) struct SerializedField<'a> {
    ident: FieldIdent,
    frame: &'a DataFrame
}
impl<'a> SerializedField<'a> {
    pub fn new(ident: FieldIdent, frame: &'a DataFrame) -> SerializedField<'a> {
        SerializedField {
            ident,
            frame
        }
    }
}

struct SerializeFn<S: Serializer> {
    serializer: Option<S>,
}
macro_rules! sresult { ($s:tt) => (Result<$s::Ok, $s::Error>) }
fn do_serialize<'a, 'b, T: DataType + Serialize, S: 'a + Serializer>(
        sfn: &mut SerializeFn<S>, field: &DataIndex<T>
    ) -> sresult![S]
{
    let serializer = sfn.serializer.take().unwrap();
    let mut seq = serializer.serialize_seq(Some(field.len()))?;
    for idx in 0..field.len() {
        match field.get_data(idx).unwrap() {
            MaybeNa::Exists(&ref val) =>  seq.serialize_element(val)?,
            MaybeNa::Na =>  seq.serialize_element("null")?
        }
    }
    seq.end()
}
impl<Ser: Serializer> FieldMapFn for SerializeFn<Ser> {
    type Output = sresult![Ser];
    fn apply_unsigned<T: DataIndex<u64>>(&mut self, field: &T) -> sresult![Ser] {
        do_serialize(self, field)
    }
    fn apply_signed<T: DataIndex<i64>>(&mut self, field: &T) -> sresult![Ser] {
        do_serialize(self, field)
    }
    fn apply_text<T: DataIndex<String>>(&mut self, field: &T) -> sresult![Ser] {
        do_serialize(self, field)
    }
    fn apply_boolean<T: DataIndex<bool>>(&mut self, field: &T) -> sresult![Ser] {
        do_serialize(self, field)
    }
    fn apply_float<T: DataIndex<f64>>(&mut self, field: &T) -> sresult![Ser] {
        do_serialize(self, field)
    }
}


impl<'b> Serialize for SerializedField<'b> {
    fn serialize<S>(&self, serializer: S) -> sresult![S] where S: Serializer {
        self.frame.field_apply_to(
            &mut SerializeFn { serializer: Some(serializer) },
            &self.ident
        ).unwrap_or(
            Err(ser::Error::custom(format!("missing field: {}", self.ident.to_string())))
        )
    }
}
