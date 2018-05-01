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

impl DataFrame {
    pub fn apply<F: MapFn>(&self, f: &mut F, ident: &FieldIdent)
        -> error::Result<Vec<F::Output>>
    {
        (0..self.nrows()).map(|idx| {
            self.store.apply(f, &ident, self.map_index(idx))
        }).collect()
    }
    pub fn apply_field<F: FieldMapFn>(&self, f: &mut F, ident: &FieldIdent)
        -> error::Result<F::Output>
    {
        self.store.apply_field(&mut FrameFieldMapFn { frame: &self, field_fn: f }, &ident)
    }
    pub fn apply_elem<F: MapFn>(&self, f: &mut F, ident: &FieldIdent, idx: usize)
        -> error::Result<F::Output>
    {
        self.store.apply(f, &ident, self.map_index(idx))
    }
}

// impl<'a> Apply<FieldIndexSelector<'a>> for DataFrame {
//     fn apply<F: MapFn>(&self, f: &mut F, select: &FieldIndexSelector)
//         -> error::Result<F::Output>
//     {
//         let (ident, idx) = select.index();
//         self.store.apply(f, &FieldIndexSelector(ident, self.map_index(idx)))
//     }
// }

// impl<T: DataType> FieldDataIndex<T> for DataFrame {
//     fn get_field_data(&self, ident: &FieldIdent, idx: usize) -> error::Result<MaybeNa<&T>> {
//         self.store.get_field_data(ident, idx)
//     }
//     fn field_len(&self, _: &FieldIdent) -> usize {
//         self.nrows()
//     }
// }


// impl<'a> ApplyToField<FieldSelector<'a>> for DataFrame {
//     fn apply_to_field<F: FieldFn>(&self, f: F, select: FieldSelector)
//         -> error::Result<F::Output>
//     {
//         self.store.apply_to_field(FrameFieldFn { frame: &self, field_fn: f }, select)
//     }
// }
// impl<'a, 'b, 'c> ApplyToField2<FieldSelector<'a>> for (&'b DataFrame, &'c DataFrame) {
//     fn apply_to_field2<F: Field2Fn>(&self, f: F, select: (FieldSelector, FieldSelector))
//         -> error::Result<F::Output>
//     {
//         (self.0.store.as_ref(), self.1.store.as_ref()).apply_to_field2(
//             FrameField2Fn { frames: (&self.0, &self.1), field_fn: f }, select)
//     }
// }

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
struct Framed<'a, 'b, T: DataType, D: 'b> {
    frame: &'a DataFrame,
    data: &'b D,
    dtype: PhantomData<T>,
}
impl<'a, 'b, T: DataType, D: 'b +> Framed<'a, 'b, T, D> {
    fn new(frame: &'a DataFrame, data: &'b D) -> Framed<'a, 'b, T, D> {
        Framed { frame, data, dtype: PhantomData }
    }
}
impl<'a, 'b, T: DataType, D: 'b + DataIndex<T>> DataIndex<T> for Framed<'a, 'b, T, D> {
    fn get_data(&self, idx: usize) -> error::Result<MaybeNa<&T>> {
        self.data.get_data(self.frame.map_index(idx))
    }
    fn len(&self) -> usize {
        self.frame.nrows()
    }

}


// struct FrameMapFn<'a, F: MapFn> {
//     frame: &'a DataFrame,
//     map_fn: F,
// }
// impl<'a, F: MapFn> MapFn for FrameMapFn<'a, F> {
//     type Output = F::Output;
//     fn apply_unsigned(&mut self, value: MaybeNa<&u64>) -> F::Output {
//         self.map_fn.apply_unsigned(&Framed::new(self.frame, value))
//     }
//     fn apply_signed(&mut self, value: MaybeNa<&i64>) -> F::Output {
//         self.map_fn.apply_signed(&Framed::new(self.frame, value))
//     }
//     fn apply_text(&mut self, value: MaybeNa<&String>) -> F::Output {
//         self.map_fn.apply_text(&Framed::new(self.frame, value))
//     }
//     fn apply_boolean(&mut self, value: MaybeNa<&bool>) -> F::Output {
//         self.map_fn.apply_boolean(&Framed::new(self.frame, value))
//     }
//     fn apply_float(&mut self, value: MaybeNa<&f64>) -> F::Output {
//         self.map_fn.apply_float(&Framed::new(self.frame, value))
//     }
// }

struct FrameFieldMapFn<'a, 'b, F: 'b + FieldMapFn> {
    frame: &'a DataFrame,
    field_fn: &'b mut F,
}
impl<'a, 'b, F: 'b + FieldMapFn> FieldMapFn for FrameFieldMapFn<'a, 'b, F> {
    type Output = F::Output;
    fn apply_unsigned<T: DataIndex<u64>>(&mut self, field: &T) -> F::Output {
        self.field_fn.apply_unsigned(&Framed::new(self.frame, field))
    }
    fn apply_signed<T: DataIndex<i64>>(&mut self, field: &T) -> F::Output {
        self.field_fn.apply_signed(&Framed::new(self.frame, field))
    }
    fn apply_text<T: DataIndex<String>>(&mut self, field: &T) -> F::Output {
        self.field_fn.apply_text(&Framed::new(self.frame, field))
    }
    fn apply_boolean<T: DataIndex<bool>>(&mut self, field: &T) -> F::Output {
        self.field_fn.apply_boolean(&Framed::new(self.frame, field))
    }
    fn apply_float<T: DataIndex<f64>>(&mut self, field: &T) -> F::Output {
        self.field_fn.apply_float(&Framed::new(self.frame, field))
    }
}
// struct FrameField2Fn<'a, 'b, F: Field2Fn> {
//     frames: (&'a DataFrame, &'b DataFrame),
//     field_fn: F,
// }
// impl<'a, 'b, F: Field2Fn> Field2Fn for FrameField2Fn<'a, 'b, F> {
//     type Output = F::Output;
//     fn apply_unsigned<T: DataIndex<u64>>(&mut self, field: &(&T, &T)) -> F::Output {
//         self.field_fn.apply_unsigned(&(
//             &Framed::new(self.frames.0, field.0),
//             &Framed::new(self.frames.1, field.1)
//         ))
//     }
//     fn apply_signed<T: DataIndex<i64>>(&mut self, field: &(&T, &T)) -> F::Output {
//         self.field_fn.apply_signed(&(
//             &Framed::new(self.frames.0, field.0),
//             &Framed::new(self.frames.1, field.1)
//         ))
//     }
//     fn apply_text<T: DataIndex<String>>(&mut self, field: &(&T, &T)) -> F::Output {
//         self.field_fn.apply_text(&(
//             &Framed::new(self.frames.0, field.0),
//             &Framed::new(self.frames.1, field.1)
//         ))
//     }
//     fn apply_boolean<T: DataIndex<bool>>(&mut self, field: &(&T, &T)) -> F::Output {
//         self.field_fn.apply_boolean(&(
//             &Framed::new(self.frames.0, field.0),
//             &Framed::new(self.frames.1, field.1)
//         ))
//     }
//     fn apply_float<T: DataIndex<f64>>(&mut self, field: &(&T, &T)) -> F::Output {
//         self.field_fn.apply_float(&(
//             &Framed::new(self.frames.0, field.0),
//             &Framed::new(self.frames.1, field.1)
//         ))
//     }

// }

pub(crate) struct SerializedField<'a> {
    pub(crate) ident: FieldIdent,
    pub(crate) frame: &'a DataFrame
}

struct SerializeFn<'b, S: Serializer> {
    serializer: Option<S>,
    frame: &'b DataFrame
}
macro_rules! sresult { ($s:tt) => (Result<$s::Ok, $s::Error>) }
fn do_serialize<'a, 'b, T: DataType + Serialize, S: 'a + Serializer>(
        sfn: &mut SerializeFn<'b, S>, field: &DataIndex<T>
    ) -> sresult![S]
{
    let serializer = sfn.serializer.take().unwrap();
    let mut seq = serializer.serialize_seq(Some(field.len()))?;
    for idx in 0..field.len() {
        match field.get_data(sfn.frame.map_index(idx)).unwrap() {
            MaybeNa::Exists(&ref val) =>  seq.serialize_element(val)?,
            MaybeNa::Na =>  seq.serialize_element("null")?
        }
    }
    seq.end()
}
impl<'b, Ser: Serializer> FieldMapFn for SerializeFn<'b, Ser> {
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
        self.frame.apply_field(
            &mut SerializeFn { serializer: Some(serializer), frame: &self.frame },
            &self.ident
        ).unwrap_or(
            Err(ser::Error::custom(format!("missing field: {}", self.ident.to_string())))
        )
    }
}
