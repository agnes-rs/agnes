/*!
Structs and implementation for Frame-level data structure. A `DataFrame` is a reference to an
underlying data store, along with record-based filtering and sorting details.
*/
use std::rc::Rc;

use bit_vec::BitVec;

use store::DataStore;
use masked::MaybeNa;
use serde::{Serialize, Serializer};
use serde::ser::{self, SerializeSeq};
use field::{FieldIdent, FieldType};
use apply::*;

/// A data record filter.
#[derive(Debug, Clone)]
pub struct Filter {
    mask: BitVec,
    len: usize,
}
impl Filter {
    /// Returns the length of this filter (the number of elements that pass the filter)
    pub fn len(&self) -> usize {
        self.len
    }
}

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
    // pub(crate) fn get_field_data(&self, field: &FieldIdent) -> Option<FieldData> {
    //     self.store.get_field_data(field)
    // }
    // pub(crate) fn get_field_data<'a>(&'a self, ident: &FieldIdent) -> Option<FrameFieldData<'a>> {
    //     self.store.get_field_data(ident).map(|field_data| FrameFieldData {
    //         frame: self,
    //         field_data: field_data
    //     })
    // }
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
}
// impl ApplyToAllFieldElems for DataFrame {
//     fn apply_to_all_field_elems<T: ElemFn>(&self, mut f: T, ident: &FieldIdent)
//         -> Option<T::Output>
//     {
//         self.get_field_data(&ident).map(|ff_data| {
//             (0..ff_data.len()).map(|idx| ff_data.apply_to_elem(f, idx));
//         })
//     }
// }
impl<'a> ApplyToElem<FieldIndexSelector<'a>> for DataFrame {
    fn apply_to_elem<T: ElemFn>(&self, f: T, select: FieldIndexSelector)
        -> Option<T::Output>
    {
        let (ident, idx) = select.index();
        self.store.apply_to_elem(f, FieldIndexSelector(ident, self.map_index(idx)))
        // self.store.apply_to_field_elem(f, ident, self.map_index(idx))
        // self.store.get_field_data(ident).and_then(|ff_data| ff_data.apply_to_elem(f, idx))
        // self.get_field_data(&ident).and_then(|ff_data| ff_data.apply_to_elem(f, idx))
    }
}
impl<'a> ApplyToField<FieldSelector<'a>> for DataFrame {
    fn apply_to_field<T: FieldFn>(&self, f: T, select: FieldSelector) -> Option<T::Output> {
        self.store.apply_to_field(f, select)
    }
}
impl<'a, 'b, 'c> ApplyToField2<FieldSelector<'a>> for (&'b DataFrame, &'c DataFrame) {
    fn apply_to_field2<T: Field2Fn>(&self, f: T, select: (FieldSelector, FieldSelector))
        -> Option<T::Output>
    {
        (self.0.store.as_ref(), self.1.store.as_ref()).apply_to_field2(f, select)
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

pub(crate) struct FramedField<'a> {
    pub(crate) ident: FieldIdent,
    pub(crate) frame: &'a DataFrame
}

struct SerializeFn<'b, S: Serializer> {
    serializer: Option<S>,
    frame: &'b DataFrame
}
macro_rules! sresult { ($s:tt) => (Result<$s::Ok, $s::Error>) }
fn do_serialize<'a, 'b, T: PartialOrd + Serialize, S: 'a + Serializer>(
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
impl<'b, Ser: Serializer> FieldFn for SerializeFn<'b, Ser> {
    type Output = Result<Ser::Ok, Ser::Error>;
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


impl<'b> Serialize for FramedField<'b> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error> where S: Serializer {
        self.frame.apply_to_field(
            SerializeFn { serializer: Some(serializer), frame: &self.frame },
            FieldSelector(&self.ident)
        ).unwrap_or(
            Err(ser::Error::custom(format!("missing field: {}", self.ident.to_string())))
        )
    }
}

// pub struct FrameFieldData<'a, 'b, T: 'static + PartialOrd> {
//     frame: &'a DataFrame,
//     masked_data: &'b MaskedData<T>
// }
// impl<'a, 'b, T: PartialOrd> FrameFieldData<'a, 'b, T> {
//     pub fn len(&self) -> usize {
//         self.frame.nrows()
//     }
// }
// macro_rules! impl_ffd_apply_to_elem {
//     ($($ty:ty)*) => {$(
//         impl<'a, 'b> ApplyToElem for FrameFieldData<'a, 'b, $ty> {
//             fn apply_to_elem<F: ElemFn>(&self, f: F, idx: usize) -> Option<F::Output> {
//                 self.masked_data.apply_to_elem(f, self.frame.map_index(idx))
//             }
//         }
//     )*}
// }
// impl_ffd_apply_to_elem!(u64 i64 String bool f64);
// // impl<'a, 'b, T: PartialOrd> ApplyToElem for FrameFieldData<'a, 'b, T> {
// //     fn apply_to_elem<F: ElemFn>(&self, f: F, idx: usize) -> Option<F::Output> {
// //         self.masked_data.apply_to_elem(f, self.frame.map_index(idx))
// //     }
// // }
// macro_rules! impl_ffd_apply_to_field {
//     ($($ty:ty)*) => {$(
//         impl<'a, 'b> ApplyToField for FrameFieldData<'a, 'b, $ty> {
//             fn apply_to_field<F: FieldFn>(&self, f: F) -> Option<F::Output> {
//                 self.masked_data.apply_to_field(f)
//             }
//         }
//     )*}
// }
// impl_ffd_apply_to_field!(u64 i64 String bool f64);

// impl<'a, 'b, T: PartialOrd + Serialize> Serialize for FrameFieldData<'a, 'b, T> {
//     fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error> where S: Serializer {
//         self.masked_data.serialize(serializer)
//     }
// }
// impl<'a, 'b> DataIndex<u64> for FrameFieldData<'a, 'b, u64> {
//     fn get_data(&self, idx: usize) -> Option<MaybeNa<&u64>> {
//         self.masked_data.get(self.frame.map_index(idx))
//         // self.frame.store.get_unsigned_field(&self.ident)
//         //     .and_then(|masked: &MaskedData<u64>| masked.get(idx))
//     }
// }
// impl<'a, 'b> DataIndex<i64> for FrameFieldData<'a, 'b, i64> {
//     fn get_data(&self, idx: usize) -> Option<MaybeNa<&i64>> {
//         self.masked_data.get(self.frame.map_index(idx))
//         // self.frame.store.get_signed_field(&self.ident)
//         //     .and_then(|masked: &MaskedData<i64>| masked.get(idx))
//     }
// }
// impl<'a, 'b> DataIndex<String> for FrameFieldData<'a, 'b, String> {
//     fn get_data(&self, idx: usize) -> Option<MaybeNa<&String>> {
//         self.masked_data.get(self.frame.map_index(idx))
//         // self.frame.store.get_text_field(&self.ident)
//         //     .and_then(|masked: &MaskedData<String>| masked.get(idx))
//     }
// }
// impl<'a, 'b> DataIndex<bool> for FrameFieldData<'a, 'b, bool> {
//     fn get_data(&self, idx: usize) -> Option<MaybeNa<&bool>> {
//         self.masked_data.get(self.frame.map_index(idx))
//         // self.frame.store.get_boolean_field(&self.ident)
//         //     .and_then(|masked: &MaskedData<bool>| masked.get(idx))
//     }
// }
// impl<'a, 'b> DataIndex<f64> for FrameFieldData<'a, 'b, f64> {
//     fn get_data(&self, idx: usize) -> Option<MaybeNa<&f64>> {
//         self.masked_data.get(self.frame.map_index(idx))
//         // self.frame.store.get_float_field(&self.ident)
//         //     .and_then(|masked: &MaskedData<f64>| masked.get(idx))
//     }
// }
// pub trait GetData<T: PartialOrd> {
//     fn get_data<'a>(&'a self, ident: &FieldIdent, idx: usize) -> Option<MaybeNa<&T>>;
// }
// impl GetData<u64> for DataFrame {
//     fn get_data<'a>(&'a self, ident: &FieldIdent, idx: usize) -> Option<MaybeNa<&u64>> {
//         self.store.get_unsigned_field(ident).and_then(|masked: &MaskedData<u64>| masked.get(idx))
//     }
// }
// impl GetData<i64> for DataFrame {
//     fn get_data<'a>(&'a self, ident: &FieldIdent, idx: usize) -> Option<MaybeNa<&i64>> {
//         self.store.get_signed_field(ident).and_then(|masked: &MaskedData<i64>| masked.get(idx))
//     }
// }
// impl GetData<String> for DataFrame {
//     fn get_data<'a>(&'a self, ident: &FieldIdent, idx: usize) -> Option<MaybeNa<&String>> {
//         self.store.get_text_field(ident).and_then(|masked: &MaskedData<String>| masked.get(idx))
//     }
// }
// impl GetData<bool> for DataFrame {
//     fn get_data<'a>(&'a self, ident: &FieldIdent, idx: usize) -> Option<MaybeNa<&bool>> {
//         self.store.get_boolean_field(ident).and_then(|masked: &MaskedData<bool>| masked.get(idx))
//     }
// }
// impl GetData<f64> for DataFrame {
//     fn get_data<'a>(&'a self, ident: &FieldIdent, idx: usize) -> Option<MaybeNa<&f64>> {
//         self.store.get_float_field(ident).and_then(|masked: &MaskedData<f64>| masked.get(idx))
//     }
// }
