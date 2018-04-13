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
use std::fmt::{self, Display, Formatter};

use indexmap::IndexMap;
use serde::ser::{self, Serialize, Serializer, SerializeMap};
use prettytable as pt;

use frame::{DataFrame, FramedField};
use masked::{MaybeNa};
use field::{FieldIdent, RFieldIdent, FieldType};
use error;
use store::DataStore;
use join::{Join, sort_merge_join, compute_merged_frames,
    compute_merged_field_list};
use apply::*;

/// A field in a `DataView`. Contains the (possibly-renamed) field identifier and the store index
/// with the underlying data.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ViewField {
    /// The field identifier, along with renaming information (if exists)
    pub rident: RFieldIdent,
    /// Frame index of the underlying data
    pub frame_idx: usize,
}

/// A 'view' into a data store. The primary struct for viewing and manipulating data.
#[derive(Debug, Clone, Default)]
pub struct DataView {
    pub(crate) frames: Vec<DataFrame>,
    pub(crate) fields: IndexMap<FieldIdent, ViewField>,
}

impl DataView {
    /// Generate a new subview of this DataView.
    pub fn v<L: IntoFieldList>(&self, s: L) -> DataView {
        let mut sub_fields = IndexMap::new();
        for ident in s.into_field_list().iter() {
            if let Some(field) = self.fields.get(ident) {
                sub_fields.insert(ident.clone(), field.clone());
            }
        }
        DataView {
            frames: self.frames.clone(),
            fields: sub_fields,
        }
    }
    /// Generate a new subview of this DataView, generating an error if a specified field does
    /// not exist.
    pub fn subview<L: IntoFieldList>(&self, s: L) -> error::Result<DataView> {
        let mut sub_fields = IndexMap::new();
        for ident in s.into_field_list().iter() {
            if let Some(field) = self.fields.get(ident) {
                sub_fields.insert(ident.clone(), field.clone());
            } else {
                return Err(error::AgnesError::FieldNotFound(ident.clone()));
            }
        }
        Ok(DataView {
            frames: self.frames.clone(),
            fields: sub_fields,
        })
    }
    /// Number of rows in this data view
    pub fn nrows(&self) -> usize {
        if self.frames.len() == 0 { 0 } else { self.frames[0].nrows() }
    }
    /// Returns `true` if the DataView is empty (has no rows or has no fields)
    pub fn is_empty(&self) -> bool {
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
    /// Return the field type for specified field
    pub fn get_field_type(&self, ident: &FieldIdent) -> Option<FieldType> {
        self.fields.get(ident).and_then(|view_field: &ViewField| {
            self.frames[view_field.frame_idx].get_field_type(&view_field.rident.ident)
        })
    }
    /// Returns `true` if this `DataView` contains this field.
    pub fn has_field(&self, s: &FieldIdent) -> bool {
        self.fields.contains_key(s)
    }

    // pub(crate) fn get_field_data(&self, field_name: &str) -> Option<FrameFieldData> {
    //     self.fields.get(field_name).and_then(|view_field: &ViewField| {
    //         self.get_viewfield_data(view_field)
    //     })
    // }
    // pub(crate) fn get_viewfield_data(&self, view_field: &ViewField) -> Option<FrameFieldData> {
    //     self.frames[view_field.frame_idx].get_field_data(&view_field.rident.ident)
    // }

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

    /// Merge this `DataView` with another `DataView` object, creating a new `DataView` with the
    /// same number of rows and all the fields from both source `DataView` objects.
    pub fn merge(&self, other: &DataView) -> error::Result<DataView> {
        if self.nrows() != other.nrows() {
            return Err(error::AgnesError::DimensionMismatch(
                "number of rows mismatch in merge".into()));
        }

        // compute merged stores (and mapping from 'other' store index references to combined
        // store vector)
        let (new_frames, other_store_indices) = compute_merged_frames(self, other);

        // compute merged field list
        let new_fields = compute_merged_field_list(self, other, &other_store_indices, None)?;

        Ok(DataView {
            frames: new_frames,
            fields: new_fields
        })
    }

    /// Combine two `DataView` objects using specified join, creating a new `DataStore` object with
    /// a subset of records from the two source `DataView`s according to the join parameters.
    ///
    /// Note that since this is creating a new `DataStore` object, it will be allocated new data to
    /// store the contents of the joined `DataView`s.
    pub fn join(&self, other: &DataView, join: Join) -> error::Result<DataStore> {
        match join.predicate {
            // Predicate::Equal => {
            //     hash_join(self, other, join)
            // },
            _ => {
                sort_merge_join(self, other, join)
            }
        }
    }
}

// impl ApplyToAllFieldElems for DataView {
//     fn apply_to_all_field_elems<T: ElemFn>(&self, mut f: T, ident: &FieldIdent)
//         -> Option<T::Output>
//     {
//         self.fields.get(&ident.to_string()).and_then(|view_field: &ViewField| {
//             self.frames[view_field.frame_idx].apply_to_all_field_elems(f, &ident)
//         })
//     }
// }

impl<'a> ApplyToElem<FieldIndexSelector<'a>> for DataView {
    fn apply_to_elem<T: ElemFn>(&self, f: T, select: FieldIndexSelector<'a>)
        -> Option<T::Output>
    {
        let (ident, idx) = select.index();
        self.fields.get(ident).and_then(|view_field: &ViewField| {
            self.frames[view_field.frame_idx].apply_to_elem(f, FieldIndexSelector(&ident, idx))
        })
    }
}
impl<'a> ApplyToField<FieldSelector<'a>> for DataView {
    fn apply_to_field<T: FieldFn>(&self, f: T, select: FieldSelector) -> Option<T::Output> {
        let ident = select.index();
        self.fields.get(ident).and_then(|view_field: &ViewField| {
            self.frames[view_field.frame_idx].apply_to_field(f,
                FieldSelector(&view_field.rident.ident))
        })
    }
}
// two fields on same dataview
impl<'a> ApplyToField2<FieldSelector<'a>> for DataView {
    fn apply_to_field2<T: Field2Fn>(&self, f: T, select: (FieldSelector, FieldSelector))
        -> Option<T::Output>
    {
        (self, self).apply_to_field2(f, select)
    }
}
// fields on two different dataviews
impl<'a, 'b, 'c> ApplyToField2<FieldSelector<'a>> for (&'b DataView, &'c DataView) {
    fn apply_to_field2<T: Field2Fn>(&self, f: T, select: (FieldSelector, FieldSelector))
        -> Option<T::Output>
    {
        let (ident0, ident1) = (select.0.index(), select.1.index());
        let vf0 = self.0.fields.get(ident0);
        let vf1 = self.1.fields.get(ident1);
        // let frame0 = self.0.fields.get(ident0).map(|view_field: &ViewField| {
        //     &self.0.frames[view_field.frame_idx]
        // });
        // let frame1 = self.1.fields.get(ident1).map(|view_field: &ViewField| {
        //     &self.1.frames[view_field.frame_idx]
        // });
        match (vf0, vf1) {
            (Some(vf0), Some(vf1)) => {
                (&self.0.frames[vf0.frame_idx], &self.1.frames[vf1.frame_idx])
                    .apply_to_field2(f, (
                        FieldSelector(&vf0.rident.ident),
                        FieldSelector(&vf1.rident.ident)
                    ))
            },
            _ => None
        }
    }
}


impl From<DataStore> for DataView {
    fn from(store: DataStore) -> DataView {
        let mut fields = IndexMap::new();
        for field in &store.fields {
            let ident = field.ty_ident.ident.clone();
            fields.insert(ident.clone(), ViewField {
                rident: RFieldIdent {
                    ident: ident,
                    rename: None
                },
                frame_idx: 0,
            });
        }
        DataView {
            frames: vec![store.into()],
            fields: fields
        }
    }
}

impl Display for DataView {
    fn fmt(&self, f: &mut Formatter) -> Result<(), fmt::Error> {
        if self.frames.len() == 0 || self.fields.len() == 0 {
            return write!(f, "Empty DataView");
        }
        const MAX_ROWS: usize = 1000;
        let nrows = self.frames[0].nrows();

        let mut table = pt::Table::new();
        table.set_titles(self.fields.keys().into());
        // let all_data = self.fields.values()
        //     .filter_map(|field| {
        //         // this should be guaranteed by construction of the DataView
        //         debug_assert_eq!(nrows, self.frames[field.frame_idx].nrows());
        //         self.frames[field.frame_idx].get_field_data(&field.rident.ident)
        //     })
        //     .collect::<Vec<_>>();
        for i in 0..nrows.min(MAX_ROWS) {
            let mut row = pt::row::Row::empty();
            for field in self.fields.values() {
                self.apply_to_elem(AddCellToRow { row: &mut row },
                    FieldIndexSelector(&field.rident.ident, i));
            }

            // for field_data in &all_data {
                // unwrap() in add_row_cell should be safe: store guarantees that all fields have
                // the same length (given by nrows)
                // match *field_data {
                //     FieldData::Unsigned(col) => row.add_cell(cell!(col.get(i).unwrap())),
                //     FieldData::Signed(col) => row.add_cell(cell!(col.get(i).unwrap())),
                //     FieldData::Text(col) => row.add_cell(cell!(col.get(i).unwrap())),
                //     FieldData::Boolean(col) => row.add_cell(cell!(col.get(i).unwrap())),
                //     FieldData::Float(col) => row.add_cell(cell!(col.get(i).unwrap())),
                // };
            // }
            table.add_row(row);
        }
        table.set_format(*pt::format::consts::FORMAT_NO_BORDER_LINE_SEPARATOR);
        table.fmt(f)
    }
}

struct AddCellToRow<'a> {
    row: &'a mut pt::row::Row
}
macro_rules! impl_apply_cell_to_row {
    ($name:tt; $ty:ty) => {
        fn $name(&mut self, value: MaybeNa<&$ty>) {
            self.row.add_cell(cell!(value))
        }
    }
}
impl<'a> ElemFn for AddCellToRow<'a> {
    type Output = ();
    impl_apply_cell_to_row!(apply_unsigned; u64);
    impl_apply_cell_to_row!(apply_signed;   i64);
    impl_apply_cell_to_row!(apply_text;     String);
    impl_apply_cell_to_row!(apply_boolean;  bool);
    impl_apply_cell_to_row!(apply_float;    f64);
}

// impl Serialize for DataView {
//     fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error> where S: Serializer {
//         let mut map = serializer.serialize_map(Some(self.fields.len()))?;
//         for field in self.fields.values() {
//             if let Some(data) = self.frames[field.frame_idx].get_field_data(&field.rident.ident) {
//                 assert!(self.frames[field.frame_idx].nrows() == data.len());
//                 map.serialize_entry(&field.rident.to_string(), &data)?;
//             }
//         }
//         map.end()
//     }
// }


// pub struct SerializeViewFn<'a, S: 'a + Serializer> {
//     serializer: &'a S,
//     map: &'a mut S::SerializeMap
// }
// impl<'a, S: Serializer> FieldFn for SerializeViewFn<'a, S> {
//     type Output = Result<S::Ok, S::Error>;
//     fn apply_unsigned<T: DataIndex<u64>>(&mut self, field: &T) -> Result<S::Ok, S::Error> {

//         let mut seq = self.serializer.serialize_seq(Some(field.len()));
//         (0..field.len()).all(|idx| match field.get_data(idx).unwrap() {
//             MaybeNa::Exists(&ref val) =>  seq.serialize_element(val)?,
//             MaybeNa::Na =>  seq.serialize_element("null")?
//         });
//         seq.end()
//     }
// }
impl Serialize for DataView {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error> where S: Serializer {
        let mut map = serializer.serialize_map(Some(self.fields.len()))?;
        for field in self.fields.values() {
            map.serialize_entry(&field.rident.to_string(), &FramedField {
                ident: field.rident.ident.clone(),
                frame: &self.frames[field.frame_idx]
            })?;
            // self.frames[field.frame_idx].apply_to_field(
            //     SerializeViewFn { serializer: &serializer, map: &mut map },
            //     NilSelector
            // );
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
pub struct FieldView {
    frame: DataFrame,
    field: RFieldIdent,
}

impl Serialize for FieldView {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where S: Serializer {
        if self.frame.has_field(&self.field.ident) {
            FramedField {
                ident: self.field.to_renamed_field_ident(),
                frame: &self.frame
            }.serialize(serializer)
        // }
        // if let Some(data) = self.frame.get_field_data(&self.field.ident) {
        //     data.serialize(serializer)
        } else {
            Err(ser::Error::custom(format!("missing field: {}", self.field.to_string())))
        }
    }
}
impl SerializeAsVec for FieldView {}

impl DataView {
    /// Create a `FieldView` object from a `DataView` object, if possible. Typically, use this on
    /// `DataView` objects with only a single field; however, if the `DataView` object has multiple
    /// fields, the first one will be used for this `FieldView`. Returns `None` if the `DataView`
    /// has no fields (is empty).
    pub fn as_fieldview(&self) -> Option<FieldView> {
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

impl IntoFieldList for Vec<FieldIdent> {
    fn into_field_list(self) -> Vec<FieldIdent> {
        self
    }
}
impl<'a> IntoFieldList for Vec<&'a FieldIdent> {
    fn into_field_list(self) -> Vec<FieldIdent> {
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
    ($val:expr) => {
        impl<'a> IntoFieldList for [&'a str; $val] {
            fn into_field_list(self) -> Vec<FieldIdent> {
                self.iter().map(|s| FieldIdent::Name(s.to_string())).collect()
            }
        }
    }
}
impl_into_field_list_str_arr!(1);
impl_into_field_list_str_arr!(2);
impl_into_field_list_str_arr!(3);
impl_into_field_list_str_arr!(4);
impl_into_field_list_str_arr!(5);
impl_into_field_list_str_arr!(6);
impl_into_field_list_str_arr!(7);
impl_into_field_list_str_arr!(8);
impl_into_field_list_str_arr!(9);
impl_into_field_list_str_arr!(10);
impl_into_field_list_str_arr!(11);
impl_into_field_list_str_arr!(12);
impl_into_field_list_str_arr!(13);
impl_into_field_list_str_arr!(14);
impl_into_field_list_str_arr!(15);
impl_into_field_list_str_arr!(16);
impl_into_field_list_str_arr!(17);
impl_into_field_list_str_arr!(18);
impl_into_field_list_str_arr!(19);
impl_into_field_list_str_arr!(20);


impl IntoFieldList for String {
    fn into_field_list(self) -> Vec<FieldIdent> {
        vec![FieldIdent::Name(self)]
    }
}
impl IntoFieldList for Vec<String> {
    fn into_field_list(mut self) -> Vec<FieldIdent> {
        self.drain(..).map(|s| FieldIdent::Name(s)).collect()
    }
}
macro_rules! impl_into_field_list_string_arr {
    ($val:expr) => {
        impl IntoFieldList for [String; $val] {
            fn into_field_list(self) -> Vec<FieldIdent> {
                // clone necessary since we're moving to the heap
                self.iter().map(|s| FieldIdent::Name(s.clone())).collect()
            }
        }
    }
}
impl_into_field_list_string_arr!(1);
impl_into_field_list_string_arr!(2);
impl_into_field_list_string_arr!(3);
impl_into_field_list_string_arr!(4);
impl_into_field_list_string_arr!(5);
impl_into_field_list_string_arr!(6);
impl_into_field_list_string_arr!(7);
impl_into_field_list_string_arr!(8);
impl_into_field_list_string_arr!(9);
impl_into_field_list_string_arr!(10);
impl_into_field_list_string_arr!(11);
impl_into_field_list_string_arr!(12);
impl_into_field_list_string_arr!(13);
impl_into_field_list_string_arr!(14);
impl_into_field_list_string_arr!(15);
impl_into_field_list_string_arr!(16);
impl_into_field_list_string_arr!(17);
impl_into_field_list_string_arr!(18);
impl_into_field_list_string_arr!(19);
impl_into_field_list_string_arr!(20);

#[cfg(test)]
mod tests {
    use super::*;
    use test_utils::*;
    use error::*;

    #[test]
    fn merge() {
        let ds1 = sample_emp_table();
        let ds2 = sample_emp_table_extra();

        let (dv1, dv2): (DataView, DataView) = (ds1.into(), ds2.into());
        println!("{}", dv1);
        println!("{}", dv2);
        let merged_dv: DataView = dv1.merge(&dv2).expect("merge failed");
        println!("{}", merged_dv);
        assert_eq!(merged_dv.nrows(), 7);
        assert_eq!(merged_dv.nfields(), 5);
        for (left, right) in merged_dv.fieldnames().iter()
            .zip(vec!["EmpId", "DeptId", "EmpName", "DidTraining", "VacationHrs"]
                    .iter().map(|&s| FieldIdent::Name(s.into())))
        {
            assert_eq!(left, &&right);
        }
    }

    #[test]
    fn merge_dimension_mismatch() {
        let ds1 = sample_emp_table();
        let ds2 = sample_dept_table();

        let (dv1, dv2): (DataView, DataView) = (ds1.into(), ds2.into());
        println!("{}", dv1);
        println!("{}", dv2);
        match dv1.merge(&dv2) {
            Ok(_) => { panic!("Merge was expected to fail (dimension mismatch), but succeeded"); },
            Err(AgnesError::DimensionMismatch(_)) => { /* expected */ },
            Err(e) => { panic!("Incorrect error: {:?}", e); },
        };
    }

    #[test]
    fn merge_field_collision() {
        let ds1 = sample_emp_table();
        let ds2 = sample_emp_table();

        let (dv1, dv2): (DataView, DataView) = (ds1.into(), ds2.into());
        println!("{}", dv1);
        println!("{}", dv2);
        match dv1.merge(&dv2) {
            Ok(_) => { panic!("Merge expected to fail (field collision), but succeeded"); },
            Err(AgnesError::FieldCollision(fields)) => {
                assert_eq!(fields, vec!["EmpId", "DeptId", "EmpName"]
                    .iter().map(|&s| FieldIdent::Name(s.into())).collect::<Vec<_>>());
            },
            Err(e) => { panic!("Incorrect error: {:?}", e); }
        }
    }

    #[test]
    fn rename() {
        let ds = sample_emp_table();
        let mut dv: DataView = ds.into();
        println!("{}", dv);
        assert_field_lists_match(dv.fieldnames(), vec!["EmpId", "DeptId", "EmpName"]);
        dv.rename("DeptId", "Department Id").expect("rename failed");
        println!("{}", dv);
        assert_field_lists_match(dv.fieldnames(), vec!["EmpId", "Department Id", "EmpName"]);
        dv.rename("Department Id", "DeptId").expect("rename failed");
        println!("{}", dv);
        assert_field_lists_match(dv.fieldnames(), vec!["EmpId", "DeptId", "EmpName"]);
    }

    #[test]
    fn rename_field_collision() {
        let ds = sample_emp_table();
        let mut dv: DataView = ds.into();
        println!("{}", dv);
        assert_field_lists_match(dv.fieldnames(), vec!["EmpId", "DeptId", "EmpName"]);
        match dv.rename("DeptId", "EmpId") {
            Ok(_) => { panic!("Rename expected to fail (field collision), but succeeded"); },
            Err(AgnesError::FieldCollision(fields)) => {
                assert_eq!(fields, vec!["EmpId"]
                    .iter().map(|&s| FieldIdent::Name(s.into())).collect::<Vec<_>>());
            },
            Err(e) => { panic!("Incorrect error: {:?}", e); }
        }
        println!("{}", dv);
    }

    #[test]
    fn rename_field_not_found() {
        let ds = sample_emp_table();
        let mut dv: DataView = ds.into();
        println!("{}", dv);
        assert_field_lists_match(dv.fieldnames(), vec!["EmpId", "DeptId", "EmpName"]);
        match dv.rename("Department Id", "DepartmentId") {
            Ok(_) => { panic!("Rename expected to fail (field not found), but succeeded"); },
            Err(AgnesError::FieldNotFound(field)) => {
                assert_eq!(field, FieldIdent::Name("Department Id".to_string()));
            },
            Err(e) => { panic!("Incorrect error: {:?}", e); }
        }
        println!("{}", dv);
    }

    #[test]
    fn subview() {
        let ds = sample_emp_table();
        let dv: DataView = ds.into();
        assert_eq!(dv.frames[0].store_ref_count(), 1);
        assert_field_lists_match(dv.fieldnames(), vec!["EmpId", "DeptId", "EmpName"]);

        let subdv1 = dv.v("EmpId");
        assert_eq!(dv.frames[0].store_ref_count(), 2);
        assert_eq!(subdv1.nrows(), 7);
        assert_eq!(subdv1.nfields(), 1);
        let subdv1 = dv.subview("EmpId").expect("subview failed");
        assert_eq!(dv.frames[0].store_ref_count(), 3);
        assert_eq!(subdv1.nrows(), 7);
        assert_eq!(subdv1.nfields(), 1);

        let subdv2 = dv.v(vec!["EmpId", "DeptId"]);
        assert_eq!(dv.frames[0].store_ref_count(), 4);
        assert_eq!(subdv2.nrows(), 7);
        assert_eq!(subdv2.nfields(), 2);
        let subdv2 = dv.subview(vec!["EmpId", "DeptId"]).expect("subview failed");
        assert_eq!(dv.frames[0].store_ref_count(), 5);
        assert_eq!(subdv2.nrows(), 7);
        assert_eq!(subdv2.nfields(), 2);

        let subdv3 = dv.v(vec!["EmpId", "DeptId", "EmpName"]);
        assert_eq!(dv.frames[0].store_ref_count(), 6);
        assert_eq!(subdv3.nrows(), 7);
        assert_eq!(subdv3.nfields(), 3);
        let subdv3 = dv.subview(vec!["EmpId", "DeptId", "EmpName"]).expect("subview failed");
        assert_eq!(dv.frames[0].store_ref_count(), 7);
        assert_eq!(subdv3.nrows(), 7);
        assert_eq!(subdv3.nfields(), 3);

        // Subview of a subview
        let subdv4 = subdv2.v("DeptId");
        assert_eq!(dv.frames[0].store_ref_count(), 8);
        assert_eq!(subdv4.nrows(), 7);
        assert_eq!(subdv4.nfields(), 1);
        let subdv4 = subdv2.subview("DeptId").expect("subview failed");
        assert_eq!(dv.frames[0].store_ref_count(), 9);
        assert_eq!(subdv4.nrows(), 7);
        assert_eq!(subdv4.nfields(), 1);
    }

    #[test]
    fn subview_fail() {
        let ds = sample_emp_table();
        let dv: DataView = ds.into();
        assert_eq!(dv.frames[0].store_ref_count(), 1);
        assert_field_lists_match(dv.fieldnames(), vec!["EmpId", "DeptId", "EmpName"]);

        // "Employee Name" does not exist
        let subdv1 = dv.v(vec!["EmpId", "DeptId", "Employee Name"]);
        assert_eq!(dv.frames[0].store_ref_count(), 2);
        assert_eq!(subdv1.nrows(), 7);
        assert_eq!(subdv1.nfields(), 2);
        match dv.subview(vec!["EmpId", "DeptId", "Employee Name"]) {
            Ok(_) => { panic!("expected error (field not found), but succeeded"); },
            Err(AgnesError::FieldNotFound(field)) => {
                assert_eq!(field, FieldIdent::Name("Employee Name".into()));
            },
            Err(e) => { panic!("Incorrect error: {:?}", e); }
        }

        let subdv2 = dv.v("Nonexistant");
        assert_eq!(dv.frames[0].store_ref_count(), 3);
        assert_eq!(subdv2.nrows(), 7); // still 7 rows, just no fields
        assert_eq!(subdv2.nfields(), 0);
        match dv.subview(vec!["Nonexistant"]) {
            Ok(_) => { panic!("expected error (field not found), but succeeded"); },
            Err(AgnesError::FieldNotFound(field)) => {
                assert_eq!(field, FieldIdent::Name("Nonexistant".into()));
            },
            Err(e) => { panic!("Incorrect error: {:?}", e); }
        }
    }
}
