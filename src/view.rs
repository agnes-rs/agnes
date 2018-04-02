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
use std::rc::Rc;

use indexmap::IndexMap;
use serde::ser::{self, Serialize, Serializer, SerializeMap};
use prettytable as pt;

use store::DataStore;
use masked::FieldData;
use field::{FieldIdent, RFieldIdent};
use error;
use join::{Join, Predicate, hash_join, sort_merge_join, compute_merged_stores,
    compute_merged_field_list};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ViewField {
    pub rident: RFieldIdent,
    pub store_idx: usize,
}


/// A 'view' into a data store. The primary struct for viewing and manipulating data.
#[derive(Debug, Clone, Default)]
pub struct DataView {
    pub(crate) stores: Vec<Rc<DataStore>>,
    pub(crate) fields: IndexMap<String, ViewField>,
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
            stores: self.stores.clone(),
            fields: sub_fields,
        }
    }
    /// Number of rows in this data view
    pub fn nrows(&self) -> usize {
        if self.stores.len() == 0 { 0 } else { self.stores[0].nrows() }
    }
    /// Number of fields in this data view
    pub fn nfields(&self) -> usize {
        self.fields.len()
    }

    pub(crate) fn get_field_data(&self, view_field: &ViewField) -> Option<FieldData> {
        self.stores[view_field.store_idx].get_field_data(&view_field.rident.ident)
    }

    /// Rename a field of this DataView.
    pub fn rename<T, U>(&mut self, orig: T, new: U) -> error::Result<()> where
        T: Into<FieldIdent>,
        U: Into<FieldIdent>
    {
        let (orig, new) = (orig.into(), new.into());
        let new_vf = if let Some(ref orig_vf) = self.fields.get(&orig.to_string()) {
            ViewField {
                rident: RFieldIdent {
                    ident: orig_vf.rident.ident.clone(),
                    rename: Some(new.to_string())
                },
                store_idx: orig_vf.store_idx,
            }
        } else {
            return Err(error::AgnesError::FieldNotFound(orig));
        };
        self.fields.insert(new_vf.rident.to_string(), new_vf);
        self.fields.swap_remove(&orig.to_string());
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
        let (new_stores, other_store_indices) = compute_merged_stores(self, other);

        // compute merged field list
        let new_fields = compute_merged_field_list(self, other, &other_store_indices)?;

        Ok(DataView {
            stores: new_stores,
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
            Predicate::Equal => {
                hash_join(self, other, join)
            },
            _ => {
                sort_merge_join(self, other, join)
            }
        }
    }
}

impl From<DataStore> for DataView {
    fn from(store: DataStore) -> DataView {
        let mut fields = IndexMap::new();
        for field in &store.fields {
            let ident = field.ty_ident.ident.clone();
            fields.insert(ident.to_string(), ViewField {
                rident: RFieldIdent {
                    ident: ident.clone(),
                    rename: None
                },
                store_idx: 0,
            });
        }
        DataView {
            stores: vec![Rc::new(store)],
            fields: fields
        }
    }
}

impl Display for DataView {
    fn fmt(&self, f: &mut Formatter) -> Result<(), fmt::Error> {
        if self.stores.len() == 0 || self.fields.len() == 0 {
            return write!(f, "Empty DataView");
        }
        const MAX_ROWS: usize = 1000;
        let nrows = self.stores[0].nrows();

        let mut table = pt::Table::new();
        table.set_titles(self.fields.keys().into());
        let all_data = self.fields.values()
            .filter_map(|field| {
                // this should be guaranteed by construction of the DataView
                assert_eq!(nrows, self.stores[field.store_idx].nrows());
                self.stores[field.store_idx].get_field_data(&field.rident.ident)
            })
            .collect::<Vec<_>>();
        for i in 0..nrows.min(MAX_ROWS) {
            let mut row = pt::row::Row::empty();
            for field_data in &all_data {
                // col.get(i).unwrap() should be safe: store guarantees that all fields have
                // the same length (given by nrows)
                match *field_data {
                    FieldData::Unsigned(col) => row.add_cell(cell!(col.get(i).unwrap())),
                    FieldData::Signed(col) => row.add_cell(cell!(col.get(i).unwrap())),
                    FieldData::Text(col) => row.add_cell(cell!(col.get(i).unwrap())),
                    FieldData::Boolean(col) => row.add_cell(cell!(col.get(i).unwrap())),
                    FieldData::Float(col) => row.add_cell(cell!(col.get(i).unwrap())),
                };
            }
            table.add_row(row);
        }
        table.set_format(*pt::format::consts::FORMAT_NO_BORDER_LINE_SEPARATOR);
        table.fmt(f)
    }
}

impl Serialize for DataView {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error> where S: Serializer {
        let mut map = serializer.serialize_map(Some(self.fields.len()))?;
        for field in self.fields.values() {
            if let Some(data) = self.stores[field.store_idx].get_field_data(&field.rident.ident) {
                assert!(self.stores[field.store_idx].nrows() == data.len());
                map.serialize_entry(&field.rident.to_string(), &data)?;
            }
        }
        map.end()
    }
}

/// Marker trait to denote an object that serializes into a vector format
pub trait SerializeAsVec: Serialize {}
impl<T> SerializeAsVec for Vec<T> where T: Serialize {}

/// A 'view' into a single field's data in a data store. This is a specialty view used to serialize
/// a `DataView` as a single sequence instead of as a map.
#[derive(Debug, Clone)]
pub struct FieldView {
    store: Rc<DataStore>,
    field: RFieldIdent,
}

impl Serialize for FieldView {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where S: Serializer {
        if let Some(data) = self.store.get_field_data(&self.field.ident) {
            data.serialize(serializer)
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
                store: self.stores[field.store_idx].clone(),
                field: field.rident.clone(),
            })
        }
    }
}

/// Conversion trait for converting into a vector of Strings. Used for indexing into a `DataView`.
pub trait IntoFieldList {
    /// Convert into a `Vec<String>`
    fn into_field_list(self) -> Vec<String>;
}


impl<'a> IntoFieldList for &'a str {
    fn into_field_list(self) -> Vec<String> {
        vec![self.to_string()]
    }
}
impl<'a> IntoFieldList for Vec<&'a str> {
    fn into_field_list(self) -> Vec<String> {
        self.iter().map(|s| s.to_string()).collect()
    }
}
macro_rules! impl_into_field_list_str_arr {
    ($val:expr) => {
        impl<'a> IntoFieldList for [&'a str; $val] {
            fn into_field_list(self) -> Vec<String> {
                self.iter().map(|s| s.to_string()).collect()
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
    fn into_field_list(self) -> Vec<String> {
        vec![self]
    }
}
impl IntoFieldList for Vec<String> {
    fn into_field_list(self) -> Vec<String> {
        self
    }
}
macro_rules! impl_into_field_list_string_arr {
    ($val:expr) => {
        impl IntoFieldList for [String; $val] {
            fn into_field_list(self) -> Vec<String> {
                // clone necessary since we're moving to the heap
                self.iter().cloned().collect()
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
