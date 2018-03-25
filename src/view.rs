//! Main `DataView` struct and associated implementations.

use std::fmt::{self, Display, Formatter};
use std::ops::Rem;
use std::rc::Rc;

use indexmap::IndexSet;
use serde::ser::{self, Serialize, Serializer, SerializeMap};

use store::DataStore;
use masked::FieldData;
use field::FieldIdent;
use MaybeNa;

/// A 'view' into a data store. The primary struct for viewing and manipulating data.
#[derive(Debug, Clone, Default)]
pub struct DataView {
    store: Rc<DataStore>,
    fields: IndexSet<FieldIdent>,
}

impl DataView {
    /// Generate a new subview of this DataView.
    pub fn v<L: IntoFieldList>(&self, s: L) -> DataView {
        let mut sub_fields = IndexSet::new();
        for ident in s.into_field_list().iter() {
            if let Some(field) = self.fields.get(ident) {
                sub_fields.insert(field.clone());
            }
        }
        DataView {
            store: self.store.clone(),
            fields: sub_fields,
        }
    }
}

/// A shortcut for `dataview.v`
impl<'a, L: IntoFieldList> Rem<L> for &'a DataView {
    type Output = DataView;

    fn rem(self, s: L) -> DataView {
        self.v(s)
    }
}

/// Conversion trait for converting into a vector of `FieldIdent` objects. Used for indexing into
/// a `DataView`.
pub trait IntoFieldList {
    /// Convert into a `Vec<FieldIdent>`
    fn into_field_list(self) -> Vec<FieldIdent>;
}


impl<'a> IntoFieldList for &'a str {
    fn into_field_list(self) -> Vec<FieldIdent> {
        vec![FieldIdent::Name(self.to_string())]
    }
}
impl<'a> IntoFieldList for Vec<&'a str> {
    fn into_field_list(self) -> Vec<FieldIdent> {
        self.iter().map(|s| FieldIdent::Name(s.to_string())).collect()
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
                self.iter().map(|s| FieldIdent::Name(s.to_string())).collect()
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

impl From<DataStore> for DataView {
    fn from(store: DataStore) -> DataView {
        let mut fields = IndexSet::new();
        for field in &store.fields {
            fields.insert(field.ty_ident.ident.clone());
        }
        DataView {
            store: Rc::new(store),
            fields: fields
        }
    }
}

macro_rules! format_opt {
    ($fmt:expr, $value:expr) => {{
        match $value {
            MaybeNa::Exists(ref value) => format!($fmt, value),
            MaybeNa::Na                => "NA".to_string()
        }
    }}
}
macro_rules! write_column {
    ($formatter:expr, $value:expr, width = $width:expr) => {{
        const GAP: &str = "  ";
        write!($formatter, "{:>width$.width$}{}", $value, GAP, width = $width)
    }}
}
macro_rules! write_column_opt {
    ($formatter:expr, $value:expr, width = $width:expr) => {{
        match $value {
            MaybeNa::Exists(ref value) => write_column!($formatter, value, width = $width),
            MaybeNa::Na                => write_column!($formatter, "NA", width = $width)
        }
    }}
}

impl Display for DataView {
    fn fmt(&self, f: &mut Formatter) -> Result<(), fmt::Error> {
        const MAX_COL_WIDTH: usize = 30;

        let mut cols = vec![];
        for field in &self.fields {
            if let Some(data) = self.store.get_field_data(field) {
                // go through the data to find the column width
                let mut max_width = 0;
                for i in 0..self.store.nrows() {
                    // col.get(i).unwrap() should be safe: store guarantees that all fields have the
                    // same length (given by self.store.nrows())
                    max_width = max_width.max(match data {
                        FieldData::Unsigned(col) => {
                            format_opt!("{}", col.get(i).unwrap()).len()
                        },
                        FieldData::Signed(col) => { format_opt!("{}", col.get(i).unwrap()).len() },
                        FieldData::Text(col) => { format_opt!("{}", col.get(i).unwrap()).len() },
                        FieldData::Boolean(col) => { format_opt!("{}", col.get(i).unwrap()).len() },
                        FieldData::Float(col) => { format_opt!("{}", col.get(i).unwrap()).len() },
                    });
                }
                cols.push((data, MAX_COL_WIDTH.min(max_width), field.to_string()));
            }
        }
        for j in 0..cols.len() {
            write_column!(f, cols[j].2, width = cols[j].1)?;
        }
        writeln!(f)?;
        for i in 0..self.store.nrows() {
            for j in 0..cols.len() {
                let col_width = cols[j].1;
                // col.get(i).unwrap() should be safe: store guarantees that all fields have the
                // same length (given by self.store.nrows())
                match cols[j].0 {
                    FieldData::Unsigned(col) => {
                        write_column_opt!(f, col.get(i).unwrap(), width = col_width)?;
                    },
                    FieldData::Signed(col) => {
                        write_column_opt!(f, col.get(i).unwrap(), width = col_width)?;
                    },
                    FieldData::Text(col) => {
                        write_column_opt!(f, col.get(i).unwrap(), width = col_width)?;
                    },
                    FieldData::Boolean(col) => {
                        write_column_opt!(f, col.get(i).unwrap(), width = col_width)?;
                    },
                    FieldData::Float(col) => {
                        write_column_opt!(f, col.get(i).unwrap(), width = col_width)?;
                    },
                }
            }
            writeln!(f)?;
        }
        Ok(())
    }
}

impl Serialize for DataView {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error> where S: Serializer {
        let mut map = serializer.serialize_map(Some(self.fields.len()))?;
        for field in &self.fields {
            if let Some(data) = self.store.get_field_data(field) {
                assert!(self.store.nrows() == data.len());
                map.serialize_entry(&field.to_string(), &data)?;
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
    field: FieldIdent,
}

impl Serialize for FieldView {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where S: Serializer {
        if let Some(data) = self.store.get_field_data(&self.field) {
            data.serialize(serializer)
        } else {
            Err(ser::Error::custom(format!("missing field: {}", self.field)))
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
            Some(FieldView {
                store: self.store.clone(),
                // self.fields it not empty, so unwrap is safe
                field: self.fields.iter().next().unwrap().clone(),
            })
        }
    }
}
