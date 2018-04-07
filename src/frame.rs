/*!
Structs and implementation for Frame-level data structure. A `DataFrame` is a reference to an
underlying data store, along with record-based filtering and sorting details.
*/
use std::rc::Rc;

use bit_vec::BitVec;

use store::DataStore;
use masked::FieldData;
use field::{FieldIdent, FieldType};

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
    filter: Option<Filter>,
    sort_order: Option<Vec<usize>>,
    store: Rc<DataStore>,
}
impl DataFrame {
    /// Number of rows that pass the filter in this frame.
    pub fn nrows(&self) -> usize {
        match self.filter {
            Some(ref filter) => filter.len(),
            None => self.store.nrows()
        }
    }
    pub(crate) fn get_field_data(&self, field: &FieldIdent) -> Option<FieldData> {
        self.store.get_field_data(field)
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
}
impl From<DataStore> for DataFrame {
    fn from(store: DataStore) -> DataFrame {
        DataFrame {
            filter: None,
            sort_order: None,
            store: Rc::new(store)
        }
    }
}
