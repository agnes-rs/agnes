/*!
Traits and structures for selecting a field or fields from a data structure.
*/
use std::slice::Iter;
use std::sync::Arc;
use std::iter::FromIterator;

use store::DataStore;
use field::FieldIdent;
use frame::{DataFrame, Framed};
use view::{IntoFieldList, DataView, ViewField};
use access::{FieldData, OwnedOrRef};
use error::*;

/// Trait for types that can have one or more fields selected (for applying functions).
pub trait Select {
    /// Select the specified fields.
    fn select<'a, L: IntoFieldList>(&'a self, fields: L) -> SelectionList<'a, Self>;
    /// Select one field. Useful shothand for using `select` with only one field.
    fn select_one<'a, I: Into<FieldIdent>>(&'a self, ident: I) -> Selection<'a, Self> {
        self.select(vec![ident.into()]).first().unwrap()
    }
}
impl<D> Select for D {
    fn select<'a, L: IntoFieldList>(&'a self, fields: L) -> SelectionList<'a, Self> {
        let mut fields = fields.into_field_list();
        let list = fields.drain(..).map(|ident| Selection::new(self, ident))
            .collect::<SelectionList<D>>();
        list
    }
}

/// Type for accessing a specified field (identified by a `FieldIdent`) for an underlying data
/// structure.
#[derive(Debug, Clone)]
pub struct Selection<'a, D: 'a + ?Sized> {
    /// Underlying data structure for this selection. Contains the field identified by `ident`.
    pub data: &'a D,
    /// Identifier of the field within the `data` structure.
    pub ident: FieldIdent,
}
impl<'a, D> Selection<'a, D> {
    /// Create a new `Selection` object from specified data and identifier.
    pub fn new(data: &'a D, ident: FieldIdent) -> Selection<'a, D> {
        Selection {
            data,
            ident
        }
    }
}

/// Utility trait for directly accessing field data for the specified field from a data structure.
pub trait Field<'a> {
    /// Get the `FieldData` structure for the specified field of this data structue.
    fn field<I: Into<FieldIdent>>(&'a self, ident: I) -> Result<FieldData<'a>>;
}

impl<'a, D> Field<'a> for D where D: 'a, Selection<'a, D>: GetFieldData<'a> {
    fn field<I: Into<FieldIdent>>(&'a self, ident: I) -> Result<FieldData<'a>> {
        self.select_one(ident).get_field_data()
    }
}

/// Trait for retrieving a `FieldData` struct (containing the data for a single field) from a data
/// structure. Used with `Selection` objects (which select the specified field to retrieve).
pub trait GetFieldData<'a> {
    /// Get a `FieldData` oject from this data structure.
    fn get_field_data(&self) -> Result<FieldData<'a>>;
}
impl<'a> GetFieldData<'a> for Selection<'a, DataView> {
    fn get_field_data(&self) -> Result<FieldData<'a>> {
        self.data.fields.get(&self.ident)
            .ok_or(AgnesError::FieldNotFound(self.ident.clone()))
            .and_then(|view_field: &ViewField| {
                self.data.frames[view_field.frame_idx]
                    .select_one(view_field.rident.ident.clone())
                    .get_field_data()
            }
        )
    }
}
impl<'a> GetFieldData<'a> for Selection<'a, DataFrame> {
    fn get_field_data(&self) -> Result<FieldData<'a>> {
        self.data.store
            .select_one(self.ident.clone())
            .get_field_data()
            .map(|field_data| {
                match field_data {
                    FieldData::Unsigned(data) =>
                        FieldData::Unsigned(OwnedOrRef::Owned(Box::new(
                            Framed::new(&self.data, data)
                        ))),
                    FieldData::Signed(data) =>
                        FieldData::Signed(OwnedOrRef::Owned(Box::new(
                            Framed::new(&self.data, data)
                        ))),
                    FieldData::Text(data) =>
                        FieldData::Text(OwnedOrRef::Owned(Box::new(
                            Framed::new(&self.data, data)
                        ))),
                    FieldData::Boolean(data) =>
                        FieldData::Boolean(OwnedOrRef::Owned(Box::new(
                            Framed::new(&self.data, data)
                        ))),
                    FieldData::Float(data) =>
                        FieldData::Float(OwnedOrRef::Owned(Box::new(
                            Framed::new(&self.data, data)
                        ))),
                }
            })
    }
}
impl<'a> GetFieldData<'a> for Selection<'a, Arc<DataStore>> {
    fn get_field_data(&self) -> Result<FieldData<'a>> {
        self.data.get_field_data(&self.ident)
            .ok_or(AgnesError::FieldNotFound(self.ident.clone()))
    }
}

/// Set of selections (output of a `select` call).
pub struct SelectionList<'a, D: 'a + ?Sized> {
    data: Vec<Selection<'a, D>>
}
impl<'a, D> SelectionList<'a, D> where D: 'a + ?Sized, Selection<'a, D>: GetFieldData<'a>
{
    /// Provides a `Vec` of `FieldData` structs containing data for the fields in this
    /// `SelectionList`.
    pub fn field_data(&'a self) -> Result<Vec<FieldData<'a>>> {
        self.data.iter()
            .map(|selection| selection.get_field_data())
            .collect::<Result<Vec<_>>>()
    }
    /// Provides an iterator over the `FieldData` structs for the fields in this `SelectionList`.
    pub fn field_iter(&'a self) -> FieldIter<'a, D> {
        FieldIter { inner: self.data.iter() }
    }
}
impl<'a, D> SelectionList<'a, D> where D: 'a + ?Sized {
    fn first(&self) -> Option<Selection<'a, D>> {
        self.data.get(0).map(|&ref selection| {
            Selection {
                data: selection.data,
                ident: selection.ident.clone()
            }
        })
    }
}

/// Iterator over fields reference in a `SelectionList` object.
pub struct FieldIter<'a, D: 'a + ?Sized> where Selection<'a, D>: GetFieldData<'a> {
    inner: Iter<'a, Selection<'a, D>>,
}
impl<'a, D: 'a + ?Sized> Iterator for FieldIter<'a, D>
    where Selection<'a, D>: GetFieldData<'a>
{
    type Item = Result<FieldData<'a>>;

    fn next(&mut self) -> Option<Result<FieldData<'a>>> {
        self.inner.next().map(|selection| selection.get_field_data())
    }
}

impl<'a, D> FromIterator<Selection<'a, D>> for SelectionList<'a, D>
    where D: 'a + ?Sized
{
    fn from_iter<I: IntoIterator<Item=Selection<'a, D>>>(iter: I) -> Self {
        let mut v = vec![];

        for i in iter {
            v.push(i);
        }

        SelectionList { data: v }
    }
}

#[cfg(test)]
mod tests {
    use super::Field;

    use masked::MaybeNa;
    use test_utils::*;

    #[test]
    fn select() {
        let dv = sample_merged_emp_table();
        println!("{}", dv);
        // let result = dv.select_one("EmpId").get_field_data().unwrap()
        //     .map(|datum: MaybeNa<&u64>| if datum.exists() { 1i64 } else { 0 }).unwrap();
        // for datum in result.data_iter::<i64>() {
        //     assert_eq!(datum.unwrap(), &1i64);
        // }
        let result = dv.field("EmpId").unwrap().data_iter()
            .map(|datum: MaybeNa<&u64>| if datum.exists() { 1i64 } else { 0 })
            .collect::<Vec<_>>();
        assert_eq!(result, vec![1, 1, 1, 1, 1, 1, 1]);
    }
}
