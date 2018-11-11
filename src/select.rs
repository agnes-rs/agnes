/*!
Traits and structures for selecting a field from a data structure.
*/
use std::fmt::Debug;
use std::marker::PhantomData;

use field::Value;
use access::{DataIndex};
use error::*;
use fieldlist::{FieldTypes, FSelector};
// use data_types::{AssocTypes, DataType, TypeSelector, DTypeList};

/// Type for accessing a specified field (identified by a `FieldIdent`) for an underlying data
/// structure.
#[derive(Debug, Clone)]
pub struct Selection<D: DataIndex>
{
    /// Underlying data structure for this selection. Contains the field identified by `ident`.
    data: D,
}
impl<D> Selection<D>
    where D: DataIndex
{
    /// Create a new `Selection` object from specified data and identifier.
    pub fn new(data: D) -> Selection<D> {
        Selection {
            data,
        }
    }
}
impl<U> DataIndex for Selection<U>
    where U: DataIndex,
          <U as DataIndex>::DType: Debug
{
    type DType = U::DType;

    fn get_datum(&self, idx: usize) -> Result<Value<&Self::DType>> {
        self.data.get_datum(idx)
    }
    fn len(&self) -> usize {
        self.data.len()
    }
}

/// Trait for accessing the data of a single field as a [Selection](struct.Selection.html) struct
/// which implements [DataIndex](../access/trait.DataIndex.html).
pub trait FSelect
{
    /// Returns a [Selection](struct.Selection.html) struct containing the data for the field
    /// specified by `ident`.
    ///
    /// This method is a convenience method for calling the [select](trait.SelectField.html#select)
    /// method on the [SelectField](trait.SelectField.html) trait.
    fn field<'a, Ident: 'a>(&'a self)
        -> Selection<<Self as SelectField<'a, Ident>>::Output>
        where Self: SelectField<'a, Ident>,
    {
        Selection::new(SelectField::select(self))
    }
}

/// Trait implemented by data structures to provide access to data for a single field.
pub trait SelectField<'a, Ident>
{
    /// The return type for the `select` method.
    type Output: DataIndex;

    /// Returns an object that provides [DataIndex](../access/trait.DataIndex.html) access to the
    /// data in the field specified by `ident`.
    fn select_field(&'a self) -> Self::Output;
}

#[cfg(test)]
mod tests {
    use super::FSelect;

    use field::Value;
    // use test_utils::*;
    use access::DataIndex;
    use error::*;

    // #[test]
    // fn select() {
    //     let dv = sample_merged_emp_table();
    //     println!("{}", dv);
    //     let result = dv.field("EmpId").unwrap().iter()
    //         .map(|datum: Value<&u64>| if datum.exists() { 1i64 } else { 0 })
    //         .collect::<Vec<_>>();
    //     assert_eq!(result, vec![1, 1, 1, 1, 1, 1, 1]);
    // }

    // #[test]
    // fn select_wrong_type() {
    //     let dv = sample_merged_emp_table();
    //     println!("{}", dv);
    //     let result = dv.field::<i64, _>("EmpId");
    //     match result {
    //         Err(AgnesError::IncompatibleTypes { .. }) => {},
    //         Err(_) => { panic!["wrong error when calling field() with incorrect type"]; },
    //         Ok(_) => { panic!["expected error when calling field() with incorrect type, but \
    //                            received Ok"]; }
    //     }
    // }
}
