/*!
Traits and structures for selecting a field from a data structure.
*/
use std::marker::PhantomData;
use field::Value;
use field::{FieldIdent};
use access::{DataIndex};
use error::*;
use data_types::{AssocTypes, DataType, TypeSelector, DTypeList};

/// Type for accessing a specified field (identified by a `FieldIdent`) for an underlying data
/// structure.
#[derive(Debug, Clone)]
pub struct Selection<DTypes, D, T> {
    /// Underlying data structure for this selection. Contains the field identified by `ident`.
    data: D,
    /// Identifier of the field within the `data` structure.
    pub(crate) ident: FieldIdent,
    _marker_dt: PhantomData<DTypes>,
    _marker_t: PhantomData<T>
}
impl<DTypes, D, T> Selection<DTypes, D, T> {
    /// Create a new `Selection` object from specified data and identifier.
    pub fn new(data: D, ident: FieldIdent) -> Selection<DTypes, D, T> {
        Selection {
            data,
            ident,
            _marker_dt: PhantomData,
            _marker_t: PhantomData,
        }
    }
}
impl<DTypes, U, T> DataIndex<DTypes> for Selection<DTypes, U, T>
    where DTypes: DTypeList,
          T: DataType<DTypes>,
          U: DataIndex<DTypes, DType=T>,
{
    type DType = T;

    fn get_datum(&self, idx: usize) -> Result<Value<&T>> {
        self.data.get_datum(idx)
    }
    fn len(&self) -> usize {
        self.data.len()
    }
}

/// Trait for accessing the data of a single field as a [Selection](struct.Selection.html) struct
/// which implements [DataIndex](../access/trait.DataIndex.html).
pub trait Field<DTypes>
    where DTypes: DTypeList
{
    /// Returns a [Selection](struct.Selection.html) struct containing the data for the field
    /// specified by `ident`.
    ///
    /// This method is a convenience method for calling the [select](trait.SelectField.html#select)
    /// method on the [SelectField](trait.SelectField.html) trait.
    fn field<'a, T: 'a + DataType<DTypes>, I: Into<FieldIdent>>(&'a self, ident: I)
        -> Result<Selection<DTypes, <Self as SelectField<'a, T, DTypes>>::Output, T>>
        where Self: SelectField<'a, T, DTypes>,
              DTypes: 'a + AssocTypes,
              DTypes::Storage: TypeSelector<DTypes, T>
    {
        let ident = ident.into();
        SelectField::select(self, ident.clone())
            .map(|data| Selection::new(data, ident))
    }
}

/// Trait implemented by data structures to provide access to data for a single field.
pub trait SelectField<'a, T, DTypes>
    where DTypes: DTypeList,
          T: 'a + DataType<DTypes>
{
    /// The return type for the `select` method.
    type Output: DataIndex<DTypes, DType=T>;

    /// Returns an object that provides [DataIndex](../access/trait.DataIndex.html) access to the
    /// data in the field specified by `ident`.
    fn select(&'a self, ident: FieldIdent) -> Result<Self::Output>
        where DTypes: AssocTypes,
              DTypes::Storage: TypeSelector<DTypes, T>;
}

#[cfg(test)]
mod tests {
    use super::Field;

    use field::Value;
    use test_utils::*;
    use access::DataIndex;
    use error::*;

    #[test]
    fn select() {
        let dv = sample_merged_emp_table();
        println!("{}", dv);
        let result = dv.field("EmpId").unwrap().iter()
            .map(|datum: Value<&u64>| if datum.exists() { 1i64 } else { 0 })
            .collect::<Vec<_>>();
        assert_eq!(result, vec![1, 1, 1, 1, 1, 1, 1]);
    }

    #[test]
    fn select_wrong_type() {
        let dv = sample_merged_emp_table();
        println!("{}", dv);
        let result = dv.field::<i64, _>("EmpId");
        match result {
            Err(AgnesError::IncompatibleTypes { .. }) => {},
            Err(_) => { panic!["wrong error when calling field() with incorrect type"]; },
            Ok(_) => { panic!["expected error when calling field() with incorrect type, but \
                               received Ok"]; }
        }
    }
}
