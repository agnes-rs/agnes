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

pub trait Field<DTypes>
    where DTypes: DTypeList
{
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

pub trait SelectField<'a, T, DTypes>
    where DTypes: DTypeList,
          T: 'a + DataType<DTypes>
{
    type Output: DataIndex<DTypes, DType=T>;

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

    #[test]
    fn select() {
        let dv = sample_merged_emp_table();
        println!("{}", dv);
        let result = dv.field("EmpId").unwrap().iter()
            .map(|datum: Value<&u64>| if datum.exists() { 1i64 } else { 0 })
            .collect::<Vec<_>>();
        assert_eq!(result, vec![1, 1, 1, 1, 1, 1, 1]);
    }
}
