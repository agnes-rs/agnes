use data_types::{DTypeList, DataType, MaxLen, TypeSelector};
use field::FieldIdent;
use select::Field;
use error;

/// Trait that provides a function for filtering a data structure's contents.
pub trait Filter<DTypes, T>: Field<DTypes>
    where T: 'static + DataType<DTypes>,
          DTypes: DTypeList,
          DTypes::Storage: MaxLen<DTypes> + TypeSelector<DTypes, T>
{
    /// Filter the contents of this data structure by applying the supplied predicate on the
    /// specified field.
    fn filter<I: Into<FieldIdent>, F: Fn(&T) -> bool>(&mut self, ident: I, pred: F)
        -> error::Result<Vec<usize>>;
}
