use access::{DataIndex};
use field::{Value};
use error::*;
use data_types::{DataType, DTypeList};

/// Provides a utility for matching a specific value. Returns `true` if the chosen element
/// matches the provided target value.
pub trait Matches<DTypes, T> {
    /// Returns `true` if the element at the specified index matches the target value.
    fn matches(&self, idx: usize, target: &T) -> Result<bool>;
}

impl<DTypes, T, U> Matches<DTypes, T> for U
    where DTypes: DTypeList,
          T: DataType<DTypes> + PartialEq<T>,
          U: DataIndex<DTypes, DType=T> {
    fn matches(&self, idx: usize, target: &T) -> Result<bool> {
        match self.get_datum(idx)? {
            Value::Exists(datum) => Ok(datum.eq(target)),
            Value::Na => Ok(false)
        }
    }
}
// impl<'a, DTypes, T, U> Matches<DTypes, Value<&'a T>> for U
//     where T: DataType<DTypes> + PartialEq<T>,
//           U: DataIndex<DTypes, DType=T>
// {
//     fn matches(&self, idx: usize, target: &Value<&'a T>) -> Result<bool> {
//         Ok(self.get_datum(idx)?.eq(target))
//     }
// }

// macro_rules! impl_matches {
//     ($t:ty, $($variant:tt)*) => {

// impl<'a, 'b> Matches<$t> for FieldData<'a> {
//     fn matches(&self, idx: usize, target: $t) -> Result<bool> {
//         Ok(match *self {
//             $($variant)*(ref datum) => {
//                 match datum.get_data(idx)? {
//                     Value::Exists(datum) => datum == target,
//                     Value::Na => false
//                 }
//             },
//             _ => false
//         })
//     }
// }
// impl<'a, 'b> Matches<Value<$t>> for FieldData<'a> {
//     fn matches(&self, idx: usize, target: Value<$t>) -> Result<bool> {
//         Ok(match *self {
//             $($variant)*(ref data) => data.get_data(idx)? == target,
//             _ => false
//         })
//     }
// }

// impl<'a, 'b, D> Matches<$t> for Selection<'a, D>
//     where Selection<'a, D>: GetFieldData<'a>
// {
//     fn matches(&self, idx: usize, target: $t) -> Result<bool> {
//         self.get_field_data().and_then(|fd| fd.matches(idx, target))
//     }
// }
// impl<'a, 'b, D> Matches<Value<$t>> for Selection<'a, D>
//     where Selection<'a, D>: GetFieldData<'a>
// {
//     fn matches(&self, idx: usize, target: Value<$t>) -> Result<bool> {
//         self.get_field_data().and_then(|fd| fd.matches(idx, target))
//     }
// }

//     }
// }

// impl_matches![&'b u64, FieldData::Unsigned];
// impl_matches![&'b i64, FieldData::Signed];
// impl_matches![&'b String, FieldData::Text];
// impl_matches![&'b bool, FieldData::Boolean];
// impl_matches![&'b f64, FieldData::Float];


/// Trait for finding an index set of values in a field that match a predicate. Returns a vector of
// indices of all elements in the field that pass the predicate.
pub trait DataFilter<'a, DTypes, T> {
    /// Returns vector of indices of all elements in this data structure specified that pass the
    /// predicate.
    fn data_filter<F: Fn(&T) -> bool>(&'a self, f: F) -> Vec<usize>;
}


impl<'a, DTypes, T, U> DataFilter<'a, DTypes, T> for U
    where DTypes: DTypeList,
          T: DataType<DTypes>,
          U: DataIndex<DTypes, DType=T>
{
    fn data_filter<F: Fn(&T) -> bool>(&'a self, f: F) -> Vec<usize> {
        let mut result = vec![];
        for (idx, datum) in self.iter().enumerate() {
            if let Value::Exists(ref val) = datum {
                if (f)(val) {
                    result.push(idx)
                }
            }
        }
        result
    }
}

// impl<'a, T: 'a + DataType> DataFilter<'a, T> for FieldData<'a> where FieldData<'a>: DIter<'a, T> {
//     fn data_filter<F: Fn(&T) -> bool>(&'a self, f: F) -> Vec<usize> {
//         let mut result = vec![];
//         for (idx, datum) in self.data_iter().enumerate() {
            // if let Value::Exists(ref val) = datum {
            //     if (f)(val) {
            //         result.push(idx)
            //     }
            // }
//         }
//         result
//     }
// }

#[cfg(test)]
mod tests {
    use super::*;
    use test_utils::*;
    use select::Field;
    use field::FieldIdent;

    use data_types::standard::*;

    #[test]
    fn matches() {
        let view: DataView = sample_emp_table().into();

        assert_eq!(view.field::<u64, _>("EmpId").unwrap().matches(1, &2u64).unwrap(), true);
        assert_eq!(view.field::<u64, _>("EmpId").unwrap().matches(1, &3u64).unwrap(), false);

        match view.field::<u64, _>("EmpId").unwrap().matches(9, &2u64).unwrap_err() {
            AgnesError::IndexError { index, len } => {
                assert_eq!(index, 9);
                assert_eq!(len, view.nrows());
            },
            e => panic!("expected IndexError, received {:?}", e)
        }

        match view.field::<u64, _>("Foo") {
            Ok(_) => panic!("expected FieldNotFound error, but succeeded"),
            Err(e) => match e {
                AgnesError::FieldNotFound(ident) => {
                    assert_eq!(ident, FieldIdent::Name("Foo".to_string()));
                },
                e => panic!("expected FieldNotFound, received {:?}", e)
            }
        }

        assert_eq!(view.field::<String, _>("EmpName").unwrap().matches(1, &"Jamie".to_string())
            .unwrap(), true);
        assert_eq!(view.field("EmpName").unwrap().matches(1, &"Jamie".to_string()).unwrap(), true);
        assert_eq!(view.field::<String, _>("EmpName").unwrap().matches(1, &"Sally".to_string())
            .unwrap(), false);
        assert_eq!(view.field("EmpName").unwrap().matches(1, &"Sally".to_string()).unwrap(), false);

        let view: DataView = sample_emp_table_extra().into();

        assert_eq!(view.field::<i64, _>("SalaryOffset").unwrap().matches(1, &4i64).unwrap(), true);
        assert_eq!(view.field("SalaryOffset").unwrap().matches(1, &4i64).unwrap(), true);
        assert_eq!(view.field::<i64, _>("SalaryOffset").unwrap().matches(1, &-3i64).unwrap(),
            false);
        assert_eq!(view.field("SalaryOffset").unwrap().matches(1, &-3i64).unwrap(), false);


        assert_eq!(view.field::<bool, _>("DidTraining").unwrap().matches(1, &false).unwrap(), true);
        assert_eq!(view.field("DidTraining").unwrap().matches(1, &false).unwrap(), true);
        assert_eq!(view.field::<bool, _>("DidTraining").unwrap().matches(1, &true).unwrap(), false);
        assert_eq!(view.field("DidTraining").unwrap().matches(1, &true).unwrap(), false);

        assert_eq!(view.field::<f64, _>("VacationHrs").unwrap().matches(1, &54.1).unwrap(), true);
        assert_eq!(view.field("VacationHrs").unwrap().matches(1, &54.1).unwrap(), true);
        assert_eq!(view.field::<f64, _>("VacationHrs").unwrap().matches(1, &47.3).unwrap(), false);
        assert_eq!(view.field("VacationHrs").unwrap().matches(1, &47.3).unwrap(), false);
    }
}
