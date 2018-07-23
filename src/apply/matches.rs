use apply::{Selection, GetFieldData};
use access::{FieldData, DataIndex, DIter};
use masked::{MaybeNa};
use error::*;
use field::DataType;

/// Provides a utility for matching a specific value. Returns `true` if the chosen element
/// matches the provided target value.
pub trait Matches<T> {
    /// Returns `true` if the element at the specified index matches the target value.
    fn matches(&self, idx: usize, target: T) -> Result<bool>;
}

macro_rules! impl_matches {
    ($t:ty, $($variant:tt)*) => {

impl<'a, 'b> Matches<$t> for FieldData<'a> {
    fn matches(&self, idx: usize, target: $t) -> Result<bool> {
        Ok(match *self {
            $($variant)*(ref datum) => {
                match datum.get_data(idx)? {
                    MaybeNa::Exists(datum) => datum == target,
                    MaybeNa::Na => false
                }
            },
            _ => false
        })
    }
}
impl<'a, 'b> Matches<MaybeNa<$t>> for FieldData<'a> {
    fn matches(&self, idx: usize, target: MaybeNa<$t>) -> Result<bool> {
        Ok(match *self {
            $($variant)*(ref data) => data.get_data(idx)? == target,
            _ => false
        })
    }
}

impl<'a, 'b, D> Matches<$t> for Selection<'a, D>
    where Selection<'a, D>: GetFieldData<'a>
{
    fn matches(&self, idx: usize, target: $t) -> Result<bool> {
        self.get_field_data().and_then(|fd| fd.matches(idx, target))
    }
}
impl<'a, 'b, D> Matches<MaybeNa<$t>> for Selection<'a, D>
    where Selection<'a, D>: GetFieldData<'a>
{
    fn matches(&self, idx: usize, target: MaybeNa<$t>) -> Result<bool> {
        self.get_field_data().and_then(|fd| fd.matches(idx, target))
    }
}

    }
}

impl_matches![&'b u64, FieldData::Unsigned];
impl_matches![&'b i64, FieldData::Signed];
impl_matches![&'b String, FieldData::Text];
impl_matches![&'b bool, FieldData::Boolean];
impl_matches![&'b f64, FieldData::Float];


/// Trait for finding an index set of values in a field that match a predicate. Returns a vector of
// indices of all elements in the field that pass the predicate.
pub trait DataFilter<'a, T> {
    /// Returns vector of indices of all elements in this data structure specified that pass the
    /// predicate.
    fn data_filter<F: Fn(&T) -> bool>(&'a self, f: F) -> Vec<usize>;
}

impl<'a, T: 'a + DataType> DataFilter<'a, T> for FieldData<'a> where FieldData<'a>: DIter<'a, T> {
    fn data_filter<F: Fn(&T) -> bool>(&'a self, f: F) -> Vec<usize> {
        let mut result = vec![];
        for (idx, datum) in self.data_iter().enumerate() {
            if let MaybeNa::Exists(ref val) = datum {
                if (f)(val) {
                    result.push(idx)
                }
            }
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use view::DataView;
    use super::*;
    use test_utils::*;
    use apply::{Select, Field};
    use field::FieldIdent;

    #[test]
    fn matches() {
        let view: DataView = sample_merged_emp_table().into();

        for x in view.field("EmpId").unwrap().data_iter::<u64>() {
            println!("{:?}", x);
        }

        assert_eq!(view.select_one("EmpId").matches(1, &2u64).unwrap(), true);
        assert_eq!(view.field("EmpId").unwrap().matches(1, &2u64).unwrap(), true);
        assert_eq!(view.select_one("EmpId").matches(1, &3u64).unwrap(), false);
        assert_eq!(view.field("EmpId").unwrap().matches(1, &3u64).unwrap(), false);

        match view.select_one("EmpId").matches(9, &2u64).unwrap_err() {
            AgnesError::IndexError { index, len } => {
                assert_eq!(index, 9);
                assert_eq!(len, view.nrows());
            },
            e => panic!("expected IndexError, received {:?}", e)
        }

        match view.select_one("Foo").matches(9, &2u64).unwrap_err() {
            AgnesError::FieldNotFound(ident) => {
                assert_eq!(ident, FieldIdent::Name("Foo".to_string()));
            },
            e => panic!("expected FieldNotFound, received {:?}", e)
        }

        assert_eq!(view.select_one("SalaryOffset").matches(1, &4i64).unwrap(), true);
        assert_eq!(view.field("SalaryOffset").unwrap().matches(1, &4i64).unwrap(), true);
        assert_eq!(view.select_one("SalaryOffset").matches(1, &-3i64).unwrap(), false);
        assert_eq!(view.field("SalaryOffset").unwrap().matches(1, &-3i64).unwrap(), false);

        assert_eq!(view.select_one("EmpName").matches(1, &"Jamie".to_string()).unwrap(), true);
        assert_eq!(view.field("EmpName").unwrap().matches(1, &"Jamie".to_string()).unwrap(), true);
        assert_eq!(view.select_one("EmpName").matches(1, &"Sally".to_string()).unwrap(), false);
        assert_eq!(view.field("EmpName").unwrap().matches(1, &"Sally".to_string()).unwrap(), false);

        assert_eq!(view.select_one("DidTraining").matches(1, &false).unwrap(), true);
        assert_eq!(view.field("DidTraining").unwrap().matches(1, &false).unwrap(), true);
        assert_eq!(view.select_one("DidTraining").matches(1, &true).unwrap(), false);
        assert_eq!(view.field("DidTraining").unwrap().matches(1, &true).unwrap(), false);

        assert_eq!(view.select_one("VacationHrs").matches(1, &54.1).unwrap(), true);
        assert_eq!(view.field("VacationHrs").unwrap().matches(1, &54.1).unwrap(), true);
        assert_eq!(view.select_one("VacationHrs").matches(1, &47.3).unwrap(), false);
        assert_eq!(view.field("VacationHrs").unwrap().matches(1, &47.3).unwrap(), false);
    }
}
