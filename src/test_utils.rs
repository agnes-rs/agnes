use store::DataStore;
use masked::{FieldData, MaskedData, MaybeNa};

pub(crate) fn sample_emp_table() -> DataStore {
    emp_table(vec![0u64, 2, 5, 6, 8, 9, 10], vec![1u64, 2, 1, 1, 3, 4, 4],
        vec!["Sally", "Jamie", "Bob", "Cara", "Louis", "Louise", "Ann"])
}
pub(crate) fn emp_table(empids: Vec<u64>, deptids: Vec<u64>, names: Vec<&str>) -> DataStore {
    emp_table_from_masked(empids.into(), deptids.into(), names.into())
}
pub(crate) fn emp_table_from_masked(empids: MaskedData<u64>, deptids: MaskedData<u64>,
    names: MaskedData<String>) -> DataStore
{
    DataStore::with_data(
        // unsigned
        vec![
            ("EmpId", empids),
            ("DeptId", deptids)
        ],
        // signed
        None,
        // text
        vec![
            ("EmpName", names)
        ],
        // boolean
        None,
        // float
        None
    )
}

pub(crate) fn sample_dept_table() -> DataStore {
    dept_table(vec![1u64, 2, 3, 4], vec!["Marketing", "Sales", "Manufacturing", "R&D"])
}
pub(crate) fn dept_table(deptids: Vec<u64>, names: Vec<&str>) -> DataStore {
    dept_table_from_masked(deptids.into(), names.into())
}
pub(crate) fn dept_table_from_masked(deptids: MaskedData<u64>, names: MaskedData<String>)
    -> DataStore
{
    DataStore::with_data(
        // unsigned
        vec![
            ("DeptId", deptids)
        ],
        // signed
        None,
        // text
        vec![
            ("DeptName", names)
        ],
        // boolean
        None,
        // float
        None
    )
}

macro_rules! impl_test_helpers {
    ($name:tt; $variant:path, $dtype:ty) => {
        pub(crate) mod $name {
            use super::{FieldData, MaybeNa};
            #[allow(dead_code)]
            pub(crate) fn assert_sorted_eq(left: FieldData, right: Vec<$dtype>) {
                if let $variant(masked) = left {
                    let mut masked = masked.as_vec();
                    masked.sort();
                    let mut right = right.iter()
                        .map(|val| MaybeNa::Exists(val)).collect::<Vec<_>>();
                    right.sort();
                    for (lval, rval) in masked.iter().zip(right.iter()) {
                        assert_eq!(lval, rval);
                    }
                } else {
                    panic!("$name::assert_sorted_eq called with incorrect type FieldData")
                }
            }
            #[allow(dead_code)]
            pub(crate) fn assert_pred<F: Fn(&$dtype) -> bool>(left: FieldData, f: F) {
                if let $variant(masked) = left {
                    for val in masked.as_vec().iter() {
                        match val {
                            &MaybeNa::Exists(&ref val) => {
                                assert!(f(val), "predicate failed");
                            },
                            &MaybeNa::Na => {
                                panic!("$name::assert_pred called with NA value");
                            }
                        }
                    };
                } else {
                    panic!("$name::assert_pred called with incorrect type FieldData")
                }
            }
        }
    }
}
impl_test_helpers!(unsigned; FieldData::Unsigned, u64);
impl_test_helpers!(text;     FieldData::Text,     String);
