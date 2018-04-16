use view::IntoFieldList;
use store::DataStore;
use masked::MaskedData;

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
pub(crate) fn sample_emp_table_extra() -> DataStore {
    DataStore::with_data(
        None,
        None,
        None,
        vec![
            ("DidTraining", vec![false, false, true, true, true, false, true].into())
        ],
        vec![
            ("VacationHrs", vec![47.3, 54.1, 98.3, 12.2, -1.2, 5.4, 22.5].into()),
        ]
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
    ($name:tt; $dtype:ty) => {

pub(crate) mod $name {
    use apply::*;
    use field::FieldIdent;

    #[allow(dead_code)]
    pub(crate) fn assert_vec_eq<'a, T, R>(left: &T, ident: &'a FieldIdent, mut right: Vec<R>)
        where T: ApplyToField<FieldSelector<'a>> + Matches<FieldIndexSelector<'a>, $dtype>,
              R: Into<$dtype>
    {
        let right: Vec<$dtype> = right.drain(..).map(|r| r.into()).collect();
        for (i, rval) in (0..right.len()).zip(right) {
            assert!(left.matches(FieldIndexSelector(ident, i), rval.clone()).unwrap());
        }
    }

    #[allow(dead_code)]
    pub(crate) fn assert_sorted_eq<'a, T, R>(left: &T, ident: &'a FieldIdent, mut right: Vec<R>)
        where T: ApplyToField<FieldSelector<'a>> + Matches<FieldIndexSelector<'a>, $dtype>,
              R: Into<$dtype>
    {
        let left_order = left.sort_order_by(FieldSelector(ident)).unwrap();
        let mut right: Vec<$dtype> = right.drain(..).map(|r| r.into()).collect();
        right.sort();

        for (lidx, rval) in left_order.iter().zip(right.iter()) {
            assert!(left.matches(FieldIndexSelector(ident, *lidx), rval.clone()).unwrap());
        }
    }

    #[allow(dead_code)]
    pub(crate) fn assert_pred<'a, T, F>(left: &T, field: &'a FieldIdent, f: F)
        where T: MatchesAll<FieldSelector<'a>, $dtype>, F: Fn(&$dtype) -> bool
    {
        assert!(left.matches_all(FieldSelector(field), f).unwrap());
    }
}

    }
}

impl_test_helpers!(unsigned; u64);
impl_test_helpers!(text;     String);

pub(crate) fn assert_field_lists_match<L: IntoFieldList, R: IntoFieldList>(left: L, right: R) {
    assert_eq!(left.into_field_list(), right.into_field_list());
}
