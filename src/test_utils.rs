use view::IntoFieldList;
use store::DataStore;
use view::DataView;
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
pub(crate) fn sample_merged_emp_table() -> DataView {
        let ds = sample_emp_table();
        let orig_dv: DataView = ds.into();
        orig_dv.merge(&sample_emp_table_extra().into()).unwrap()
}
pub(crate) trait MergedWithSample {
    fn merged_with_sample_emp_table(self, name: &str) -> DataView;
}
impl MergedWithSample for Vec<u64> {
    fn merged_with_sample_emp_table(self, name: &str) -> DataView {
        let orig_dv: DataView = sample_emp_table().into();
        orig_dv
            .merge(&DataStore::with_data(vec![(name, self.into())], None, None, None, None).into())
            .unwrap()
    }
}
impl MergedWithSample for Vec<i64> {
    fn merged_with_sample_emp_table(self, name: &str) -> DataView {
        let orig_dv: DataView = sample_emp_table().into();
        orig_dv
            .merge(&DataStore::with_data(None, vec![(name, self.into())], None, None, None).into())
            .unwrap()
    }
}
impl MergedWithSample for Vec<String> {
    fn merged_with_sample_emp_table(self, name: &str) -> DataView {
        let orig_dv: DataView = sample_emp_table().into();
        orig_dv
            .merge(&DataStore::with_data(None, None, vec![(name, self.into())], None, None).into())
            .unwrap()
    }
}
impl MergedWithSample for Vec<bool> {
    fn merged_with_sample_emp_table(self, name: &str) -> DataView {
        let orig_dv: DataView = sample_emp_table().into();
        orig_dv
            .merge(&DataStore::with_data(None, None, None, vec![(name, self.into())], None).into())
            .unwrap()
    }
}
impl MergedWithSample for Vec<f64> {
    fn merged_with_sample_emp_table(self, name: &str) -> DataView {
        let orig_dv: DataView = sample_emp_table().into();
        orig_dv
            .merge(&DataStore::with_data(None, None, None, None, vec![(name, self.into())]).into())
            .unwrap()
    }
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

macro_rules! impl_assert_vec_eq_and_pred {
    ($dtype:ty) => {

use view::DataView;
use apply::{Matches, MatchesAll};

#[allow(dead_code)]
pub(crate) fn assert_dv_eq_vec<'a, R>(left: &DataView, ident: &'a FieldIdent, mut right: Vec<R>)
    // where T: ApplyToField<FieldSelector<'a>> + Matches<FieldIndexSelector<'a>, $dtype>,
          where R: Into<$dtype>
{
    let right: Vec<$dtype> = right.drain(..).map(|r| r.into()).collect();
    for (i, rval) in (0..right.len()).zip(right) {
        assert!(left.matches(rval.clone(), ident, i).unwrap());
    }
}

#[allow(dead_code)]
pub(crate) fn assert_dv_pred<'a, F>(left: &DataView, ident: &'a FieldIdent, f: F)
    where F: Fn(&$dtype) -> bool
{
    assert!(left.matches_all(f, ident).unwrap());
}

    }
}

macro_rules! impl_assert_sorted_eq {
    ($dtype:ty) => {

use apply::SortOrderBy;

#[allow(dead_code)]
pub(crate) fn assert_dv_sorted_eq<'a, R>(left: &DataView, ident: &'a FieldIdent, mut right: Vec<R>)
    where //T: ApplyToField<FieldSelector<'a>> + Matches<FieldIndexSelector<'a>, $dtype>,
          R: Into<$dtype>
{
    let left_order = left.sort_order_by(ident).unwrap();
    println!("{:?}", left_order);
    let mut right: Vec<$dtype> = right.drain(..).map(|r| r.into()).collect();
    right.sort();

    for (lidx, rval) in left_order.iter().zip(right.iter()) {
        assert!(left.matches(rval.clone(), ident, *lidx).unwrap());
    }
}

    }
}

macro_rules! impl_test_helpers {
    ($name:tt; $dtype:ty) => {

pub(crate) mod $name {
    use field::FieldIdent;

    impl_assert_vec_eq_and_pred!($dtype);
    impl_assert_sorted_eq!($dtype);

}

    }
}

impl_test_helpers!(unsigned; u64);
impl_test_helpers!(signed;   i64);
impl_test_helpers!(text;     String);
impl_test_helpers!(boolean;  bool);

pub(crate) mod float {
    use field::FieldIdent;
    use apply::SortOrderBy;

    impl_assert_vec_eq_and_pred!(f64);

    #[allow(dead_code)]
    pub(crate) fn assert_dv_sorted_eq<'a, R>(left: &DataView, ident: &'a FieldIdent,
        mut right: Vec<R>)
        where //T: ApplyToField<FieldSelector<'a>> + Matches<FieldIndexSelector<'a>, f64>,
              R: Into<f64>
    {
        let left_order = left.sort_order_by(ident).unwrap();
        let mut right: Vec<f64> = right.drain(..).map(|r| r.into()).collect();
        right.sort_by(|a, b| a.partial_cmp(b).unwrap());

        for (lidx, rval) in left_order.iter().zip(right.iter()) {
            assert!(left.matches(rval.clone(), ident, *lidx).unwrap());
        }
    }

}

pub(crate) fn assert_field_lists_match<L: IntoFieldList, R: IntoFieldList>(left: L, right: R) {
    assert_eq!(left.into_field_list(), right.into_field_list());
}
