// use view::IntoFieldList;
// use store::{WithClonedDataFromIter};
use typenum::Add1;

use field::FieldData;
use access::DataIndex;
use fieldlist::FieldCons;
use cons::Nil;
use field::Value;
use store::DataStore;
use view::ViewMerge;

namespace![
    pub namespace emp_table {
        field EmpId: u64;
        field DeptId: u64;
        field EmpName: String;
    }
];

macro_rules! emp_table_from_field {
    ($empids:expr, $deptids:expr, $names:expr) => {{
        $crate::store::DataStore::<$crate::cons::Nil>::empty()
            .add_field($empids)
            .add_field($deptids)
            .add_field($names)
    }}
}
macro_rules! emp_table {
    ($empids:expr, $deptids:expr, $names:expr) => {{
        emp_table_from_field![$empids.into(), $deptids.into(), $names.into()]
    }}
}
macro_rules! sample_emp_table {
    () => {{
        emp_table![
            vec![0u64, 2, 5, 6, 8, 9, 10],
            vec![1u64, 2, 1, 1, 3, 4, 4],
            vec!["Sally", "Jamie", "Bob", "Cara", "Louis", "Louise", "Ann"]
        ]
    }}
}
pub fn sample_emp_table() -> emp_table::Store
{
    sample_emp_table![]
}

namespace![
    pub namespace extra_emp: emp_table {
        field SalaryOffset: i64;
        field DidTraining: bool;
        field VacationHrs: f32;
    }
];

pub fn sample_emp_table_extra()
    -> extra_emp::Store
{
    DataStore::<Nil>::empty()
        .add_cloned_field_from_iter(&[-5i64, 4, 12, -33, 10, 0, -1])
        .add_cloned_field_from_iter(&[false, false, true, true, true, false, true])
        .add_cloned_field_from_iter(&[47.3, 54.1, 98.3, 12.2, -1.2, 5.4, 22.5])
}



namespace![
    pub namespace full_emp_table: extra_emp {
        field EmpId: u64;
        field DeptId: u64;
        field EmpName: String;
        field SalaryOffset: i64;
        field DidTraining: bool;
        field VacationHrs: f32;
    }
];

pub fn sample_emp_table_full()
    -> full_emp_table::Store
{
    DataStore::<Nil>::empty()
        .add_cloned_field_from_iter(&[0u64, 2, 5, 6, 8, 9, 10])
        .add_cloned_field_from_iter(&[1u64, 2, 1, 1, 3, 4, 4])
        .add_field_from_iter(
            ["Sally", "Jamie", "Bob", "Cara", "Louis", "Louise", "Ann"].iter()
            .map(|&s| s.to_string())
        )
        .add_cloned_field_from_iter(&[-5i64, 4, 12, -33, 10, 0, -1])
        .add_cloned_field_from_iter(&[false, false, true, true, true, false, true])
        .add_cloned_field_from_iter(&[47.3, 54.1, 98.3, 12.2, -1.2, 5.4, 22.5])
}

namespace![
    pub namespace dept_table: full_emp_table {
        field DeptId: u64;
        field DeptName: String;
    }
];

pub fn sample_dept_table()
    -> dept_table::Store
{
    dept_table(vec![1u64, 2, 3, 4], vec!["Marketing", "Sales", "Manufacturing", "R&D"])
}
pub fn dept_table(
    deptids: Vec<u64>, names: Vec<&str>
)
    -> dept_table::Store
{
    dept_table_from_field(deptids.into(), names.into())
}
pub fn dept_table_from_field(
    deptids: FieldData<u64>, names: FieldData<String>
)
    -> dept_table::Store
{
    dept_table::Store::empty()
        .add_field(deptids)
        .add_field(names)
}

pub fn sample_merged_emp_table() -> <emp_table::View as ViewMerge<extra_emp::View>>::Output
{
    sample_emp_table().into_view().merge(&sample_emp_table_extra().into_view()).unwrap()
}
// pub fn sample_merged_emp_table() -> dt_std::DataView {
    // let ds = sample_emp_table();
    // let orig_dv: dt_std::DataView = ds.into();
    // orig_dv.merge(&sample_emp_table_extra().into()).unwrap()
// }
// pub trait MergedWithSample {
//     fn merged_with_sample_emp_table(self, name: &str) -> dt_std::DataView;
// }
// impl MergedWithSample for Vec<u64> {
//     fn merged_with_sample_emp_table(self, name: &str) -> dt_std::DataView {
//         let orig_dv: dt_std::DataView = sample_emp_table().into();
//         orig_dv
//             .merge(&dt_std::DataStore::empty().with_data_vec(name, self).unwrap().into())
//             .unwrap()
//     }
// }
// impl MergedWithSample for Vec<i64> {
//     fn merged_with_sample_emp_table(self, name: &str) -> dt_std::DataView {
//         let orig_dv: dt_std::DataView = sample_emp_table().into();
//         orig_dv
//             .merge(&dt_std::DataStore::empty().with_data_vec(name, self).unwrap().into())
//             .unwrap()
//     }
// }
// impl MergedWithSample for Vec<String> {
//     fn merged_with_sample_emp_table(self, name: &str) -> dt_std::DataView {
//         let orig_dv: dt_std::DataView = sample_emp_table().into();
//         orig_dv
//             .merge(&dt_std::DataStore::empty().with_data_vec(name, self).unwrap().into())
//             .unwrap()
//     }
// }
// impl MergedWithSample for Vec<bool> {
//     fn merged_with_sample_emp_table(self, name: &str) -> dt_std::DataView {
//         let orig_dv: dt_std::DataView = sample_emp_table().into();
//         orig_dv
//             .merge(&dt_std::DataStore::empty().with_data_vec(name, self).unwrap().into())
//             .unwrap()
//     }
// }
// impl MergedWithSample for Vec<f64> {
//     fn merged_with_sample_emp_table(self, name: &str) -> dt_std::DataView {
//         let orig_dv: dt_std::DataView = sample_emp_table().into();
//         orig_dv
//             .merge(&dt_std::DataStore::empty().with_data_vec(name, self).unwrap().into())
//             .unwrap()
//     }
// }

// pub fn sample_dept_table() -> dt_std::DataStore
// {
//     dept_table(vec![1u64, 2, 3, 4], vec!["Marketing", "Sales", "Manufacturing", "R&D"])
// }
// pub fn dept_table(
//     deptids: Vec<u64>, names: Vec<&str>
// )
//     -> dt_std::DataStore
// {
//     dept_table_from_field(deptids.into(), names.into())
// }
// pub fn dept_table_from_field(
//     deptids: FieldData<dt_std::Types, u64>, names: FieldData<dt_std::Types, String>
// )
//     -> dt_std::DataStore
// {
//     dt_std::DataStore::empty()
//         .with_cloned_data_from_iter("DeptId", deptids.iter()).unwrap()
//         .with_cloned_data_from_iter("DeptName", names.iter()).unwrap()
// }

// macro_rules! impl_assert_vec_eq_and_pred {
//     ($dtype:ty) => {

// use select::FSelect;
// use filter::Matches;
// use field::Value;
// use access::DataIndex;
// use view::DataView;
// use fieldlist::FSelector;
// // use data_types::{DataType, DTypeList, MaxLen, TypeSelector};

// #[allow(dead_code)]
// pub fn assert_dv_eq_vec<'a, Fields, Ident, FIdx, R>(
//     left: &DataView<Fields>, mut right: Vec<R>
// )
//     where R: Into<$dtype>,
//           Fields: FSelector<Ident, FIdx, DType=$dtype>
// {
//     let right: Vec<$dtype> = right.drain(..).map(|r| r.into()).collect();
//     for (i, rval) in (0..right.len()).zip(right) {
//         assert!(left.field::<Ident, FIdx>().unwrap().matches(i, &rval).unwrap());
//     }
// }

// #[allow(dead_code)]
// pub fn assert_dv_pred<'a, Fields, Ident, FIdx, F>(
//     left: &DataView<Fields>, mut f: F
// )
//     where F: FnMut(&$dtype) -> bool,
//           Fields: FSelector<Ident, FIdx, DType=$dtype>
// {
//     assert!(left.field::<Ident, FIdx>().unwrap().iter().all(|val| {
//         match val {
//             Value::Exists(val) => f(val),
//             Value::Na => false
//         }
//     }));
// }

//     }
// }

// macro_rules! impl_assert_sorted_eq {
//     ($dtype:ty) => {

// use apply::sort::sort_order;

// #[allow(dead_code)]
// pub fn assert_dv_sorted_eq<'a, Fields, Ident, FIdx, R>(
//     left: &DataView<Fields>, mut right: Vec<R>
// )
//     where R: Into<$dtype>,
//           Fields: FSelector<Ident, FIdx, DType=$dtype>
// {
//     let left_order = sort_order(&left.field::<Ident, FIdx>().unwrap());
//     let mut right: Vec<$dtype> = right.drain(..).map(|r| r.into()).collect();
//     right.sort();

//     for (lidx, rval) in left_order.iter().zip(right.iter()) {
//         assert!(left.field::<Ident, FIdx>().unwrap().matches(*lidx, &rval).unwrap());
//     }
// }

//     }
// }

// macro_rules! impl_test_helpers {
//     ($name:tt; $dtype:ty) => {

// pub mod $name {

//     impl_assert_vec_eq_and_pred!($dtype);
//     impl_assert_sorted_eq!($dtype);

// }

//     }
// }

// impl_test_helpers!(unsigned; u64);
// impl_test_helpers!(signed;   i64);
// impl_test_helpers!(text;     String);
// impl_test_helpers!(boolean;  bool);

// pub mod float {
//     use field::FieldIdent;
//     use apply::sort::sort_order;

//     impl_assert_vec_eq_and_pred!(f64);

//     #[allow(dead_code)]
//     pub fn assert_dv_sorted_eq<'a, Fields, Ident, FIdx, R>(
//         left: &DataView<Fields>, ident: &'a FieldIdent, mut right: Vec<R>
//     )
//         where R: Into<f64>,
//               Fields: FSelector<Ident, FIdx, DType=f64>
//     {
//         let left_order = sort_order(&left.field::<f64, _>(ident).unwrap());
//         let mut right: Vec<f64> = right.drain(..).map(|r| r.into()).collect();
//         right.sort_by(|a, b| a.partial_cmp(b).unwrap());

//         for (lidx, rval) in left_order.iter().zip(right.iter()) {
//             assert!(left.field(ident).unwrap().matches(*lidx, &rval).unwrap());
//         }
//     }

// }

// pub fn assert_field_lists_match<L: IntoFieldList, R: IntoFieldList>(left: L, right: R) {
//     assert_eq!(left.into_field_list(), right.into_field_list());
// }
