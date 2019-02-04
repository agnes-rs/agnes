use cons::Nil;
use field::FieldData;
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
            .push_back_field($empids)
            .push_back_field($deptids)
            .push_back_field($names)
    }};
}

macro_rules! emp_table {
    ($empids:expr, $deptids:expr, $names:expr) => {{
        emp_table_from_field![
            $empids.into(),
            $deptids.into(),
            $names
                .iter()
                .map(|s| s.to_string())
                .collect::<Vec<_>>()
                .into()
        ]
    }};
}

macro_rules! sample_emp_table {
    () => {{
        emp_table![
            vec![0u64, 2, 5, 6, 8, 9, 10],
            vec![1u64, 2, 1, 1, 3, 4, 4],
            vec!["Sally", "Jamie", "Bob", "Cara", "Louis", "Louise", "Ann"]
        ]
    }};
}

pub fn sample_emp_table() -> emp_table::Store {
    sample_emp_table![]
}

namespace![
    pub namespace extra_emp: emp_table {
        field SalaryOffset: i64;
        field DidTraining: bool;
        field VacationHrs: f32;
    }
];

pub fn sample_emp_table_extra() -> extra_emp::Store {
    DataStore::<Nil>::empty()
        .push_back_cloned_from_iter(&[-5i64, 4, 12, -33, 10, 0, -1])
        .push_back_cloned_from_iter(&[false, false, true, true, true, false, true])
        .push_back_cloned_from_iter(&[47.3, 54.1, 98.3, 12.2, -1.2, 5.4, 22.5])
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

pub fn sample_emp_table_full() -> full_emp_table::Store {
    DataStore::<Nil>::empty()
        .push_back_cloned_from_iter(&[0u64, 2, 5, 6, 8, 9, 10])
        .push_back_cloned_from_iter(&[1u64, 2, 1, 1, 3, 4, 4])
        .push_back_from_iter(
            ["Sally", "Jamie", "Bob", "Cara", "Louis", "Louise", "Ann"]
                .iter()
                .map(|&s| s.to_string()),
        )
        .push_back_cloned_from_iter(&[-5i64, 4, 12, -33, 10, 0, -1])
        .push_back_cloned_from_iter(&[false, false, true, true, true, false, true])
        .push_back_cloned_from_iter(&[47.3, 54.1, 98.3, 12.2, -1.2, 5.4, 22.5])
}

namespace![
    pub namespace dept_table: full_emp_table {
        field DeptId: u64;
        field DeptName: String;
    }
];

pub fn sample_dept_table() -> dept_table::Store {
    dept_table(
        vec![1u64, 2, 3, 4],
        vec!["Marketing", "Sales", "Manufacturing", "R&D"],
    )
}

pub fn dept_table(deptids: Vec<u64>, names: Vec<&str>) -> dept_table::Store {
    dept_table_from_field(
        deptids.into(),
        names
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>()
            .into(),
    )
}

pub fn dept_table_from_field(
    deptids: FieldData<u64>,
    names: FieldData<String>,
) -> dept_table::Store {
    dept_table::Store::empty()
        .push_back_field(deptids)
        .push_back_field(names)
}

pub fn sample_merged_emp_table() -> <emp_table::View as ViewMerge<extra_emp::View>>::Output {
    sample_emp_table()
        .into_view()
        .merge(&sample_emp_table_extra().into_view())
        .unwrap()
}
