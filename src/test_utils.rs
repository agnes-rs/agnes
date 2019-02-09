/*!
Functions for generating sample data tables for tests.
*/
use cons::Nil;
use field::FieldData;
use store::DataStore;
use view::ViewMerge;

namespace![
    pub table emp_table {
        EmpId: u64,
        DeptId: u64,
        EmpName: String
    }
    pub table extra_emp {
        SalaryOffset: i64,
        DidTraining: bool,
        VacationHrs: f32,
    }
    pub table full_emp_table {
        EmpId: u64,
        DeptId: u64,
        EmpName: String,
        SalaryOffset: i64,
        DidTraining: bool,
        VacationHrs: f32,
    }
    pub table dept_table {
        DeptId: u64,
        DeptName: String,
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

/// Generates basic sample employee table (with `EmpId`, `DeptId`, `EmpName`)
pub fn sample_emp_table() -> emp_table::Store {
    sample_emp_table![]
}

/// Generates 'extra' data fields (`SalaryOffset`, `DidTraining`, `VacationHrs`) for employee table
/// (for use in merge tests).
pub fn sample_emp_table_extra() -> extra_emp::Store {
    DataStore::<Nil>::empty()
        .push_back_cloned_from_iter(&[-5i64, 4, 12, -33, 10, 0, -1])
        .push_back_cloned_from_iter(&[false, false, true, true, true, false, true])
        .push_back_cloned_from_iter(&[47.3, 54.1, 98.3, 12.2, -1.2, 5.4, 22.5])
}

/// Generates 'full' employee table with all the fields from the basic employee table (`EmpId`,
/// `DeptId`, `EmpName`) as well as the 'extra' fields (`SalaryOffset`, `DidTraining`,
/// `VacationHrs`)
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

/// Generates 'full' employee table merged from the basic employee table generated by
/// [sample_emp_table](func.sample_emp_table.html) and the `extra` employee table generated
/// by [sample_emp_table_extra](func.sample_emp_table_extra.html).
pub fn sample_merged_emp_table() -> <emp_table::View as ViewMerge<extra_emp::View>>::Output {
    sample_emp_table()
        .into_view()
        .merge(&sample_emp_table_extra().into_view())
        .unwrap()
}

/// Generates sample department table (with fields `DeptId`, `DeptName`).
pub fn sample_dept_table() -> dept_table::Store {
    dept_table(
        vec![1u64, 2, 3, 4],
        vec!["Marketing", "Sales", "Manufacturing", "R&D"],
    )
}

/// Generates a department table with provided `DeptId`s and `DeptName`s in `Vec`s.
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

/// Generates a department table with provided `DeptId`s and `DeptName`s in
/// [FieldData](../field/struct.FieldData.html) structs.
pub fn dept_table_from_field(
    deptids: FieldData<u64>,
    names: FieldData<String>,
) -> dept_table::Store {
    dept_table::Store::empty()
        .push_back_field(deptids)
        .push_back_field(names)
}
