#[macro_use]
extern crate agnes;

tablespace![
    pub table employee {
        EmpId: u64,
        DeptId: u64,
        EmpName: String,
    }
];

fn main() {
    let emp_table = table![
        employee::EmpId = [0, 1, 2, 3, 4, 5, 6];
        employee::DeptId = [0, 2, 1, 1, 1, 0, 1];
        employee::EmpName =
            ["Astrid", "Bob", "Calvin", "Deborah", "Eliza", "Franklin", "Gunther"];
    ];
    assert_eq!((emp_table.nrows(), emp_table.nfields()), (7, 3));
    println!("{}", emp_table);
}
