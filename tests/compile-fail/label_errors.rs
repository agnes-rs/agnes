#[macro_use] extern crate agnes;

use agnes::{
    label::LCons,
    cons::Nil,
    test_utils::*
};

fn main()
{
    let dv = sample_emp_table().into_view();
    assert_eq!(dv.fieldnames(), vec!["EmpId", "DeptId", "EmpName"]);
    assert_eq!(dv.nrows(), 7);
    assert_eq!(dv.nfields(), 3);

    use emp_table::*;
    let subdv1 = dv.v::<Labels![EmployeeName]>();
    //~^ ERROR cannot find type `EmployeeName` in this scope
}
