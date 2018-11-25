#[macro_use] extern crate agnes;

use agnes::{
    label::LCons,
    cons::Nil,
    test_utils::*
};

fn main()
{
    let ds = sample_emp_table();
    let dv = ds.into_view();
    assert_eq!(dv.fieldnames(), vec!["EmpId", "DeptId", "EmpName"]);
    assert_eq!(dv.nrows(), 7);
    assert_eq!(dv.nfields(), 3);

    let subdv1 = dv.v::<Labels![EmployeeName]>();
    //~^ ERROR Use of undeclared type or module `EmployeeName`
}
