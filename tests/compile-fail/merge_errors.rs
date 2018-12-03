#[macro_use] extern crate agnes;

use agnes::{
    label::LCons,
    cons::Nil,
    test_utils::*
};

fn main()
{
    let dv1 = sample_emp_table().into_view();
    let dv2 = sample_emp_table().into_view();

    println!("{}", dv1);
    println!("{}", dv2);

    let merged = dv1.merge(&dv2).unwrap();
    //~^ ERROR E0271
}
