#[macro_use]
extern crate agnes;

tablespace![
    table salary {
        EmpId: u64,
        Year2010: f64,
        Year2011: f64,
        Year2012: f64,
        Year2013: f64,
        Year2014: f64,
        SalaryYear: String,
        Salary: f64,
        TotalYearlySalary: f64,
    }
];

use salary::*;

fn main() {
    // create table directly from some data
    let orig_table = table![
        EmpId = [0u64, 1u64, 2u64];
        Year2010 = [1500.0, 900.0, 600.0];
        Year2011 = [1600.0, 920.0, 800.0];
        Year2012 = [1700.0, 940.0, 900.0];
        Year2013 = [1850.0, 940.0, 1020.0];
        Year2014 = [2000.0, 970.0, 1100.0];
    ];

    // quick check to make sure we loaded the right table: with 3 rows, 6 fields
    assert_eq!((orig_table.nrows(), orig_table.nfields()), (3, 6));
    println!("Original table:\n\n{}", orig_table);

    let melted_table = orig_table
        .melt::<Labels![Year2010, Year2011, Year2012, Year2013, Year2014], SalaryYear, Salary, _>();

    // melted table should have 15 rows -- 5 for each of our 3 employees -- and 3 fields
    assert_eq!((melted_table.nrows(), melted_table.nfields()), (15, 3));
    assert_eq!(
        melted_table.fieldnames(),
        vec!["EmpId", "SalaryYear", "Salary"]
    );
    println!("Melted table:\n\n{}", melted_table);

    // compute the total salary per year, aggregated over employees
    let agg_table = melted_table
        .aggregate::<Labels![SalaryYear], Salary, TotalYearlySalary, _, _, _>(0.0, |accum, val| {
            *accum = *accum + val.unwrap_or(&0.0);
        });

    // we're left with five rows (one for each year of data), and two columns (year name and sum)
    assert_eq!((agg_table.nrows(), agg_table.nfields()), (5, 2));
    println!("Aggregated table:\n\n{}", agg_table);
}
