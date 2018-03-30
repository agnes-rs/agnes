extern crate agnes;
extern crate csv_sniffer;

use agnes::DataView;

mod common;

#[test]
fn csv_load_test() {
    let mut csv_rdr = common::load_test_file("gdp.nopreamble.csv");
    let dv: DataView = csv_rdr.read().unwrap().into();
    println!("{}", &dv%["Country Name", "1983"]);
    println!("{}", dv.v(["Country Name", "1983"]).v(["Country Name"]));
}

#[test]
fn csv_load_test_skip() {
    let mut csv_rdr = common::load_test_file("gdp.csv");
    let dv: DataView = csv_rdr.read().unwrap().into();
    println!("{}", &dv%["Country Name", "1983"]);
}
