extern crate agnes;
extern crate csv_sniffer;

use agnes::data_types::csv::*;

mod common;

#[test]
fn csv_load_test() {
    let (mut csv_rdr, metadata) = common::load_csv_file("gdp.nopreamble.csv");
    assert_eq!(metadata.num_fields, 63);
    assert_eq!(metadata.dialect.header.num_preamble_rows, 0);
    let dv: DataView = csv_rdr.read().unwrap().into();
    assert_eq!(dv.nrows(), 264);
    assert_eq!(dv.nfields(), 63);
    let subdv = dv.v(["Country Name", "1983"]);
    assert_eq!(subdv.nrows(), 264);
    assert_eq!(subdv.nfields(), 2);
    // println!("{}", dv.v(["Country Name", "1983"]));
    // println!("{}", dv.v(["Country Name", "1983"]).v(["Country Name"]));
}

#[test]
fn csv_load_test_skip() {
    let (mut csv_rdr, metadata) = common::load_csv_file("gdp.csv");
    assert_eq!(metadata.num_fields, 63);
    assert_eq!(metadata.dialect.header.num_preamble_rows, 4);
    let dv: DataView = csv_rdr.read().unwrap().into();
    assert_eq!(dv.nrows(), 264);
    assert_eq!(dv.nfields(), 63);
    let subdv = dv.v(["Country Name", "1983"]);
    assert_eq!(subdv.nrows(), 264);
    assert_eq!(subdv.nfields(), 2);
    // println!("{}", dv.v(["Country Name", "1983"]));
}
