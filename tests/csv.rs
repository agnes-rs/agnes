#[macro_use]
extern crate agnes;
extern crate csv_sniffer;
extern crate typenum;

use agnes::cons::Nil;
use agnes::label::LCons;

mod common;

namespace![
    pub namespace gdp {
        field CountryName: String;
        field CountryCode: String;
        field Year1983: f64;
    }
];

#[test]
fn csv_load_test() {
    use gdp::*;

    let gdp_spec = spec![
        fieldname gdp::CountryName = "Country Name";
        fieldname gdp::CountryCode = "Country Code";
        fieldname gdp::Year1983 = "1983";
    ];

    let (mut csv_rdr, metadata) = common::load_csv_file("gdp.nopreamble.csv", gdp_spec);

    assert_eq!(metadata.num_fields, 63);
    assert_eq!(metadata.dialect.header.num_preamble_rows, 0);
    let dv = csv_rdr.read().unwrap().into_view();
    assert_eq!(dv.nrows(), 264);
    assert_eq!(dv.nfields(), 3);
    let subdv = dv.v::<Labels![CountryName, Year1983]>();
    assert_eq!(subdv.nrows(), 264);
    assert_eq!(subdv.nfields(), 2);
    println!("{}", subdv);
}

#[test]
fn csv_load_test_skip() {
    use gdp::*;

    let gdp_spec = spec![
        fieldname gdp::CountryName = "Country Name";
        fieldname gdp::CountryCode = "Country Code";
        fieldname gdp::Year1983 = "1983";
    ];

    let (mut csv_rdr, metadata) = common::load_csv_file("gdp.csv", gdp_spec);

    assert_eq!(metadata.num_fields, 63);
    assert_eq!(metadata.dialect.header.num_preamble_rows, 4);
    let dv = csv_rdr.read().unwrap().into_view();
    assert_eq!(dv.nrows(), 264);
    assert_eq!(dv.nfields(), 3);

    let subdv = dv.v::<Labels![CountryName, Year1983]>();
    assert_eq!(subdv.nrows(), 264);
    assert_eq!(subdv.nfields(), 2);
    println!("{}", subdv);
}
