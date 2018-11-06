// #[macro_use] extern crate agnes;
// extern crate csv_sniffer;
// extern crate typenum;

// use agnes::data_types::csv::*;

// mod common;

// #[test]
// fn csv_load_test() {
//     spec![
//         let spec = {
//             CountryName("Country Name"): String,
//             CountryCode("Country Code"): String,
//             Year1983("1983"): f64,
//         };
//     ];
//     let (mut csv_rdr, metadata) = common::load_csv_file(
//         "gdp.nopreamble.csv",
//         spec
//     );
//     assert_eq!(metadata.num_fields, 63);
//     assert_eq!(metadata.dialect.header.num_preamble_rows, 0);
//     let dv: DataView = csv_rdr.read().unwrap().into();
//     assert_eq!(dv.nrows(), 264);
//     assert_eq!(dv.nfields(), 3);
//     let subdv = dv.v(["Country Name", "1983"]);
//     assert_eq!(subdv.nrows(), 264);
//     assert_eq!(subdv.nfields(), 2);
//     // println!("{}", dv.v(["Country Name", "1983"]));
//     // println!("{}", dv.v(["Country Name", "1983"]).v(["Country Name"]));
// }

// #[test]
// fn csv_load_test_skip() {
//     spec![
//         let spec = {
//             CountryName("Country Name"): String,
//             CountryCode("Country Code"): String,
//             Year1983("1983"): f64,
//         };
//     ];    let (mut csv_rdr, metadata) = common::load_csv_file(
//         "gdp.csv",
//         spec
//     );
//     assert_eq!(metadata.num_fields, 63);
//     assert_eq!(metadata.dialect.header.num_preamble_rows, 4);
//     let dv: DataView = csv_rdr.read().unwrap().into();
//     assert_eq!(dv.nrows(), 264);
//     assert_eq!(dv.nfields(), 3);
//     let subdv = dv.v(["Country Name", "1983"]);
//     assert_eq!(subdv.nrows(), 264);
//     assert_eq!(subdv.nfields(), 2);
//     // println!("{}", dv.v(["Country Name", "1983"]));
// }
