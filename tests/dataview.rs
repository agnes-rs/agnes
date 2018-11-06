// #[macro_use] extern crate agnes;
// extern crate serde;
// extern crate serde_json;
// extern crate csv_sniffer;
// extern crate typenum;

// mod common;

// use agnes::data_types::csv::*;

// #[test]
// fn subview() {
//     spec![
//         let spec = {
//             CountryName("Country Name"): String,
//             CountryCode("Country Code"): String,
//             Year1983("1983"): f64,
//         };
//     ];

//     let (mut csv_rdr, _) = common::load_csv_file(
//         "gdp.csv",
//         spec
//     );
//     let dv: DataView = csv_rdr.read().unwrap().into();
//     assert_eq!(dv.nrows(), 264);
//     assert_eq!(dv.nfields(), 3);
//     let subview = dv.v(["Country Name", "1983"]);
//     assert_eq!(subview.nrows(), 264);
//     assert_eq!(subview.nfields(), 2);
// }

// #[test]
// fn rename() {
//     spec![
//         let spec = {
//             State("state"): String,
//             Value1("val1"): u64,
//             Value2("val2"): f64,
//         };
//     ];
//     let (mut csv_rdr, _) = common::load_csv_file(
//         "sample1.csv",
//         spec
//     );

//     let mut dv: DataView = csv_rdr.read().unwrap().into();
//     println!("{}", dv);
//     println!("{}", serde_json::to_string(&dv).unwrap());
//     dv.rename("state", "ST").unwrap();
//     println!("{}", dv);
//     println!("{}", serde_json::to_string(&dv).unwrap());
// }

// #[test]
// fn merge() {
//     spec![
//         let gdp_spec = {
//             GdpCountryName("Country Name"): String,
//             GdpCountryCode("Country Code"): String,
//             GdpYear1983("1983"): f64,
//         };
//     ];
//     let (mut csv_rdr, _) = common::load_csv_file(
//         "gdp.csv",
//         gdp_spec
//     );
//     let mut dv_gdp: DataView = DataView::from(csv_rdr.read().unwrap())
//         .v(["Country Name", "Country Code", "1983"]);

//     spec![
//         let life_spec = {
//             LifeCountryName("Country Name"): String,
//             LifeCountryCode("Country Code"): String,
//             LifeYear1983("1983"): f64,
//         };
//     ];
//     let (mut csv_rdr, _) = common::load_csv_file(
//         "life.csv",
//         life_spec
//     );
//     // only take extra '1983' column
//     let mut dv_life: DataView = DataView::from(csv_rdr.read().unwrap()).v("1983");


//     dv_gdp.rename("1983", "1983 GDP").unwrap();
//     dv_life.rename("1983", "1983 Life Expectancy").unwrap();

//     let dv = dv_gdp.merge(&dv_life).unwrap();
//     println!("{}", dv);
//     // println!("{}", serde_json::to_string(&dv).unwrap());
// }
