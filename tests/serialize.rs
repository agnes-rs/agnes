#[macro_use] extern crate agnes;
extern crate serde;
#[macro_use] extern crate serde_json;
extern crate csv_sniffer;
extern crate typenum;

use agnes::data_types::csv::*;

mod common;

// #[test]
// fn csv_serialize_test() {
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
//     let dv: DataView = csv_rdr.read().unwrap().into();
//     println!("{}", dv);
//     let dv_json: serde_json::Value = serde_json::from_slice(
//         &serde_json::to_vec(&dv).unwrap()).unwrap();

//     assert_eq!(dv_json, json![{
//       "state": [
//         "OH",
//         "PA",
//         "NH",
//         "NC",
//         "CA",
//         "NY",
//         "VA",
//         "SC"
//       ],
//       "val1": [
//         4,
//         54,
//         23,
//         21,
//         85,
//         32,
//         44,
//         89
//       ],
//       "val2": [
//         5.03,
//         2.34,
//         0.42,
//         0.204,
//         0.32,
//         3.21,
//         5.66,
//         9.11
//       ]
//     }]);
// }
