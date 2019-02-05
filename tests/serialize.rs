#[macro_use]
extern crate agnes;
extern crate serde;
#[macro_use]
extern crate serde_json;
extern crate csv_sniffer;

mod common;

namespace![
    pub namespace sample {
        field State: String;
        field Value1: u64;
        field Value2: f64;
    }
];

#[test]
fn csv_serialize_test() {
    use sample::*;

    let sample_spec = spec![
        fieldname State = "state";
        fieldname Value1 = "val1";
        fieldname Value2 = "val2";
    ];
    let (mut csv_rdr, _) = common::load_csv_file("sample1.csv", sample_spec);

    let dv = csv_rdr.read().unwrap().into_view();
    println!("{}", dv);
    let dv_json: serde_json::Value =
        serde_json::from_slice(&serde_json::to_vec(&dv).unwrap()).unwrap();

    assert_eq!(
        dv_json,
        json![{
          "State": [
            "OH",
            "PA",
            "NH",
            "NC",
            "CA",
            "NY",
            "VA",
            "SC"
          ],
          "Value1": [
            4,
            54,
            23,
            21,
            85,
            32,
            44,
            89
          ],
          "Value2": [
            5.03,
            2.34,
            0.42,
            0.204,
            0.32,
            3.21,
            5.66,
            9.11
          ]
        }]
    );
}
