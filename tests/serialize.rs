extern crate agnes;
extern crate serde;
extern crate serde_json;
extern crate csv_sniffer;

use agnes::view::DataView;

mod common;

#[test]
fn csv_serialize_test() {
    let (mut csv_rdr, _) = common::load_csv_file("sample1.csv");
    let dv: DataView = csv_rdr.read().unwrap().into();
    println!("{}", dv);
    println!("{}", serde_json::to_string(&dv).unwrap());
}
