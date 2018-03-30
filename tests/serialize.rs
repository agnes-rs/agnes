#[macro_use] extern crate agnes;
extern crate serde;
extern crate serde_json;

use std::path::Path;

use agnes::source::{CsvReader};
use agnes::field::{FieldIdent, FieldType};
use agnes::view::DataView;

mod common;

#[test]
fn csv_serialize_test() {
    // let data_filepath = Path::new(file!()).parent().unwrap().join("data/sample1.csv");
    // let file = FileSource::new(data_filepath);
    // let mut csv_rdr = CsvReader::new(
    //     CsvSourceBuilder::new(file)
    //         .fields(fields![
    //             "state" => FieldType::Text,
    //             "val1" => FieldType::Unsigned,
    //             "val2" => FieldType::Float
    //         ])
    //         .build()
    // ).unwrap();
    let mut csv_rdr = common::load_test_file("sample1.csv");
    let dv: DataView = csv_rdr.read().unwrap().into();
    println!("{}", dv);
    println!("{}", serde_json::to_string(&dv).unwrap());

}
