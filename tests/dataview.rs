extern crate agnes;
extern crate serde;
extern crate serde_json;

mod common;

use std::path::Path;

use agnes::source::CsvReader;
use agnes::field::{FieldIdent, FieldType};
use agnes::view::DataView;

#[test]
fn rename() {
    let mut csv_rdr = common::load_test_file("sample1.csv");

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
    let mut dv: DataView = csv_rdr.read().unwrap().into();
    println!("{}", dv);
    println!("{}", serde_json::to_string(&dv).unwrap());
    dv.rename("state", "ST").unwrap();
    println!("{}", dv);
    println!("{}", serde_json::to_string(&dv).unwrap());
}

#[test]
fn aggregate() {
//     let data_filepath_gdp = Path::new(file!()).parent().unwrap().join("data/gdp.csv");
//     let mut csv_rdr = CsvReader::new(
//         CsvSourceBuilder::new(data_filepath_gdp)
//             .has_headers(HasHeaders::YesSkip(4))
//             .fields(fields![
//                 "Country Name" => FieldType::Text,
//                 "Country Code" => FieldType::Text,
//                 "1983"         => FieldType::Float
//             ])
//             .build()
//     ).unwrap();
    let mut csv_rdr = common::load_test_file("gdp.csv");
    let mut dv_gdp: DataView = DataView::from(csv_rdr.read().unwrap())
        .v(["Country Name", "Country Code", "1983"]);

    // let data_filepath_life = Path::new(file!()).parent().unwrap().join("data/life.csv");
    // let mut csv_rdr = CsvReader::new(
    //     CsvSourceBuilder::new(data_filepath_life)
    //         .has_headers(HasHeaders::YesSkip(4))
    //         .fields(fields![
    //             "1983"         => FieldType::Float
    //         ])
    //         .build()
    // ).unwrap();
    let mut csv_rdr = common::load_test_file("life.csv");
    // only take extra '1983' column
    let mut dv_life: DataView = DataView::from(csv_rdr.read().unwrap()).v("1983");


    dv_gdp.rename("1983", "1983 GDP").unwrap();
    dv_life.rename("1983", "1983 Life Expectancy").unwrap();

    let dv = dv_gdp.merge(&dv_life).unwrap();
    println!("{}", dv);
    // println!("{}", serde_json::to_string(&dv).unwrap());
}
