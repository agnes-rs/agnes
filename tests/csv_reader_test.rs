#[macro_use] extern crate agnes;

use std::path::Path;

use agnes::source::{HasHeaders, CsvReader, CsvSourceBuilder};
use agnes::field::{FieldIdent, FieldType};
use agnes::DataView;

#[test]
fn csv_load_test() {
    let data_filepath = Path::new(file!()).parent().unwrap().join("data/gdp.nopreamble.csv");
    let mut csv_rdr = CsvReader::new(
        CsvSourceBuilder::new(data_filepath)
            .fields(fields![
                "Country Name" => FieldType::Text,
                "Country Code" => FieldType::Text,
                "1983"         => FieldType::Float
            ])
            .build()
    ).unwrap();
    let dv: DataView = csv_rdr.read().unwrap().into();
    println!("{}", &dv%["Country Name", "1983"]);
    println!("{}", dv.v(["Country Name", "1983"]).v(["Country Name"]));
}

#[test]
fn csv_load_test_skip() {
    let data_filepath = Path::new(file!()).parent().unwrap().join("data/gdp.csv");
    let mut csv_rdr = CsvReader::new(
        CsvSourceBuilder::new(data_filepath)
            .has_headers(HasHeaders::YesSkip(4))
            .fields(fields![
                "Country Name" => FieldType::Text,
                "1983"         => FieldType::Float
            ])
            .build()
    ).unwrap();
    let dv: DataView = csv_rdr.read().unwrap().into();
    println!("{}", &dv%["Country Name", "1983"]);
}
