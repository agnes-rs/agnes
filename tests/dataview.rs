#[macro_use] extern crate agnes;
extern crate serde;
extern crate serde_json;

use std::path::Path;

use agnes::source::{HasHeaders, CsvReader, CsvSourceBuilder, FileSource};
use agnes::field::{FieldIdent, FieldType};
use agnes::view::DataView;

#[test]
fn rename() {
    let data_filepath = Path::new(file!()).parent().unwrap().join("data/sample1.csv");
    let file = FileSource::new(data_filepath);
    let mut csv_rdr = CsvReader::new(
        CsvSourceBuilder::new(file)
            .fields(fields![
                "state" => FieldType::Text,
                "val1" => FieldType::Unsigned,
                "val2" => FieldType::Float
            ])
            .build()
    ).unwrap();
    let mut dv: DataView = csv_rdr.read().unwrap().into();
    println!("{}", dv);
    println!("{}", serde_json::to_string(&dv).unwrap());
    dv.rename("state", "ST").unwrap();
    println!("{}", dv);
    println!("{}", serde_json::to_string(&dv).unwrap());
}

#[test]
fn aggregate() {
    let data_filepath_gdp = Path::new(file!()).parent().unwrap().join("data/gdp.csv");
    let mut csv_rdr = CsvReader::new(
        CsvSourceBuilder::new(data_filepath_gdp)
            .has_headers(HasHeaders::YesSkip(4))
            .fields(fields![
                "Country Name" => FieldType::Text,
                "Country Code" => FieldType::Text,
                "1983"         => FieldType::Float
            ])
            .build()
    ).unwrap();
    let mut dv_gdp: DataView = csv_rdr.read().unwrap().into();

    let data_filepath_life = Path::new(file!()).parent().unwrap().join("data/life.csv");
    let mut csv_rdr = CsvReader::new(
        CsvSourceBuilder::new(data_filepath_life)
            .has_headers(HasHeaders::YesSkip(4))
            .fields(fields![
                "1983"         => FieldType::Float
            ])
            .build()
    ).unwrap();
    let mut dv_life: DataView = csv_rdr.read().unwrap().into();

    dv_gdp.rename("1983", "1983 GDP").unwrap();
    dv_life.rename("1983", "1983 Life Expectancy").unwrap();

    let dv = dv_gdp.merge(&dv_life).unwrap();
    println!("{}", dv);
    // println!("{}", serde_json::to_string(&dv).unwrap());
}
