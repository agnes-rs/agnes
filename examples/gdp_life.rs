extern crate agnes;
extern crate csv_sniffer;

use std::path::Path;

use agnes::source::{CsvSource, CsvReader};
use agnes::filter::Filter;
use agnes::data_types::csv::*;
use agnes::join::{Join, JoinKind};

pub fn load_csv_file(filename: &str) -> CsvReader {
    let data_filepath = Path::new(file!()) // start as this file
        .parent().unwrap()                 // navigate up to examples directory
        .join("data")                      // navigate into data directory
        .join(filename);                   // navigate to target file

    let source = CsvSource::new(data_filepath.into()).unwrap();
    CsvReader::new(&source).unwrap()
}

fn main() {
    let mut csv_rdr = load_csv_file("gdp/API_NY.GDP.MKTP.CD_DS2_en_csv_v2.csv");
    let dv_gdp = DataView::from(csv_rdr.read().unwrap())
        .v(["Country Name", "Country Code", "1983"]);

    let mut csv_rdr = load_csv_file("gdp/Metadata_Country_API_NY.GDP.MKTP.CD_DS2_en_csv_v2.csv");
    let mut dv_gdp_metadata = DataView::from(csv_rdr.read().unwrap())
        .v(["Country Code", "Region"]);
    dv_gdp_metadata.filter("Region", |_: &String| true).unwrap();

    let mut dv_gdp_joined: DataView = dv_gdp.join::<String>(&dv_gdp_metadata, &Join::equal(
        JoinKind::Inner,
        "Country Code",
        "Country Code"
    )).unwrap().into();

    let mut csv_rdr = load_csv_file("life/API_SP.DYN.LE00.IN_DS2_en_csv_v2.csv");
    let mut dv_life = DataView::from(csv_rdr.read().unwrap()).v(["Country Code", "1983"]);

    dv_gdp_joined.rename("1983", "1983 GDP").unwrap();
    dv_life.rename("1983", "1983 Life Expectancy").unwrap();

    let dv: DataView = dv_gdp_joined.join::<String>(&dv_life, &Join::equal(
        JoinKind::Inner,
        "Country Code",
        "Country Code"
    )).unwrap().into();
    let dv = dv.v(["Country Name", "1983 GDP", "1983 Life Expectancy"]);

    println!("{}", dv);
}
