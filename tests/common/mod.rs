use std::path::Path;

use agnes::source::{CsvSource, CsvReader};
use csv_sniffer::metadata::Metadata;

pub fn load_csv_file(filename: &str) -> (CsvReader, Metadata) {
    let data_filepath = Path::new(file!()) // start as this file
        .parent().unwrap()                 // navigate up to common directory
        .parent().unwrap()                 // navigate up to tests directory
        .join("data")                      // navigate into data directory
        .join(filename);                   // navigate to target file

    let source = CsvSource::new(data_filepath.into()).unwrap();
    (CsvReader::new(&source).unwrap(), source.metadata().clone())
}
