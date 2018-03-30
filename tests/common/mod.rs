
use std::path::Path;
use agnes::source::{CsvSource, CsvReader};

pub fn load_test_file(filename: &str) -> CsvReader {
    let data_filepath = Path::new(file!()) // start as this file
        .parent().unwrap()                 // navigate up to common directory
        .parent().unwrap()                 // navigate up to tests directory
        .join("data")                      // navigate into data directory
        .join(filename);                   // navigate to target file

    CsvReader::new(CsvSource::new(data_filepath.into()).unwrap()).unwrap()
}
