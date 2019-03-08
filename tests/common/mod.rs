use std::fmt::Debug;
use std::path::Path;

use csv_sniffer::metadata::Metadata;

use agnes::source::csv::{CsvReader, CsvSource, IntoCsvSrcSchema};

pub fn load_csv_file<Schema>(
    filename: &str,
    schema: Schema,
) -> (CsvReader<Schema::CsvSrcSchema>, Metadata)
where
    Schema: IntoCsvSrcSchema,
    <Schema as IntoCsvSrcSchema>::CsvSrcSchema: Debug,
{
    let data_filepath = Path::new(file!()) // start as this file
        .parent()
        .unwrap() // navigate up to common directory
        .parent()
        .unwrap() // navigate up to tests directory
        .join("data") // navigate into data directory
        .join(filename); // navigate to target file

    let source = CsvSource::new(data_filepath).unwrap();
    (
        CsvReader::new(&source, schema).unwrap(),
        source.metadata().clone(),
    )
}
