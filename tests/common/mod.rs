use std::fmt::Debug;
use std::path::Path;

use csv_sniffer::metadata::Metadata;

use agnes::source::csv::{CsvReader, CsvSource, IntoCsvSrcSpec};

pub fn load_csv_file<Spec>(filename: &str, spec: Spec) -> (CsvReader<Spec::CsvSrcSpec>, Metadata)
where
    Spec: IntoCsvSrcSpec,
    <Spec as IntoCsvSrcSpec>::CsvSrcSpec: Debug,
{
    let data_filepath = Path::new(file!()) // start as this file
        .parent()
        .unwrap() // navigate up to common directory
        .parent()
        .unwrap() // navigate up to tests directory
        .join("data") // navigate into data directory
        .join(filename); // navigate to target file

    let source = CsvSource::new(data_filepath.into()).unwrap();
    (
        CsvReader::new(&source, spec).unwrap(),
        source.metadata().clone(),
    )
}
