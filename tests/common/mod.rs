// use std::path::Path;

// use csv_sniffer::metadata::Metadata;

// // use agnes::source::csv2::{CsvSource, CsvReader, IntoCsvSrcSpec};
// use agnes::source::{CsvSource, CsvReader};
// use agnes::fieldlist::{AssocFields, FieldSpecs};
// use agnes::data_types::csv::*;

// pub fn load_csv_file<Spec>(filename: &str, spec: Spec) -> (CsvReader, Metadata)
//     where Spec: FieldSpecs<Types> + AssocFields
// // pub fn load_csv_file<Spec>(filename: &str, spec: Spec) -> (CsvReader<Spec::CsvSrcSpec>, Metadata)
// //     where Spec: IntoCsvSrcSpec
// {
//     let data_filepath = Path::new(file!()) // start as this file
//         .parent().unwrap()                 // navigate up to common directory
//         .parent().unwrap()                 // navigate up to tests directory
//         .join("data")                      // navigate into data directory
//         .join(filename);                   // navigate to target file

//     let source = CsvSource::new(data_filepath.into()).unwrap();
//     (CsvReader::new(&source, spec).unwrap(), source.metadata().clone())
// }
