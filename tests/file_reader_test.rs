extern crate agnes;
extern crate futures;
extern crate hyper;
extern crate hyper_tls;
extern crate tokio_core;
extern crate tokio_io;

use std::io::{Write, Read};
use std::path::Path;

use agnes::source::{FileReader, FileLocator};

// #[test]
// fn load_test() {

//     let file1_contents = {
//         let uri: hyper::Uri = "https://gist.githubusercontent.com/jblondin/\
//             9e06a2c8e8d6c25a24034c52b4ce103a/raw/\
//             1cf9c8b531e11b9bc16f56b88be4c615dc103eb1/sample1.csv".parse().unwrap();
//         let csv = FileSource::new(uri);
//         let mut buf: Vec<u8> = vec![];
//         csv.read_chunks(|chunk| buf.write_all(&chunk).unwrap()).unwrap();
//         String::from_utf8(buf).unwrap()
//     };

//     let file2_contents = {
//         let data_filepath = Path::new(file!()).parent().unwrap().join("data/sample1.csv");
//         let csv = FileSource::new(data_filepath);
//         let mut buf: Vec<u8> = vec![];
//         csv.read_chunks(|chunk| buf.write_all(&chunk).unwrap()).unwrap();
//         String::from_utf8(buf).unwrap()
//     };

//     assert_eq!(file1_contents, file2_contents);

//     // $ wc -c tests/data/sample1.csv
//     // 103 tests/data/sample1.csv
//     assert_eq!(file1_contents.len(), 103);
// }

#[test]
fn load_test_sync() {
    let file1_contents = {
        let uri: hyper::Uri = "https://gist.githubusercontent.com/jblondin/\
            9e06a2c8e8d6c25a24034c52b4ce103a/raw/\
            1cf9c8b531e11b9bc16f56b88be4c615dc103eb1/sample1.csv".parse().unwrap();
        let mut reader = FileReader::new(&FileLocator::Http(uri)).unwrap();

        let mut buf = String::new();
        reader.read_to_string(&mut buf).unwrap();
        buf
    };

    let file2_contents = {
        let data_filepath = Path::new(file!()).parent().unwrap().join("data/sample1.csv");
        let mut reader = FileReader::new(&FileLocator::File(data_filepath)).unwrap();

        let mut buf = String::new();
        reader.read_to_string(&mut buf).unwrap();
        buf
    };

    assert_eq!(file1_contents, file2_contents);

    // $ wc -c tests/data/sample1.csv
    // 103 tests/data/sample1.csv
    assert_eq!(file1_contents.len(), 103);

}
