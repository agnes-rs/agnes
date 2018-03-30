//! Data sources.

mod file;
pub use self::file::{LocalFileReader, FileReader, HttpFileReader, FileLocator};

mod csv;
pub use self::csv::{CsvSource, CsvReader};

pub(crate) mod decode;
