//! Data sources.

mod file;
pub use self::file::{FileSource, FileReader};

mod file_locator;
pub use self::file_locator::FileLocator;

mod csv;
pub use self::csv::{HasHeaders, CsvSource, CsvReader, CsvSourceBuilder};

pub(crate) mod decode;
