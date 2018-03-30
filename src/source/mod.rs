//! Data sources.

mod file;
pub use self::file::FileReader;

mod file_locator;
pub use self::file_locator::FileLocator;

mod csv;
pub use self::csv::{CsvSource, CsvReader};

pub(crate) mod decode;

pub(crate) mod sample;
