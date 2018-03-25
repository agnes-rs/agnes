use std::path::{Path, PathBuf};
use hyper;

/// Identifiers / paths to find file locations.
#[derive(Debug, Clone)]
pub enum FileLocator {
    /// A web-based location (URI)
    Http(hyper::Uri),
    /// A local file
    File(PathBuf)
}


impl<'a> From<&'a Path> for FileLocator {
    fn from(orig: &'a Path) -> FileLocator {
        FileLocator::File(orig.to_path_buf())
    }
}
impl<'a, P: AsRef<Path>> From<&'a P> for FileLocator {
    fn from(orig: &'a P) -> FileLocator {
        FileLocator::File(orig.as_ref().to_path_buf())
    }
}
impl From<PathBuf> for FileLocator {
    fn from(orig: PathBuf) -> FileLocator {
        FileLocator::File(orig)
    }
}
impl From<hyper::Uri> for FileLocator {
    fn from(orig: hyper::Uri) -> FileLocator {
        FileLocator::Http(orig)
    }
}
