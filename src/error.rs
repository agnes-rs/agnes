//! General error struct and helpful conversions.

use std;
use std::error::Error;
use std::fmt;
use std::io;

use csv;
use csv_sniffer;
use hyper;
use native_tls;

use field::FieldIdent;

/// General DataFrame error enum.
#[derive(Debug)]
pub enum AgnesError {
    /// File IO error.
    Io(io::Error),
    /// Network-related error
    Net(NetError),
    /// CSV reading / parsing error
    Csv(csv::Error),
    /// CSV sniffer error
    CsvSniffer(csv_sniffer::error::SnifferError),
    /// CSV dialect error
    CsvDialect(String),
    /// Parsing error (failure parsing as specified type).
    Parse(ParseError),
    /// Charset Decoding error.
    Decode(String),
    /// Field missing from DataSource.
    FieldNotFound(FieldIdent),
    /// Dimension mismatch
    DimensionMismatch(String),
    /// Indexing error
    IndexError {
        /// out-of-bounds index
        index: usize,
        /// length of underlying data structure
        len: usize,
    },
    /// Length mismatch error
    LengthMismatch {
        /// Expected length
        expected: usize,
        /// Observed length
        actual: usize,
    },
}

/// Wrapper for DataFrame-based results.
pub type Result<T> = ::std::result::Result<T, AgnesError>;

impl fmt::Display for AgnesError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            AgnesError::Io(ref err) => write!(f, "IO error: {}", err),
            AgnesError::Net(ref err) => write!(f, "Network error: {}", err),
            AgnesError::Csv(ref err) => write!(f, "CSV error: {}", err),
            AgnesError::CsvSniffer(ref err) => write!(f, "CSV sniffer error: {}", err),
            AgnesError::CsvDialect(ref s) => write!(f, "CSV structure error: {}", s),
            AgnesError::Parse(ref err) => write!(f, "Parse error: {}", err),
            AgnesError::Decode(ref s) => write!(f, "Decode error: {}", s),
            AgnesError::FieldNotFound(ref ident) => {
                write!(f, "Missing source field: {}", ident.to_string())
            }
            AgnesError::DimensionMismatch(ref s) => write!(f, "Dimension mismatch: {}", s),
            AgnesError::IndexError { index, len } => write!(
                f,
                "Index error: index {} exceeds data length {}",
                index, len
            ),
            AgnesError::LengthMismatch { expected, actual } => write!(
                f,
                "Length mismatch: expected {} does not match actual {}",
                expected, actual
            ),
        }
    }
}

impl Error for AgnesError {
    fn description(&self) -> &str {
        match *self {
            AgnesError::Io(ref err) => err.description(),
            AgnesError::Net(ref err) => err.description(),
            AgnesError::Csv(ref err) => err.description(),
            AgnesError::CsvSniffer(ref err) => err.description(),
            AgnesError::CsvDialect(ref s) => s,
            AgnesError::Parse(ref err) => err.description(),
            AgnesError::Decode(ref s) => s,
            AgnesError::FieldNotFound(_) => "missing source field",
            AgnesError::DimensionMismatch(ref s) => s,
            AgnesError::IndexError { .. } => "indexing error",
            AgnesError::LengthMismatch { .. } => "length mismatch",
        }
    }

    fn cause(&self) -> Option<&dyn Error> {
        match *self {
            AgnesError::Io(ref err) => Some(err),
            AgnesError::Net(ref err) => Some(err),
            AgnesError::Csv(ref err) => Some(err),
            AgnesError::CsvSniffer(ref err) => Some(err),
            AgnesError::CsvDialect(_) => None,
            AgnesError::Parse(ref err) => Some(err),
            AgnesError::Decode(_) => None,
            AgnesError::FieldNotFound(_) => None,
            AgnesError::DimensionMismatch(_) => None,
            AgnesError::IndexError { .. } => None,
            AgnesError::LengthMismatch { .. } => None,
        }
    }
}

/// Error that stems from some sort of network-related exception.
#[derive(Debug)]
pub enum NetError {
    /// Invalid URI
    Uri(hyper::http::uri::InvalidUri),
    /// Unsupported Scheme
    UnsupportedScheme(Option<hyper::http::uri::Scheme>),
    /// Secure layer error.
    Tls(native_tls::Error),
    /// HTTP error.
    Http(hyper::Error),
    /// Local file error
    LocalFile,
}
impl fmt::Display for NetError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            NetError::Uri(ref err) => write!(f, "Invalid URI error: {}", err),
            NetError::UnsupportedScheme(ref scheme) => match scheme {
                Some(scheme) => write!(f, "Unsupported scheme: {}", scheme),
                None => write!(f, "Missing scheme"),
            },
            NetError::Tls(ref err) => write!(f, "TLS error: {}", err),
            NetError::Http(ref err) => write!(f, "HTTP error: {}", err),
            NetError::LocalFile => write!(f, "unable to access local file over HTTP"),
        }
    }
}
impl Error for NetError {
    fn description(&self) -> &str {
        match *self {
            NetError::Uri(ref err) => err.description(),
            NetError::UnsupportedScheme(_) => "unsupported / missing scheme",
            NetError::Tls(ref err) => err.description(),
            NetError::Http(ref err) => err.description(),
            NetError::LocalFile => "unable to read local file over HTTP",
        }
    }

    fn cause(&self) -> Option<&dyn Error> {
        match *self {
            NetError::Uri(ref err) => Some(err),
            NetError::UnsupportedScheme(_) => None,
            NetError::Tls(ref err) => Some(err),
            NetError::Http(ref err) => Some(err),
            NetError::LocalFile => None,
        }
    }
}

/// Error parsing data type from string.
#[derive(Debug)]
pub enum ParseError {
    /// Integer
    Int(std::num::ParseIntError),
    /// Boolean
    Bool(std::str::ParseBoolError),
    /// Floating-point
    Float(std::num::ParseFloatError),
    /// String
    Str(std::string::ParseError),
}
impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            ParseError::Int(ref err) => write!(f, "Integer parse error: {}", err),
            ParseError::Bool(ref err) => write!(f, "Boolean parse error: {}", err),
            ParseError::Float(ref err) => write!(f, "Float parse error: {}", err),
            ParseError::Str(ref err) => write!(f, "String parse error: {}", err),
        }
    }
}
impl Error for ParseError {
    fn description(&self) -> &str {
        match *self {
            ParseError::Int(ref err) => err.description(),
            ParseError::Bool(ref err) => err.description(),
            ParseError::Float(ref err) => err.description(),
            ParseError::Str(ref err) => err.description(),
        }
    }

    fn cause(&self) -> Option<&dyn Error> {
        match *self {
            ParseError::Int(ref err) => Some(err),
            ParseError::Bool(ref err) => Some(err),
            ParseError::Float(ref err) => Some(err),
            ParseError::Str(ref err) => Some(err),
        }
    }
}

impl From<std::num::ParseIntError> for ParseError {
    fn from(err: std::num::ParseIntError) -> ParseError {
        ParseError::Int(err)
    }
}
impl From<std::num::ParseIntError> for AgnesError {
    fn from(err: std::num::ParseIntError) -> AgnesError {
        AgnesError::Parse(err.into())
    }
}
impl From<std::num::ParseFloatError> for ParseError {
    fn from(err: std::num::ParseFloatError) -> ParseError {
        ParseError::Float(err)
    }
}
impl From<std::num::ParseFloatError> for AgnesError {
    fn from(err: std::num::ParseFloatError) -> AgnesError {
        AgnesError::Parse(err.into())
    }
}
impl From<std::str::ParseBoolError> for ParseError {
    fn from(err: std::str::ParseBoolError) -> ParseError {
        ParseError::Bool(err)
    }
}
impl From<std::str::ParseBoolError> for AgnesError {
    fn from(err: std::str::ParseBoolError) -> AgnesError {
        AgnesError::Parse(err.into())
    }
}
impl From<std::string::ParseError> for ParseError {
    fn from(err: std::string::ParseError) -> ParseError {
        ParseError::Str(err)
    }
}
impl From<std::string::ParseError> for AgnesError {
    fn from(err: std::string::ParseError) -> AgnesError {
        AgnesError::Parse(err.into())
    }
}
impl From<ParseError> for AgnesError {
    fn from(err: ParseError) -> AgnesError {
        AgnesError::Parse(err)
    }
}

impl From<io::Error> for AgnesError {
    fn from(err: io::Error) -> AgnesError {
        AgnesError::Io(err)
    }
}

impl From<NetError> for AgnesError {
    fn from(err: NetError) -> AgnesError {
        AgnesError::Net(err)
    }
}

impl From<native_tls::Error> for NetError {
    fn from(err: native_tls::Error) -> NetError {
        NetError::Tls(err)
    }
}
impl From<native_tls::Error> for AgnesError {
    fn from(err: native_tls::Error) -> AgnesError {
        AgnesError::Net(err.into())
    }
}

impl From<hyper::Error> for NetError {
    fn from(err: hyper::Error) -> NetError {
        NetError::Http(err)
    }
}
impl From<hyper::Error> for AgnesError {
    fn from(err: hyper::Error) -> AgnesError {
        AgnesError::Net(err.into())
    }
}

impl From<hyper::http::uri::InvalidUri> for NetError {
    fn from(err: hyper::http::uri::InvalidUri) -> NetError {
        NetError::Uri(err)
    }
}
impl From<hyper::http::uri::InvalidUri> for AgnesError {
    fn from(err: hyper::http::uri::InvalidUri) -> AgnesError {
        AgnesError::Net(err.into())
    }
}

impl From<csv::Error> for AgnesError {
    fn from(err: csv::Error) -> AgnesError {
        AgnesError::Csv(err)
    }
}

impl From<csv_sniffer::error::SnifferError> for AgnesError {
    fn from(err: csv_sniffer::error::SnifferError) -> AgnesError {
        AgnesError::CsvSniffer(err)
    }
}
