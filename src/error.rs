//! General error struct for entire package, as well as helpful conversions.

use std::error::Error;
use std::fmt;
use std::io;
use std;

use csv;
use native_tls;
use hyper;

use field::FieldIdent;

/// General DataFrame error enum.
#[derive(Debug)]
pub enum ViewsError {
    /// File IO error.
    Io(io::Error),
    /// Network-related error
    Net(NetError),
    /// CSV reading / parsing error
    Csv(csv::Error),
    /// Field access error.
    Field(String),
    /// Parsing error (failure parsing as specified type).
    Parse(ParseError),
    /// Charset Decoding error.
    Decode(String),
    /// Field missing from DataSource.
    MissingSourceField(FieldIdent)
}

/// Wrapper for DataFrame-based results.
pub type Result<T> = ::std::result::Result<T, ViewsError>;

impl fmt::Display for ViewsError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            ViewsError::Io(ref err) => write!(f, "IO error: {}", err),
            ViewsError::Net(ref err) => write!(f, "Network error: {}", err),
            ViewsError::Csv(ref err) => write!(f, "CSV error: {}", err),
            ViewsError::Field(ref s) => write!(f, "Field error: {}", s),
            ViewsError::Parse(ref err) => write!(f, "Parse error: {}", err),
            ViewsError::Decode(ref s) => write!(f, "Decode error: {}", s),
            ViewsError::MissingSourceField(ref ident) =>
                write!(f, "Missing source field: {}", ident.to_string()),
        }
    }
}

impl Error for ViewsError {
    fn description(&self) -> &str {
        match *self {
            ViewsError::Io(ref err) => err.description(),
            ViewsError::Net(ref err) => err.description(),
            ViewsError::Csv(ref err) => err.description(),
            ViewsError::Field(ref s) => s,
            ViewsError::Parse(ref err) => err.description(),
            ViewsError::Decode(ref s) => s,
            ViewsError::MissingSourceField(_) => "missing source field"
        }
    }

    fn cause(&self) -> Option<&Error> {
        match *self {
            ViewsError::Io(ref err) => Some(err),
            ViewsError::Net(ref err) => Some(err),
            ViewsError::Csv(ref err) => Some(err),
            ViewsError::Field(_) => None,
            ViewsError::Parse(ref err) => Some(err),
            ViewsError::Decode(_) => None,
            ViewsError::MissingSourceField(_) => None,
        }
    }
}

/// Error that stems from some sort of network-related exception.
#[derive(Debug)]
pub enum NetError {
    /// Unsupported URI scheme (http, ftp, ssh, etc.)
    UnsupportedUriScheme(Option<String>),
    /// Secure layer error.
    Tls(native_tls::Error),
    /// HTTP error.
    Http(hyper::Error),
}
impl fmt::Display for NetError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            NetError::UnsupportedUriScheme(ref t) =>
                write!(f, "Unsupported scheme: {}", t.clone().unwrap_or("none".to_string())),
            NetError::Tls(ref err) => write!(f, "TLS error: {}", err),
            NetError::Http(ref err) => write!(f, "HTTP error: {}", err),
        }
    }
}
impl Error for NetError {
    fn description(&self) -> &str {
        match *self {
            NetError::UnsupportedUriScheme(ref scheme) => {
                match *scheme {
                    Some(ref s) => &s[..],
                    None => "none",
                }
            },
            NetError::Tls(ref err) => err.description(),
            NetError::Http(ref err) => err.description(),
        }
    }

    fn cause(&self) -> Option<&Error> {
        match *self {
            NetError::UnsupportedUriScheme(_) => None,
            NetError::Tls(ref err) => Some(err),
            NetError::Http(ref err) => Some(err),
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
}
impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            ParseError::Int(ref err) => write!(f, "Integer parse error: {}", err),
            ParseError::Bool(ref err) => write!(f, "Boolean parse error: {}", err),
            ParseError::Float(ref err) => write!(f, "Float parse error: {}", err),
        }
    }
}
impl Error for ParseError {
    fn description(&self) -> &str {
        match *self {
            ParseError::Int(ref err) => err.description(),
            ParseError::Bool(ref err) => err.description(),
            ParseError::Float(ref err) => err.description(),
        }
    }

    fn cause(&self) -> Option<&Error> {
        match *self {
            ParseError::Int(ref err) => Some(err),
            ParseError::Bool(ref err) => Some(err),
            ParseError::Float(ref err) => Some(err),
        }
    }
}

impl From<std::num::ParseIntError> for ParseError {
    fn from(err: std::num::ParseIntError) -> ParseError {
        ParseError::Int(err)
    }
}
impl From<std::num::ParseIntError> for ViewsError {
    fn from(err: std::num::ParseIntError) -> ViewsError {
        ViewsError::Parse(err.into())
    }
}
impl From<std::num::ParseFloatError> for ParseError {
    fn from(err: std::num::ParseFloatError) -> ParseError {
        ParseError::Float(err)
    }
}
impl From<std::num::ParseFloatError> for ViewsError {
    fn from(err: std::num::ParseFloatError) -> ViewsError {
        ViewsError::Parse(err.into())
    }
}
impl From<std::str::ParseBoolError> for ParseError {
    fn from(err: std::str::ParseBoolError) -> ParseError {
        ParseError::Bool(err)
    }
}
impl From<std::str::ParseBoolError> for ViewsError {
    fn from(err: std::str::ParseBoolError) -> ViewsError {
        ViewsError::Parse(err.into())
    }
}
impl From<ParseError> for ViewsError {
    fn from(err: ParseError) -> ViewsError {
        ViewsError::Parse(err)
    }
}


impl From<io::Error> for ViewsError {
    fn from(err: io::Error) -> ViewsError {
        ViewsError::Io(err)
    }
}

impl From<NetError> for ViewsError {
    fn from(err: NetError) -> ViewsError {
        ViewsError::Net(err)
    }
}

impl From<native_tls::Error> for NetError {
    fn from(err: native_tls::Error) -> NetError {
        NetError::Tls(err)
    }
}
impl From<native_tls::Error> for ViewsError {
    fn from(err: native_tls::Error) -> ViewsError {
        ViewsError::Net(err.into())
    }
}

impl From<hyper::Error> for NetError {
    fn from(err: hyper::Error) -> NetError {
        NetError::Http(err)
    }
}
impl From<hyper::Error> for ViewsError {
    fn from(err: hyper::Error) -> ViewsError {
        ViewsError::Net(err.into())
    }
}

impl From<csv::Error> for ViewsError {
    fn from(err: csv::Error) -> ViewsError {
        ViewsError::Csv(err)
    }
}
