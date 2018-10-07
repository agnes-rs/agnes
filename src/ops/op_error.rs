use std::error::Error;
use std::fmt;

use error::*;

/// Error during data operations type inference.
#[derive(Debug)]
pub enum TypeError {
    /// Error during addition between Rust type (first argument, as string) and `String`
    Add(String, String),
    /// Error during subtraction between Rust type (first argument, as string) and `String`
    Sub(String, String),
    /// Error during multiplication between Rust type (first argument, as string) and `String`
    Mul(String, String),
    /// Error during division between Rust type (first argument, as string) and `String`
    Div(String, String),
}
impl fmt::Display for TypeError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            TypeError::Add(ref s, ref ft) => write!(f,
                "unable to add value of type {} to field of type {}", s, ft),
            TypeError::Sub(ref s, ref ft) => write!(f,
                "unable to subtract value of type {} from field of type {}", s, ft),
            TypeError::Mul(ref s, ref ft) => write!(f,
                "unable to multiply field of type {} by value of type {}", ft, s),
            TypeError::Div(ref s, ref ft) => write!(f,
                "unable to divide field of type {} by value of type {}", ft, s),
        }
    }
}
impl Error for TypeError {
    fn description(&self) -> &str {
        match *self {
            TypeError::Add(..) => "addition error",
            TypeError::Sub(..) => "subtraction error",
            TypeError::Mul(..) => "multiplication error",
            TypeError::Div(..) => "division error"
        }
    }

    fn cause(&self) -> Option<&dyn Error> {
        None
    }
}
impl From<TypeError> for AgnesError {
    fn from(err: TypeError) -> AgnesError {
        AgnesError::TypeInference(err)
    }
}

/// Error during data operations type inference between fields.
#[derive(Debug)]
pub enum FieldTypeError {
    /// Error during addition between two `FieldType`s
    Add(String, String),
    /// Error during subtraction between two `String`s
    Sub(String, String),
    /// Error during multiplication between two `String`s
    Mul(String, String),
    /// Error during division between two `String`s
    Div(String, String),
}
impl fmt::Display for FieldTypeError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            FieldTypeError::Add(ref left, ref right) => write!(f,
                "unable to add field of type {} to field of type {}", left, right),
            FieldTypeError::Sub(ref left, ref right) => write!(f,
                "unable to subtract field of type {} from field of type {}", right, left),
            FieldTypeError::Mul(ref left, ref right) => write!(f,
                "unable to multiply field of type {} by field of type {}", left, right),
            FieldTypeError::Div(ref left, ref right) => write!(f,
                "unable to divide field of type {} by field of type {}", left, right),
        }
    }
}
impl Error for FieldTypeError {
    fn description(&self) -> &str {
        match *self {
            FieldTypeError::Add(..) => "field addition error",
            FieldTypeError::Sub(..) => "field subtraction error",
            FieldTypeError::Mul(..) => "field multiplication error",
            FieldTypeError::Div(..) => "field division error"
        }
    }

    fn cause(&self) -> Option<&dyn Error> {
        None
    }
}
impl From<FieldTypeError> for AgnesError {
    fn from(err: FieldTypeError) -> AgnesError {
        AgnesError::FieldTypeInference(err)
    }
}
