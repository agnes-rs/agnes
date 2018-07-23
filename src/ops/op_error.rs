use std::error::Error;
use std::fmt;

use field::FieldType;
use error::*;

/// Error during data operations type inference.
#[derive(Debug)]
pub enum TypeError {
    /// Error during addition between Rust type (first argument, as string) and `FieldType`
    Add(String, FieldType),
    /// Error during subtraction between Rust type (first argument, as string) and `FieldType`
    Sub(String, FieldType),
    /// Error during multiplication between Rust type (first argument, as string) and `FieldType`
    Mul(String, FieldType),
    /// Error during division between Rust type (first argument, as string) and `FieldType`
    Div(String, FieldType),
}
impl fmt::Display for TypeError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            TypeError::Add(ref s, ft) => write!(f,
                "unable to add value of type {} to field of type {}", s, ft),
            TypeError::Sub(ref s, ft) => write!(f,
                "unable to subtract value of type {} from field of type {}", s, ft),
            TypeError::Mul(ref s, ft) => write!(f,
                "unable to multiply field of type {} by value of type {}", ft, s),
            TypeError::Div(ref s, ft) => write!(f,
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
    Add(FieldType, FieldType),
    /// Error during subtraction between two `FieldType`s
    Sub(FieldType, FieldType),
    /// Error during multiplication between two `FieldType`s
    Mul(FieldType, FieldType),
    /// Error during division between two `FieldType`s
    Div(FieldType, FieldType),
}
impl fmt::Display for FieldTypeError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            FieldTypeError::Add(left, right) => write!(f,
                "unable to add field of type {} to field of type {}", left, right),
            FieldTypeError::Sub(left, right) => write!(f,
                "unable to subtract field of type {} from field of type {}", right, left),
            FieldTypeError::Mul(left, right) => write!(f,
                "unable to multiply field of type {} by field of type {}", left, right),
            FieldTypeError::Div(left, right) => write!(f,
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
