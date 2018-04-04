//! Field-level structs.

use std::fmt;
use std::hash::{Hash, Hasher};

use csv_sniffer;

/// Identifier for a field in the source.
#[derive(Debug, Clone)]
pub enum FieldIdent {
    /// Unnamed field identifier, using the field index in the source file.
    Index(usize),
    /// Field name in the source file
    Name(String)
}
impl FieldIdent {
    /// Produce a string representation of the field identifier. Either the name if
    /// of the `FieldIdent::Name` variant, or the string "Field #" if using the `FieldIdent::Index`
    /// variant.
    pub fn to_string(&self) -> String {
        match *self {
            FieldIdent::Index(i) => format!("Field {}", i),
            FieldIdent::Name(ref s) => s.clone(),
        }
    }
}
impl fmt::Display for FieldIdent {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "{}", self.to_string())
    }
}
impl PartialEq for FieldIdent {
    fn eq(&self, other: &FieldIdent) -> bool {
        self.to_string().eq(&other.to_string())
    }
}
impl Eq for FieldIdent {}
impl Hash for FieldIdent {
    fn hash<H>(&self, state: &mut H) where H: Hasher {
        self.to_string().hash(state)
    }
}

impl From<usize> for FieldIdent {
    fn from(src: usize) -> FieldIdent {
        FieldIdent::Index(src)
    }
}
impl<'a> From<&'a str> for FieldIdent {
    fn from(src: &'a str) -> FieldIdent {
        FieldIdent::Name(src.to_string())
    }
}
impl From<String> for FieldIdent {
    fn from(src: String) -> FieldIdent {
        FieldIdent::Name(src)
    }
}

/// Valid field types
#[derive(Debug, Copy, Clone, PartialEq, Serialize, Deserialize)]
pub enum FieldType {
    /// Unsigned integer field
    Unsigned,
    /// Signed integer field
    Signed,
    /// Text (string) field
    Text,
    /// Boolean (yes/no) field
    Boolean,
    /// Floating-point field
    Float
}
impl From<csv_sniffer::Type> for FieldType {
    fn from(orig: csv_sniffer::Type) -> FieldType {
        match orig {
            csv_sniffer::Type::Unsigned => FieldType::Unsigned,
            csv_sniffer::Type::Signed   => FieldType::Signed,
            csv_sniffer::Type::Text     => FieldType::Text,
            csv_sniffer::Type::Boolean  => FieldType::Boolean,
            csv_sniffer::Type::Float     => FieldType::Float,
        }
    }
}

/// Possible-renamed field identifier
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RFieldIdent {
    /// Original field identifier
    pub ident: FieldIdent,
    /// Renamed name (if exists)
    pub rename: Option<String>,
}
impl RFieldIdent {
    /// Produce a string representation of this `RFieldIdent`. Uses the renamed name (if exists),
    /// of the result of `to_string` on the underlying `FieldIdent`.
    pub fn to_string(&self) -> String {
        self.rename.clone().unwrap_or(self.ident.to_string())
    }
    pub fn to_renamed_field_ident(&self) -> FieldIdent {
        match self.rename {
            Some(ref renamed) => FieldIdent::Name(renamed.clone()),
            None              => self.ident.clone()
        }
    }
}

/// Field identifier along with an associated type.
#[derive(Debug, Clone)]
pub struct TypedFieldIdent {
    /// Field identifier (name or original column number)
    pub ident: FieldIdent,
    /// Field type
    pub ty: FieldType
}
impl TypedFieldIdent {
    /// Create a new typed field identifier
    pub fn new(ident: FieldIdent, ty: FieldType) -> TypedFieldIdent {
        TypedFieldIdent {
            ident: ident,
            ty: ty
        }
    }
}

/// Specification of a typed field identifier along with the index in the original source data file.
#[derive(Debug, Clone)]
pub struct SrcField {
    /// Field identifier and type
    pub ty_ident: TypedFieldIdent,
    /// Index of field within the original data file
    pub src_index: usize,
}
impl SrcField {
    /// Create a new `SrcField` object from specified field identifier, type, and source index.
    pub fn new(ident: FieldIdent, ty: FieldType, src_index: usize) -> SrcField {
        SrcField {
            ty_ident: TypedFieldIdent::new(ident, ty),
            src_index: src_index
        }
    }
    /// Create a new `SrcField` object from specified typed field identifier obejct ans source
    /// index.
    pub fn from_ty_ident(ty_ident: TypedFieldIdent, src_index: usize) -> SrcField {
        SrcField {
            ty_ident: ty_ident,
            src_index: src_index
        }
    }
}

/// Details of a field within a data store
#[derive(Debug, Clone)]
pub struct DsField {
    /// Field identifier and type
    pub ty_ident: TypedFieldIdent,
    /// Index of field within the data store
    pub ds_index: usize,
}
impl DsField {
    /// Create a new `DsField` from field identifier, type, and data store index
    pub fn new(ident: FieldIdent, ty: FieldType, ds_index: usize) -> DsField {
        DsField {
            ty_ident: TypedFieldIdent::new(ident, ty),
            ds_index: ds_index,
        }
    }
    /// Create a new `DsField` from a typed field identifier and a data store index
    pub fn from_typed_field_ident(ty_ident: TypedFieldIdent, ds_index: usize) -> DsField {
        DsField {
            ty_ident: ty_ident,
            ds_index: ds_index,
        }
    }
    /// Create a new `DsField` from a `SrcField` object and data store index. The source index from
    /// the `SrcField` object will not be included in the new object.
    pub fn from_src(src: &SrcField, ds_index: usize) -> DsField {
        DsField {
            ty_ident: src.ty_ident.clone(),
            ds_index: ds_index
        }
    }
}

#[macro_export]
macro_rules! fields {
    ($($name:expr => $ty:expr),*) => {{
        use $crate::field::TypedFieldIdent;

        vec![$(
            TypedFieldIdent::new(
                FieldIdent::Name($name.to_string()),
                $ty
            )
        ),*]
    }}
}
