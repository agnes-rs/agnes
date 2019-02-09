/*!
Type aliases and macro for handling specifications of fields in a data source.
*/
use std::marker::PhantomData;

use label::*;

/// Type alias for a field label and data type.
pub type FieldSpec<Label, DType> = Labeled<Label, PhantomData<DType>>;

/// Type alias for an `LVCons`-list which only contains the data type information for the identified
/// field.
pub type FieldCons<Label, DType, Tail> = LMCons<Label, DType, Tail>;
/// Type alias for a cons-list containing a labeled field, it's data type, and an arbitrary payload.
pub type FieldPayloadCons<Label, DType, Payload, Tail> = LDVCons<Label, DType, Payload, Tail>;

/// Designation of a field in a data source -- either a field name or a field index.
#[derive(Debug, Clone)]
pub enum FieldDesignator {
    /// Field / column name in data source.
    Expr(String),
    /// Field / column index in data source.
    Idx(usize),
}
impl SelfValued for FieldDesignator {}

/// Type alias for a cons-list containing fields with their labels, data type, and source
/// designators.
pub type SpecCons<Label, DType, Tail> = FieldPayloadCons<Label, DType, FieldDesignator, Tail>;

impl<Label, DType, Tail> SpecCons<Label, DType, Tail> {
    /// Create a new `SpecCons` cons-list from a [FieldDesignator](enum.FieldDesignator.html).
    pub fn new(src_designator: FieldDesignator, tail: Tail) -> SpecCons<Label, DType, Tail> {
        SpecCons {
            head: TypedValue::from(src_designator).into(),
            tail,
        }
    }
}

//TODO: finish this example
/// Macro for creating a [SpecCons](type.SpecCons.html) cons-list to specify how to extract fields
/// from a data source. Correlates labels (defined using the
/// [namespace](../label/macro.namespace.html) macro) to field / column names or indices in a
/// data source.
///
/// # Examples
/// let gdp_spec = spec![
///     fieldname gdp::CountryName = "Country Name";
///     fieldname gdp::CountryCode = "Country Code";
///     fieldname gdp::Year1983 = "1983";
/// ];
///
/// let gdp_metadata_spec = spec![
///     fieldindex gdp_metadata::CountryCode = 0usize;
///     fieldname gdp_metadata::Region = "Region";
/// ];
#[macro_export]
macro_rules! spec {
    () => {{
        $crate::cons::Nil
    }};
    (fieldname $field_label:ty = $header:expr; $($rest:tt)*) => {{
        use $crate::fieldlist::{FieldDesignator, SpecCons};
        SpecCons::<
            $field_label,
            <$field_label as $crate::label::Typed>::DType,
            _,
        >::new(
            FieldDesignator::Expr($header.to_string()),
            spec![$($rest)*]
        )
    }};
    (fieldindex $field_label:ty = $idx:expr; $($rest:tt)*) => {{
        use $crate::fieldlist::{FieldDesignator, SpecCons};
        SpecCons::<
            $field_label,
            <$field_label as $crate::label::Typed>::DType,
            _,
        >::new(
            FieldDesignator::Idx($idx),
            spec![$($rest)*]
        )
    }};
}
