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

/// Macro for creating a source specification structure used to specify how to
/// extract fields from a data source. It correlates labels (defined using the
/// [tablespace](macro.tablespace.html) macro) to field / column names or indices in a
/// data source. This source specification structure is implemented as a
//  [SpecCons](fieldlist/type.SpecCons.html) cons-list.
///
/// The `spec` macro syntax is a list of `fieldname` or `fieldindex` declarations that connect
/// field labels to either column titles or column indices (starting from 0), respectively.
///
/// # Examples
///
/// This example defines a source specification with three column names: the `CountryName` field
/// label will take data from the column with the "Country Name" header, the `CountryCode` field
/// label will take data from the 0th (first) column, and the `Gdp2015` field label will take
/// data from the column with the "2015" label.
///
/// This example also shows the usage of the [tablespace](macro.tablespace.html) macro; see that
/// macro's documentation for its syntax and an example.
///
/// ```
/// # #[macro_use] extern crate agnes;
///
/// tablespace![
///     table gdp {
///         CountryName: String,
///         CountryCode: String,
///         Gdp2015: f64,
///     }
/// ];
///
/// fn main() {
///     let gdp_spec = spec![
///         fieldname gdp::CountryName = "Country Name";
///         fieldname gdp::CountryCode = 0usize;
///         fieldname gdp::Gdp2015 = "2015";
///     ];
///     // ...
/// }
/// ```
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
