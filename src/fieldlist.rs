use std::marker::PhantomData;

use label::*;

pub type FieldSpec<Label, DType> = Labeled<Label, PhantomData<DType>>;

/// Type alias for an `LVCons`-list which only contains the data type information for the identified
/// field.
pub type FieldCons<Label, DType, Tail> = LMCons<Label, DType, Tail>;
pub type FieldPayloadCons<Label, DType, Payload, Tail> = LDVCons<Label, DType, Payload, Tail>;

#[derive(Debug, Clone)]
pub enum FieldDesignator {
    Expr(String),
    Idx(usize),
}
impl SelfValued for FieldDesignator {}

pub type SpecCons<Label, DType, Tail> = FieldPayloadCons<Label, DType, FieldDesignator, Tail>;

impl<Label, DType, Tail> SpecCons<Label, DType, Tail> {
    pub fn new(src_designator: FieldDesignator, tail: Tail) -> SpecCons<Label, DType, Tail> {
        SpecCons {
            head: TypedValue::from(src_designator).into(),
            tail,
        }
    }
}

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
