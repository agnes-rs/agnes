use std::marker::PhantomData;

// use typenum::{Unsigned, U0, B1};

use cons::*;
use label::*;
// use data_types::{DTypeList, GetDType};


// #[derive(Debug, Clone)]
// pub struct Field<Ident, DType> {
//     _ident: PhantomData<Ident>,
//     _dtype: PhantomData<DType>,
// }

// pub trait FieldTypes {
//     type Ident;
//     type DType;
// }
// impl<Ident, DType> FieldTypes for Field<Ident, DType> {
//     type Ident = Ident;
//     type DType = DType;
// }

#[derive(Debug, Clone)]
pub struct TypedPayload<DType, Payload> {
    _dtype: PhantomData<DType>,
    payload: Payload
}
impl<DType, Payload> From<Payload> for TypedPayload<DType, Payload>
{
    fn from(payload: Payload) -> TypedPayload<DType, Payload> {
        TypedPayload {
            _dtype: PhantomData,
            payload
        }
    }
}

// #[derive(Debug, Clone)]
// pub struct FieldMarker<Field, Marker> {
//     _field: PhantomData<Field>,
//     _marker: PhantomData<Marker>
// }

pub type FieldSpec<Label, DType> = Labeled<Label, PhantomData<DType>>;

/// Type alias for an `LVCons`-list which only contains the data type information for the identified
/// field.
pub type FieldCons<Label, DType, Tail> = LMCons<Label, DType, Tail>;
pub type FieldPayloadCons<Label, DType, Payload, Tail> = LDVCons<Label, DType, Payload, Tail>;
// pub type FieldPayloadMarkerCons<Label, DType, Payload, Marker, Tail>
//     = LDMVCons<Label, DType, Marker, Payload, Tail>;

// impl<Ident, DType, Tail> FieldTypes for FieldCons<Ident, DType, Tail>
// {
//     type Ident = Ident;
//     type DType = DType;
// }
// impl FieldTypes for Nil
// {
//     type Ident = ();
//     type DType = ();
// }

// pub trait AssocField {
//     type Field;
// }
// impl<Label, DType, Tail> AssocField for FieldCons<Label, DType, Tail> {
//     type Field = Field<Label, DType>;
// }
// impl<Field, Payload, Tail> AssocField for FieldPayloadCons<Field, Payload, Tail> {
//     type Field = Field;
// }

// pub trait AssocFieldCons
// {
//     type Fields;
// }
// impl AssocFieldCons for Nil
// {
//     type Fields = Nil;
// }
// impl<Label, DType, Tail> AssocFieldCons
//     for FieldCons<Label, DType, Tail>
// {
//     type Fields = Self;
// }
// impl<Label, DType, Payload, Tail> AssocFieldCons
//     for FieldPayloadCons<Label, DType, Payload, Tail>
//     where Tail: AssocFieldCons,
// {
//     type Fields = FieldCons<Label, DType, Tail::Fields>;
// }


#[derive(Debug, Clone)]
pub enum FieldDesignator {
    Expr(String),
    Idx(usize),
}
impl SelfValued for FieldDesignator {}

pub type SpecCons<Label, DType, Tail> = FieldPayloadCons<Label, DType, FieldDesignator, Tail>;

impl<Label, DType, Tail> SpecCons<Label, DType, Tail>
{
    pub fn new(src_designator: FieldDesignator, tail: Tail) -> SpecCons<Label, DType, Tail>
    {
        SpecCons
        {
            head: TypedValue::from(src_designator).into(),
            tail
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
            FieldDesignator:Idx($idx),
            spec![$($rest)*]
        )
    }};
}

#[macro_export]
macro_rules! spec_old {
    // general end point
    (@step ) => {{
        $crate::cons::Nil
    }};

    // end points without trailing comma
    (@step $field_label:ident($src_field_name:expr): $field_ty:ty) => {{
        use $crate::fieldlist::{FieldDesignator, SpecCons};
        SpecCons::<
            $field_label,
            $field_ty,
            _
        >::new(
            FieldDesignator::Expr($src_field_name.to_string()),
            spec![@step ]
        )
    }};
    (@step $field_label:ident[$src_field_idx:expr]: $field_ty:ty) => {{
        use $crate::fieldlist::{FieldDesignator, SpecCons};
        SpecCons::<
            $field_label,
            $field_ty,
            _
        >::new(
            FieldDesignator::Idx($src_field_idx),
            spec![@step ]
        )
    }};

    // entry point / main recursion loop
    (@step $field_label:ident($src_field_name:expr): $field_ty:ty, $($rest:tt)*) => {{
        use $crate::fieldlist::{FieldDesignator, SpecCons};
        SpecCons::<
            $field_label,
            $field_ty,
            _
        >::new(
            FieldDesignator::Expr($src_field_name.to_string()),
            spec![@step $($rest)*]
        )
    }};
    (@step $field_label:ident[$src_field_idx:expr]: $field_ty:ty, $($rest:tt)*) => {{
        use $crate::fieldlist::{FieldDesignator, SpecCons};
        SpecCons::<
            $field_label,
            $field_ty,
            _
        >::new(
            FieldDesignator::Idx($src_field_idx),
            spec![@step $($rest)*]
        )
    }};
    (@start $($body:tt)*) => {{
        spec![@step $($body)*]
    }};

    (@decl_fields($pos:ty) ) => {};
    (@decl_fields($pos:ty) $field_label:ident($src_field_name:expr): $field_ty:ty) => {
        nat_label![$field_label, ::typenum::U0, $pos];
        spec![@decl_fields(::typenum::Add1<$pos>)];
    };
    (@decl_fields($pos:ty) $field_label:ident[$src_field_name:ident]: $field_ty:ty) => {
        nat_label![$field_label, ::typenum::U0, $pos];
        spec![@decl_fields(::typenum::Add1<$pos>)];
    };
    (@decl_fields($pos:ty) $field_label:ident($src_field_name:expr): $field_ty:ty, $($rest:tt)*) => {
        nat_label![$field_label, ::typenum::U0, $pos];
        spec![@decl_fields(::typenum::Add1<$pos>) $($rest)*];
    };
    (@decl_fields($pos:ty) $field_label:ident[$src_field_name:ident]: $field_ty:ty, $($rest:tt)*) => {
        nat_label![$field_label, ::typenum::U0, $pos];
        spec![@decl_fields(::typenum::Add1<$pos>) $($rest)*];
    };
    (@decl_fields $($body:tt)*) => {
        spec![@decl_fields(::typenum::consts::U0) $($body)*];
    };

    (let $spec:ident = { $($body:tt)* };) => {
        spec![@decl_fields $($body)*];
        let $spec = spec![@start $($body)*];
    };

}

// #[derive(Debug, Clone)]
// pub struct Match;
// #[derive(Debug, Clone)]
// pub struct NoMatch<Next> {
//     _marker: PhantomData<Next>,
// }

// pub trait FSelector<Ident, Searcher> {
//     type DType;
// }
// impl<TargetIdent, NonTargetIdent, TargetInTail, DType, Tail>
//     FSelector<TargetIdent, NoMatch<TargetInTail>>
//     for FieldCons<NonTargetIdent, DType, Tail>
//     where Tail: FSelector<TargetIdent, TargetInTail>
// {
//     type DType = <Tail as FSelector<TargetIdent, TargetInTail>>::DType;
// }
// impl<TargetIdent, DType, Tail>
//     FSelector<TargetIdent, Match>
//     for FieldCons<TargetIdent, DType, Tail>
// {
//     type DType = DType;
// }

// impl<TargetIdent, NonTargetIdent, TargetInTail, DType, Payload, Tail>
//     FSelector<TargetIdent, NoMatch<TargetInTail>>
//     for FieldPayloadCons<Field<NonTargetIdent, DType>, Payload, Tail>
//     where Tail: FSelector<TargetIdent, TargetInTail>
// {
//     type DType = <Tail as FSelector<TargetIdent, TargetInTail>>::DType;
// }
// impl<TargetIdent, DType, Payload, Tail> FSelector<TargetIdent, Match>
//     for FieldPayloadCons<Field<TargetIdent, DType>, Payload, Tail>
// {
//     type DType = DType;
// }

pub trait AttachPayload<Gen, DType>
{
    type Output;

    fn attach_payload() -> Self::Output;
}
impl<Gen, DType> AttachPayload<Gen, DType> for Nil
{
    type Output = Nil;

    fn attach_payload() -> Nil { Nil }
}
impl<Label, DType, Tail, Gen> AttachPayload<Gen, DType>
    for FieldCons<Label, DType, Tail>
    where Tail: AttachPayload<Gen, DType>,
          Gen: PayloadGenerator<DType>
{
    type Output = FieldPayloadCons<Label, DType, Gen::Payload, Tail::Output>;

    fn attach_payload() -> Self::Output
    {
        FieldPayloadCons {
            head: TypedValue::from(Gen::generate()).into(),
            tail: Tail::attach_payload()
        }
    }
}

pub trait PayloadGenerator<DType> {
    type Payload;

    fn generate() -> Self::Payload;
}
