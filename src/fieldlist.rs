use std::fmt;
use std::marker::PhantomData;

// use typenum::{Unsigned, U0, B1};

use cons::*;
// use data_types::{DTypeList, GetDType};


#[derive(Debug, Clone)]
pub struct Field<Ident, DType> {
    _ident: PhantomData<Ident>,
    _dtype: PhantomData<DType>,
}

pub trait FieldTypes {
    type Ident;
    type DType;
}
impl<Ident, DType> FieldTypes for Field<Ident, DType> {
    type Ident = Ident;
    type DType = DType;
}

#[derive(Debug, Clone)]
pub struct FieldPayload<Field, Payload> {
    _field: PhantomData<Field>,
    pub payload: Payload
}
impl<Field, Payload> From<Payload> for FieldPayload<Field, Payload>
{
    fn from(payload: Payload) -> FieldPayload<Field, Payload> {
        FieldPayload {
            _field: PhantomData,
            payload
        }
    }
}

#[derive(Debug, Clone)]
pub struct FieldMarker<Field, Marker> {
    _field: PhantomData<Field>,
    _marker: PhantomData<Marker>
}

pub type FieldCons<Ident, DType, Tail> = Cons<Field<Ident, DType>, Tail>;
pub type FieldPayloadCons<Field, Payload, Tail> = Cons<FieldPayload<Field, Payload>, Tail>;

impl<Ident, DType, Tail> FieldTypes for FieldCons<Ident, DType, Tail>
{
    type Ident = Ident;
    type DType = DType;
}
impl FieldTypes for Nil
{
    type Ident = ();
    type DType = ();
}

pub trait AssocField {
    type Field;
}
impl<Ident, DType, Tail> AssocField for FieldCons<Ident, DType, Tail> {
    type Field = Field<Ident, DType>;
}
impl<Field, Payload, Tail> AssocField for FieldPayloadCons<Field, Payload, Tail> {
    type Field = Field;
}

// pub trait AssocFieldCons {
//     type Fields;
// }
// impl AssocFieldCons for Nil {
//     type Fields = Nil;
// }
// impl<Ident, DType, Tail> AssocFieldCons
//     for FieldCons<Ident, DType, Tail>
// {
//     type Fields = Self;
// }
// impl<Ident, DType, Payload, Tail> AssocFieldCons
//     for FieldPayloadCons<Field<Ident, DType>, Payload, Tail>
//     where Tail: AssocFieldCons,
// {
//     type Fields = FieldCons<Ident, DType, Tail::Fields>;
// }


#[derive(Debug, Clone)]
pub enum FieldDesignator {
    Expr(String),
    Idx(usize),
}

pub type SpecCons<Field, Tail> = FieldPayloadCons<Field, FieldDesignator, Tail>;

impl<Field, Tail> SpecCons<Field, Tail> {
    pub fn new(src_designator: FieldDesignator, tail: Tail) -> SpecCons<Field, Tail>
    {
        SpecCons {
            head: FieldPayload {
                _field: PhantomData,
                payload: src_designator,
            },
            tail
        }
    }
}

#[macro_export]
macro_rules! spec {
    // general end point
    (@step ) => {{
        $crate::cons::Nil
    }};

    // end points without trailing comma
    (@step $field_ident:ident($field_name:expr): $field_ty:ty) => {{
        use $crate::fieldlist::{FieldDesignator, SpecCons, Field};
        SpecCons::<
            Field<
                $field_ident,
                $field_ty
            >,
            _
        >::new(
            FieldDesignator::Expr($field_name.to_string()),
            spec![@step ]
        )
    }};
    (@step $field_ident:ident[$src_field_idx:expr]: $field_ty:ty) => {{
        use $crate::fieldlist::{FieldDesignator, SpecCons, Field};
        SpecCons::<
            Field<
                $field_ident,
                $field_ty,
            >,
            _
        >::new(
            FieldDesignator::Idx($src_field_idx),
            spec![@step ]
        )
    }};

    // entry point / main recursion loop
    (@step $field_ident:ident($field_name:expr): $field_ty:ty, $($rest:tt)*) => {{
        use $crate::fieldlist::{FieldDesignator, SpecCons, Field};
        SpecCons::<
            Field<
                $field_ident,
                $field_ty
            >,
            _
        >::new(
            FieldDesignator::Expr($field_name.to_string()),
            spec![@step $($rest)*]
        )
    }};
    (@step $field_ident:ident[$src_field_idx:expr]: $field_ty:ty, $($rest:tt)*) => {{
        use $crate::fieldlist::{FieldDesignator, SpecCons, Field};
        SpecCons::<
            Field<
                $field_ident,
                $field_ty
            >,
            _
        >::new(
            FieldDesignator::Idx($src_field_idx),
            spec![@step $($rest)*]
        )
    }};
    (@start $($body:tt)*) => {{
        spec![@step $($body)*]
    }};

    (@decl_fields ) => {};
    (@decl_fields $field_ident:ident($field_name:expr): $field_ty:ty) => {
        #[derive(Debug)]
        struct $field_ident;
        // impl $crate::fieldlist::Position for $field_ident {
        //     type Pos = ::typenum::Add1<$pos>;
        // }
        spec![@decl_fields];
    };
    (@decl_fields $field_ident:ident[$field_name:ident]: $field_ty:ty) => {
        #[derive(Debug)]
        struct $field_ident;
        // impl $crate::fieldlist::Position for $field_ident {
        //     type Pos = ::typenum::Add1<$pos>;
        // }
        spec![@decl_fields];
    };
    (@decl_fields $field_ident:ident($field_name:expr): $field_ty:ty, $($rest:tt)*) => {
        #[derive(Debug)]
        struct $field_ident;
        // impl $crate::fieldlist::Position for $field_ident {
        //     type Pos = ::typenum::Add1<$pos>;
        // }
        spec![@decl_fields $($rest)*];
    };
    (@decl_fields $field_ident:ident[$field_name:ident]: $field_ty:ty, $($rest:tt)*) => {
        #[derive(Debug)]
        struct $field_ident;
        // impl $crate::fieldlist::Position for $field_ident {
        //     type Pos = ::typenum::Add1<$pos>;
        // }
        spec![@decl_fields $($rest)*];
    };
    // (@decl_fields $($body:tt)*) => {
    //     spec![@decl_fields $($body)*];
    // };

    (let $spec:ident = { $($body:tt)* };) => {
        spec![@decl_fields $($body)*];
        let $spec = spec![@start $($body)*];
    };

}

#[derive(Debug, Clone)]
pub struct Match;
#[derive(Debug, Clone)]
pub struct NoMatch<Next> {
    _marker: PhantomData<Next>,
}

pub trait FSelector<Ident, Searcher> {
    type DType;
}
impl<TargetIdent, NonTargetIdent, TargetInTail, DType, Tail>
    FSelector<TargetIdent, NoMatch<TargetInTail>>
    for FieldCons<NonTargetIdent, DType, Tail>
    where Tail: FSelector<TargetIdent, TargetInTail>
{
    type DType = <Tail as FSelector<TargetIdent, TargetInTail>>::DType;
}
impl<TargetIdent, DType, Tail>
    FSelector<TargetIdent, Match>
    for FieldCons<TargetIdent, DType, Tail>
{
    type DType = DType;
}

impl<TargetIdent, NonTargetIdent, TargetInTail, DType, Payload, Tail>
    FSelector<TargetIdent, NoMatch<TargetInTail>>
    for FieldPayloadCons<Field<NonTargetIdent, DType>, Payload, Tail>
    where Tail: FSelector<TargetIdent, TargetInTail>
{
    type DType = <Tail as FSelector<TargetIdent, TargetInTail>>::DType;
}
impl<TargetIdent, DType, Payload, Tail> FSelector<TargetIdent, Match>
    for FieldPayloadCons<Field<TargetIdent, DType>, Payload, Tail>
{
    type DType = DType;
}

pub trait AttachPayload<Gen, DType>
{
    // type Field: FieldTypes<DType=DType>;
    type Output;

    fn attach_payload() -> Self::Output;
}
impl<Gen, DType> AttachPayload<Gen, DType> for Nil
{
    // type Field = Nil;
    type Output = Nil;

    fn attach_payload() -> Nil { Nil }
}
impl<Ident, DType, Tail, Gen> AttachPayload<Gen, DType>
    for FieldCons<Ident, DType, Tail>
    where Tail: AttachPayload<Gen, DType>,
          Gen: PayloadGenerator<DType>
{
    type Output = FieldPayloadCons<Field<Ident, DType>, Gen::Payload, Tail::Output>;

    fn attach_payload() -> Self::Output
    {
        FieldPayloadCons {
            head: FieldPayload {
                _field: PhantomData,
                payload: Gen::generate(),
            },
            tail: Tail::attach_payload()
        }
    }
}

pub trait PayloadGenerator<DType> {
    type Payload;

    fn generate() -> Self::Payload;
}
