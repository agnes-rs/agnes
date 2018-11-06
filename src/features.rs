use std::marker::PhantomData;

use fieldlist::*;
use cons::*;

#[derive(Debug, Clone)]
pub struct Implemented;
#[derive(Debug, Clone)]
pub struct Unimplemented;

#[derive(Debug, Clone)]
pub struct Capabilities<DType, Feature, IsImpl>
{
    _dtype: PhantomData<DType>,
    _feature: PhantomData<Feature>,
    _is_impl: PhantomData<IsImpl>,
}

pub type FieldCapabilitiesCons<Field, Feature, IsImpl, Tail>
    = Cons<
        FieldMarker<Field, Capabilities<<Field as FieldTypes>::DType, Feature, IsImpl>>,
        Tail
    >;

pub trait IsImplemented<Feature> {
    type IsImpl;
}

pub type IsCapable<DType, Feature>
    = Capabilities<DType, Feature, <DType as IsImplemented<Feature>>::IsImpl>;

// pub type FieldFeatureCons<Ident, FIdx, DType, Feature, Tail>
//     = Cons<
//         FieldMarker<
//             Field<Ident, FIdx, DType>,
//             IsCapable<DType, Feature>
//         >,
//         Tail
//     >;

pub trait PartialMap<F>
{
    type Output;
    fn map(f: F) -> Self::Output;
}
impl<F> PartialMap<F> for Nil
{
    type Output = Nil;
    fn map(f: F) -> Nil { Nil }
}
impl<Field, Feature, Tail, F> PartialMap<F>
    for FieldCapabilitiesCons<Field, Feature, Implemented, Tail>
    where Tail: PartialMap<F>,
          Field: FieldTypes,
          F: Func<Field::DType>
{
    type Output = FieldPayloadCons<Field, F::Output, Tail::Output>;

    fn map(f: F) -> Self::Output
    {
        FieldPayloadCons {
            head: FieldPayload {
                _field: PhantomData,
                payload: f.call(),
            },
            tail: Tail::map()
        }
    }
}
impl<Field, Feature, Tail, F> PartialMap<F>
    for FieldCapabilitiesCons<Field, Feature, Implemented, Tail>
    where Tail: PartialMap<F>,
          F: FuncDefault
{
    type Output = FieldPayloadCons<Field, F::Output, Tail::Output>;

    fn map(f: F) -> Self::Output
    {
        FieldPayloadCons {
            head: FieldPayload {
                _field: PhantomData,
                payload: f.call(),
            },
            tail: Tail::map()
        }
    }

}

pub trait Func<DType> {
    type Output;
    fn call<Field>(&self) -> Self::Output
        where Field: FieldTypes;
}

pub trait FuncDefault {
    type Output;
    fn call<Field>(&self) -> Self::Output;
}

pub trait ReqFeature {
    type Feature;
}

pub trait DeriveCapabilities<Feature>
{
    type Output;
}
impl<Feature> DeriveCapabilities<Feature> for Nil {
    type Output = Nil;
}
impl<Ident, FIdx, DType, Tail, Feature> DeriveCapabilities<Feature>
    for FieldCons<Ident, FIdx, DType, Tail>
    where Tail: DeriveCapabilities<Feature>,
          DType: IsImplemented<Feature>
{
    type Output = Cons<
        FieldMarker<Field<Ident, FIdx, DType>, IsCapable<DType, Feature>>,
        Tail::Output
    >;
}

#[macro_export]
macro_rules! partial_map {
    ($fields:ty, $func:ty) => {{
        <
            <
                $fields as $crate::features::DeriveCapabilities<
                    <$func as $crate::features::ReqFeature>::Feature
                >
            >::Output as $crate::features::PartialMap<$func>
        >::map()
    }}
}

pub struct DisplayFeat;
