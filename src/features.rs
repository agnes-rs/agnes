use std::marker::PhantomData;

use access::DataIndex;
use cons::*;
use fieldlist::*;
use label::{LVCons, SelfValued, TypedValue, Valued};
use select::FieldSelect;
use view::DataIndexCons;

#[derive(Debug, Clone)]
pub struct Implemented;
#[derive(Debug, Clone)]
pub struct Unimplemented;

#[derive(Debug, Clone)]
pub struct Capabilities<DType, Feature, IsImpl> {
    _marker: PhantomData<(DType, Feature, IsImpl)>,
}

#[derive(Debug, Clone)]
pub struct StorageCapabilities<DType, DI, Feature, IsImpl>
where
    DI: DataIndex<DType = DType>,
{
    _marker: PhantomData<Capabilities<DType, Feature, IsImpl>>,
    pub data: DI,
}
impl<'a, DType, DI, Feature, IsImpl> SelfValued for StorageCapabilities<DType, DI, Feature, IsImpl> where
    DI: DataIndex<DType = DType>
{
}

// pub type FieldCapabilitiesCons<Label, DType, Feature, IsImpl, Tail>
//     = LMCons<Label, Capabilities<DType, Feature, IsImpl>, Tail>;
pub type StorageCapabilitiesCons<Label, DType, DI, Feature, IsImpl, Tail> =
    LVCons<Label, StorageCapabilities<DType, DI, Feature, IsImpl>, Tail>;

pub trait IsImplemented<Feature> {
    type IsImpl;
}

pub type IsCapable<DType, Feature> =
    Capabilities<DType, Feature, <DType as IsImplemented<Feature>>::IsImpl>;

pub trait PartialMap<F> {
    type Output;
    fn map(&self, f: &mut F) -> Self::Output;
}
impl<'a, F> PartialMap<F> for Nil {
    type Output = Nil;
    fn map(&self, _f: &mut F) -> Nil {
        Nil
    }
}
impl<'a, Label, DType, DI, Feature, Tail, F> PartialMap<F>
    for StorageCapabilitiesCons<Label, DType, DI, Feature, Implemented, Tail>
// for FieldCapabilitiesCons<Label, DType, Feature, Implemented, Tail>
where
    Tail: PartialMap<F>,
    F: Func<DType>,
    // DType: Debug,
    DI: DataIndex<DType = DType>,
    // &'a DI: DataIndex<DType=DType>

    // Labeled<Label, TypedValue<DType,
    //   MarkerValue<Capabilities<DType, Feature, Implemented>, Payload>>>: Valued,

    // S: FieldSelect,
    // for<'a> S: SelectFieldByLabel<Label>,
    // for<'a> <S as SelectFieldByLabel<Label>>::Output: DataIndex<DType=DType>
{
    type Output = FieldPayloadCons<Label, DType, F::Output, Tail::Output>;

    fn map(&self, f: &mut F) -> Self::Output {
        FieldPayloadCons {
            head: TypedValue::from(f.call(&self.head.value_ref().data)).into(),
            tail: self.tail.map(f),
        }
    }
}
impl<'a, Label, DType, DI, Feature, Tail, F> PartialMap<F>
    for StorageCapabilitiesCons<Label, DType, DI, Feature, Unimplemented, Tail>
// for FieldCapabilitiesCons<Label, DType, Feature, Unimplemented, Tail>
where
    Tail: PartialMap<F>,
    DI: DataIndex<DType = DType>,
    F: FuncDefault,
{
    type Output = FieldPayloadCons<Label, DType, F::Output, Tail::Output>;

    fn map(&self, f: &mut F) -> Self::Output {
        FieldPayloadCons {
            head: TypedValue::from(f.call()).into(),
            tail: self.tail.map(f),
        }
    }
}

pub trait Func<DType> {
    type Output;
    fn call<DI>(&mut self, data: &DI) -> Self::Output
    where
        DI: DataIndex<DType = DType>;
}

pub trait FuncDefault {
    type Output;
    fn call(&mut self) -> Self::Output;
}

// pub trait ReqFeature {
//     type Feature;
// }
// pub type ReqFeatureOf<T> = <T as ReqFeature>::Feature;

pub trait DeriveCapabilities<F> {
    type Output: PartialMap<F>;
    fn derive(self) -> Self::Output;
}
impl<F> DeriveCapabilities<F> for Nil {
    type Output = Nil;
    fn derive(self) -> Nil {
        Nil
    }
}
impl<Label, DType, DI, Tail, F> DeriveCapabilities<F> for DataIndexCons<Label, DType, DI, Tail>
where
    //Label: 'a,
    Tail: DeriveCapabilities<F>,
    DI: DataIndex<DType = DType> + SelfValued,
    DType: IsImplemented<F>,
    StorageCapabilitiesCons<
        Label,
        DType,
        DI,
        F,
        <DType as IsImplemented<F>>::IsImpl,
        <Tail as DeriveCapabilities<F>>::Output,
    >: PartialMap<F>,
{
    type Output = StorageCapabilitiesCons<
        Label,
        DType,
        DI,
        F,
        <DType as IsImplemented<F>>::IsImpl,
        <Tail as DeriveCapabilities<F>>::Output,
    >;
    fn derive(self) -> Self::Output {
        LVCons {
            head: StorageCapabilities {
                data: self.head.value(),
                _marker: PhantomData,
            }
            .into(),
            tail: self.tail.derive(),
        }
    }
}

// pub type DerivePartialMap<Fields, Func>
//     = <Fields as DeriveCapabilities<'a, <Func as ReqFeature>::Feature>>::Output;

// pub trait Derivable<Fields, Func>
//     where Func: ReqFeature,
//           Fields: DeriveCapabilities<<Func as ReqFeature>::Feature> {}
// impl<T, Fields, Func> Derivable<Fields, Func> for T
//     where Func: ReqFeature,
//           Fields: DeriveCapabilities<<Func as ReqFeature>::Feature> {}

// #[macro_export]
// macro_rules! partial_map {
//     ($fields:ty, $func:ty) => {{
//         DerivePartialMap<Fields, Func><
//             <
//                 $fields as $crate::features::DeriveCapabilities<
//                     <$func as $crate::features::ReqFeature>::Feature
//                 >
//             >::Output as $crate::features::PartialMap<$func>
//         >::map(<$func>::default())
//     }}
// }

// #[derive(Debug, Clone)]
// pub struct DisplayFeat;
