/*!
Framework for partial function handling (where some functionality is implemented for some but not
all of the data types of fields in a [DataView](../struct.DataView.html)). A specific piece of
partially-implemented functionality consists of a [Func](trait.Func.html) implementation for all
data types where the functionality exists, a [FuncDefault](trait.FuncDefault.html) implementation
for data types where the functionality doesn't exist, and [IsImplemented](trait.IsImplemented.html)
specification for all data types denoting whether to use the available `Func` or the fall-back
`FuncDefault`.

This module should be unnecessary once
[trait specialization](https://github.com/rust-lang/rust/issues/31844) is finalized.
*/
use std::marker::PhantomData;

use access::DataIndex;
use cons::*;
use fieldlist::*;
use label::{LVCons, SelfValued, TypedValue, Valued};
use view::{AssocDataIndexConsOf, DataIndexCons};

/// Marker struct denoting that a [Func](trait.Func.html) is implemented for a particular data type.
#[derive(Debug, Clone)]
pub struct Implemented;
/// Marker struct denoting that a [Func](trait.Func.html) is not implemented for a particular data
/// type.
#[derive(Debug, Clone)]
pub struct Unimplemented;

/// Marker struct for combination of a data type, [Func](trait.Func.html), and whether or not that
/// combination has an implementation.
#[derive(Debug, Clone)]
pub struct Capabilities<DType, Fun, IsImpl> {
    _marker: PhantomData<(DType, Fun, IsImpl)>,
}

/// Structure tracking a field (as accessed through a type that implements
/// [DataIndex](../access/trait.DataIndex.html)) along with its
/// [Capabilities](struct.Capabilities.html) details.
#[derive(Debug, Clone)]
pub struct StorageCapabilities<DType, DI, Fun, IsImpl>
where
    DI: DataIndex<DType = DType>,
{
    _marker: PhantomData<Capabilities<DType, Fun, IsImpl>>,
    data: DI,
}
impl<'a, DType, DI, Fun, IsImpl> SelfValued for StorageCapabilities<DType, DI, Fun, IsImpl> where
    DI: DataIndex<DType = DType>
{
}

/// A cons-list of [StorageCapabilities](struct.StorageCapabilities.html) structs.
pub type StorageCapabilitiesCons<Label, DType, DI, Fun, IsImpl, Tail> =
    LVCons<Label, StorageCapabilities<DType, DI, Fun, IsImpl>, Tail>;

/// Trait denoting whether a particular [Func](trait.Func.html) is implemented for a data type.
pub trait IsImplemented<Fun> {
    /// Marker trait (either [Implemented](struct.Implemented.html) or
    /// [Unimplemented](struct.Unimplemented.html)) specifying the implementation status for this
    /// function / data type combination.
    type IsImpl;
}

/// Trait for applying a partially-implemented function [Func](trait.Func.html) to a cons-list.
pub trait PartialMap<F> {
    /// The output of the function, constructed into a cons-list of function results.
    type Output;
    /// Apply the function `F` to the value in this element of a cons-list, and recurse.
    fn map(&self, f: &mut F) -> Self::Output;
}
impl<'a, F> PartialMap<F> for Nil {
    type Output = Nil;
    fn map(&self, _f: &mut F) -> Nil {
        Nil
    }
}
impl<'a, Label, DType, DI, Fun, Tail, F> PartialMap<F>
    for StorageCapabilitiesCons<Label, DType, DI, Fun, Implemented, Tail>
where
    Tail: PartialMap<F>,
    F: Func<DType>,
    DI: DataIndex<DType = DType>,
{
    type Output = FieldPayloadCons<Label, DType, F::Output, Tail::Output>;

    fn map(&self, f: &mut F) -> Self::Output {
        FieldPayloadCons {
            head: TypedValue::from(f.call(&self.head.value_ref().data)).into(),
            tail: self.tail.map(f),
        }
    }
}
impl<'a, Label, DType, DI, Fun, Tail, F> PartialMap<F>
    for StorageCapabilitiesCons<Label, DType, DI, Fun, Unimplemented, Tail>
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

/// Implementation of a function for a particular data type.
pub trait Func<DType> {
    /// Output of this function.
    type Output;
    /// Method to call this function on field data of data type `DType`.
    fn call<DI>(&mut self, data: &DI) -> Self::Output
    where
        DI: DataIndex<DType = DType>;
}

/// Default function implementation with no valid implementation exists.
pub trait FuncDefault {
    /// Output of this function.
    type Output;
    /// Method called when no [Func](trait.Func.html) implementation exists.
    fn call(&mut self) -> Self::Output;
}

/// Trait that augments a [DataIndexCons](../view/type.DataIndexCons.html) (a cons-list of
/// field access structs) with partial-function capability information as specified by
/// [IsImplemented](trait.IsImplemented.html) definitions.
pub trait DeriveCapabilities<F> {
    /// The augmented cons-list which implements [PartialMap](trait.PartialMap.html), allowing
    /// application of partially-implemented functions to a `DataView`.
    type Output: PartialMap<F>;

    /// Derive the capabilities of this cons-list.
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

/// Helper type alias that provides the derived capabilities of the
/// [DataIndexCons](../view/type.DataIndexCons.html) associated with a particular `Labels` /
/// `Frames` pair.
pub type DeriveCapabilitiesOf<Labels, Frames, F> =
    <AssocDataIndexConsOf<Labels, Frames> as DeriveCapabilities<F>>::Output;
