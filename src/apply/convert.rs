/*!
Conversion functions for converting between types of within `agnes` data structures.
*/

use std::marker::PhantomData;

use access::{DataIterator, DataIndex};
use data_types::{Func, DataType, DTypeList};
use field::{FieldData};


/// User-implementable conversion trait for converting between datatypes.
pub trait DtFrom<T> {
    /// Convert into this type.
    fn dt_from(orig: T) -> Self;
}
impl<U, T> DtFrom<T> for U where U: From<T> {
    fn dt_from(orig: T) -> U {
        U::from(orig)
    }
}

/// Function (implementing [Func](../../data_types/trait.Func.html)) converting the values of a
/// field into a new field of type `Target`.
pub struct ConvertFn<Target> {
    _marker: PhantomData<Target>
}
impl<Target> Default for ConvertFn<Target> {
    fn default() -> ConvertFn<Target> {
        ConvertFn {
            _marker: PhantomData,
        }
    }
}
impl<DTypes, T, Target> Func<DTypes, T> for ConvertFn<Target>
    where DTypes: DTypeList,
          T: DataType<DTypes> + Clone,
          Target: DataType<DTypes> + Clone + Default,
          Target: DtFrom<T>,
{
    type Output = FieldData<DTypes, Target>;

    fn call(
        &mut self,
        data: &dyn DataIndex<DTypes, DType=T>,
    )
        -> FieldData<DTypes, Target>
    {
        DataIterator::new(data)
            .map(|maybe_na| maybe_na.map(|value| Target::dt_from(value.clone())))
            .collect()
    }
}

//TODO: this is all untested! tests needed.
