use std::cmp::Ordering;

use access::DataIndex;
use data_types::{DTypeList, DataType, Func};
use field::Value;

pub fn sort_order<DTypes, T>(data: &dyn DataIndex<DTypes, DType=T>) -> Vec<usize>
    where DTypes: DTypeList,
          T: DataType<DTypes> + DtOrd
{
    let mut order = (0..data.len()).collect::<Vec<_>>();
    order.sort_unstable_by(|&left, &right| {
        // a, b are always in range, so unwraps are safe
        data.get_datum(left).unwrap().dt_cmp(&data.get_datum(right).unwrap())
    });
    order
}

// ordering is (arbitrarily) going to be:
// NA values, followed by everything else ascending
pub trait DtOrd {
    fn dt_cmp(&self, other: &Self) -> Ordering;
}

macro_rules! impl_dtordered {
    ($($dtype:ty)*) => {$(

impl DtOrd for $dtype {
    fn dt_cmp(&self, other: &$dtype) -> Ordering { self.cmp(other) }
}

    )*}
}
impl_dtordered!(u8 u16 u32 u64 u128 i8 i16 i32 i64 i128 usize isize String bool);

macro_rules! impl_float_dtordered {
    ($($dtype:ty)*) => {$(

impl DtOrd for $dtype {
    fn dt_cmp(&self, other: &$dtype) -> Ordering {
        self.partial_cmp(other).unwrap_or_else(|| {
            if self.is_nan() && !other.is_nan() {
                Ordering::Less
            } else {
                // since partial_cmp only fails for NAN, then !self.is_nan() && other.is_nan()
                Ordering::Greater
            }
        })
    }
}

    )*}
}
impl_float_dtordered![f32 f64];

impl<T> DtOrd for Value<T> where T: DtOrd {
    fn dt_cmp(&self, other: &Value<T>) -> Ordering {
        match (self, other){
            (Value::Na, Value::Na) => Ordering::Equal,
            (Value::Na, Value::Exists(_)) => Ordering::Less,
            (Value::Exists(_), Value::Na) => Ordering::Greater,
            (Value::Exists(ref left), Value::Exists(ref right)) => left.dt_cmp(right)
        }
    }
}

impl<'a, T> DtOrd for &'a T where T: DtOrd + ?Sized {
    fn dt_cmp(&self, other: &&'a T) -> Ordering {
        DtOrd::dt_cmp(*self, *other)
    }
}

pub struct SortOrderFn;
impl<DTypes, T> Func<DTypes, T> for SortOrderFn
    where DTypes: DTypeList,
          T: 'static + Default + DtOrd + DataType<DTypes>,
{
    type Output = Vec<usize>;
    fn call(
        &mut self,
        type_data: &dyn DataIndex<DTypes, DType=T>,
    ) -> Vec<usize>
    {
        sort_order(type_data)
    }
}
