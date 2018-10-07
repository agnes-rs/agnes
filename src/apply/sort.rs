use std::cmp::Ordering;

use access::DataIndex;
use data_types::{DTypeList, DataType, Func};
use field::Value;
// use apply::mapfn::*;

// pub trait CanSort: Ord {}
// impl CanSort for u64 {}
// impl CanSort for u32 {}
// impl CanSort for i64 {}
// impl CanSort for i32 {}
// impl CanSort for bool {}
// impl CanSort for String {}
// impl CanSort for str {}

// pub trait SortOrder<T, U> {
//     fn sort_order(&self) -> Vec<usize>;
// }
// impl<T, U> SortOrder<T, U> for U
//     where T: DataType + DtOrd,
//           U: DataIndex<DType=T>
// {
    // fn sort_order(&self) -> Vec<usize> {
    //     let mut order = (0..self.len()).collect::<Vec<_>>();
    //     order.sort_unstable_by(|&left, &right| {
    //         // a, b are always in range, so unwraps are safe
    //         self.get_datum(left).unwrap().dt_cmp(&self.get_datum(right).unwrap())
    //     });
    //     order
    // }
// }

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

// impl<T, U> SortOrder<T, U> for U
//     where T: DataType + DtOrd,
//           U: DataIndex<DType=T>
// {
//     fn sort_order(&self) -> Vec<usize> {
//         // ordering is (arbitrarily) going to be:
//         // NA values, followed by everything else ascending
//         let mut order = (0..self.len()).collect::<Vec<_>>();
//         order.sort_unstable_by(|&a, &b| {
//             // a, b are always in range, so unwraps are safe
//             self.get_datum(a).unwrap().cmp(&self.get_datum(b).unwrap())
//         });
//         order
//     }
// }
// impl<U> SortOrder<f64, U> for U
//     where U: DataIndex<DType=f64>
// {
//     fn sort_order(&self) -> Vec<usize> {
//         let mut order = (0..self.len()).collect::<Vec<_>>();
//         order.sort_unstable_by(|&a, &b| {
//             // a, b are always in range, so unwraps are safe
//             let (vala, valb) = (self.get_datum(a).unwrap(), self.get_datum(b).unwrap());
//             vala.partial_cmp(&valb).unwrap_or_else(|| {
//                 // partial_cmp doesn't fail for Value::NA, unwraps safe
//                 let (vala, valb) = (vala.unwrap(), valb.unwrap());
//                 if vala.is_nan() && !valb.is_nan() {
//                     Ordering::Less
//                 } else {
//                     // since partial_cmp only fails for NAN, then !vala.is_nan() && valb.is_nan()
//                     Ordering::Greater
//                 }
//             })
//         });
//         order
//     }
// }

// /// Helper trait retrieving the sort permutation for a field.
// pub trait SortOrderBy {
//     /// Returns the sort permutation for the field specified.
//     fn sort_order_by(&self, ident: &FieldIdent) -> Result<Vec<usize>>;
// }
// impl<T> SortOrderBy for T where T: FieldApplyTo {
//     fn sort_order_by(&self, ident: &FieldIdent) -> Result<Vec<usize>> {
//         self.field_apply_to(
//             &mut SortOrderFn {},
//             ident
//         )
//     }
// }
// /// Helper trait retrieving the sort permutation for a structure.
// pub trait SortOrder {
//     /// Returns the sort permutation for the data in thie data structure.
//     fn sort_order(&self) -> Result<Vec<usize>>;
// }
// impl<T> SortOrder for T where T: FieldApply {
//     fn sort_order(&self) -> Result<Vec<usize>> {
//         self.field_apply(&mut SortOrderFn {})
//     }
// }

// field_map_fn![
//     /// `FieldMapFn` function struct for retrieving the sort permutation order for a field.
//     pub SortOrderFn { type Output = Vec<usize>; }
//     fn [unsigned, signed, text, boolean](self, field) {
        // ordering is (arbitrarily) going to be:
        // NA values, followed by everything else ascending
        // let mut order = (0..field.len()).collect::<Vec<_>>();
//         order.sort_unstable_by(|&a, &b| {
//             // a, b are always in range, so unwraps are safe
//             field.get_data(a).unwrap().cmp(&field.get_data(b).unwrap())
//         });
//         order
//     }
//     fn float(self, field) {
//         let mut order = (0..field.len()).collect::<Vec<_>>();
//         order.sort_unstable_by(|&a, &b| {
//             // a, b are always in range, so unwraps are safe
//             let (vala, valb) = (field.get_data(a).unwrap(), field.get_data(b).unwrap());
//             vala.partial_cmp(&valb).unwrap_or_else(|| {
//                 // partial_cmp doesn't fail for Value::NA, unwraps safe
//                 let (vala, valb) = (vala.unwrap(), valb.unwrap());
                // if vala.is_nan() && !valb.is_nan() {
                //     Ordering::Less
                // } else {
                //     // since partial_cmp only fails for NAN, then !vala.is_nan() && valb.is_nan()
                //     Ordering::Greater
                // }
//             })
//         });
//         order
//     }
// ];

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

pub struct SortOrderFunc;
impl<DTypes, T> Func<DTypes, T> for SortOrderFunc
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
