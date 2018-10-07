macro_rules! impl_field_op {
    (
        $dtypes:ty =>
        $op:tt
        $op_fn:tt
        $op_tt:tt
        $dtype:ty
    ) => {
// START IMPL_FIELD_OP

use std::ops::$op;

use $crate::field::Value;
use $crate::access::{DataIndex, DataIterator};
use $crate::select::Selection;
use $crate::view::DataView;
use $crate::store::{DataStore, WithDataFromIter};
use $crate::error::*;

// &left <op> &right
impl<'a, 'b, DI> $op<&'b Selection<$dtypes, DI, $dtype>> for &'a Selection<$dtypes, DI, $dtype>
    where DI: DataIndex<$dtypes, DType=$dtype>,
{
    type Output = Result<DataView<$dtypes>>;

    fn $op_fn(self, rhs: &'b Selection<$dtypes, DI, $dtype>) -> Result<DataView<$dtypes>> {
        // check dimensions
        if self.len() != rhs.len() {
            return Err(AgnesError::DimensionMismatch(
                "unable to apply arithmetic operation between different-length selections".into()
            ));
        }

        // with_data_from_iter only fails on field collisions, so unwrap is safe.
        Ok(WithDataFromIter::<$dtype, $dtypes>::with_data_from_iter(
            DataStore::empty(),
            format!("{} {} {}", self.ident, stringify![$op_tt], rhs.ident),
            DataIterator::new(self).zip(DataIterator::new(rhs))
                .map(|(left_maybe_na, right_maybe_na)| {
                    match (left_maybe_na, right_maybe_na) {
                        (Value::Exists(l), Value::Exists(r)) =>
                            Value::Exists($op::$op_fn(l, r)),
                        (Value::Exists(l), Value::Na) => Value::Exists(*l),
                        (Value::Na, Value::Exists(r)) => Value::Exists(*r),
                        _ => Value::Na
                    }
                })
        ).unwrap().into())

    }
}

// left <op> right
impl<DI> $op<Selection<$dtypes, DI, $dtype>> for Selection<$dtypes, DI, $dtype>
    where DI: DataIndex<$dtypes, DType=$dtype>,
{
    type Output = Result<DataView<$dtypes>>;

    fn $op_fn(self, rhs: Selection<$dtypes, DI, $dtype>) -> Result<DataView<$dtypes>> {
        (&self).$op_fn(&rhs)
    }
}

// &left <op> right
impl<'a, DI> $op<Selection<$dtypes, DI, $dtype>> for &'a Selection<$dtypes, DI, $dtype>
    where DI: DataIndex<$dtypes, DType=$dtype>,
{
    type Output = Result<DataView<$dtypes>>;

    fn $op_fn(self, rhs: Selection<$dtypes, DI, $dtype>) -> Result<DataView<$dtypes>> {
        self.$op_fn(&rhs)
    }
}

// left <op> &right
impl<'b, DI> $op<&'b Selection<$dtypes, DI, $dtype>> for Selection<$dtypes, DI, $dtype>
    where DI: DataIndex<$dtypes, DType=$dtype>,
{
    type Output = Result<DataView<$dtypes>>;

    fn $op_fn(self, rhs: &'b Selection<$dtypes, DI, $dtype>) -> Result<DataView<$dtypes>> {
        (&self).$op_fn(rhs)
    }
}

// END IMPL_FIELD_OP
    }
}

#[macro_export]
macro_rules! field_addition {
    // handle end-comma elision
    ($dtypes:ident => $($dtype:tt,)*) => {
        field_addition![$dtypes => $($dtype),*]
    };
    ($dtypes:ident => $($dtype:tt),*) => {
        pub mod field_addition {$(
            #[allow(non_snake_case)]
            pub mod $dtype {
                use super::super::$dtypes as $dtypes;
                impl_field_op![
                    $dtypes =>
                    Add
                    add
                    +
                    $dtype
                ];
            }
        )*}
    }
}

#[macro_export]
macro_rules! field_subtraction {
    // handle end-comma elision
    ($dtypes:ident => $($dtype:tt,)*) => {
        field_subtraction![$dtypes => $($dtype),*]
    };
    ($dtypes:ident => $($dtype:tt),*) => {
        pub mod field_subtraction {$(
            #[allow(non_snake_case)]
            pub mod $dtype {
                use super::super::$dtypes as $dtypes;
                impl_field_op![
                    $dtypes =>
                    Sub
                    sub
                    -
                    $dtype
                ];
            }
        )*}
    }
}

#[macro_export]
macro_rules! field_multiplication {
    // handle end-comma elision
    ($dtypes:ident => $($dtype:tt,)*) => {
        field_multiplication![$dtypes => $($dtype),*]
    };
    ($dtypes:ident => $($dtype:tt),*) => {
        pub mod field_multiplication {$(
            #[allow(non_snake_case)]
            pub mod $dtype {
                use super::super::$dtypes as $dtypes;
                impl_field_op![
                    $dtypes =>
                    Mul
                    mul
                    *
                    $dtype
                ];
            }
        )*}
    }
}

#[macro_export]
macro_rules! field_division {
    // handle end-comma elision
    ($dtypes:ident => $($dtype:tt,)*) => {
        field_division![$dtypes => $($dtype),*]
    };
    ($dtypes:ident => $($dtype:tt),*) => {
        pub mod field_division {$(
            #[allow(non_snake_case)]
            pub mod $dtype {
                use super::super::$dtypes as $dtypes;
                impl_field_op![
                    $dtypes =>
                    Div
                    div
                    /
                    $dtype
                ];
            }
        )*}
    }
}

#[macro_export]
macro_rules! field_ops {
    // handle end-comma elision
    ($dtypes:ident => $($dtype:tt,)*) => {
        field_ops![$dtypes => $($dtype),*]
    };
    ($dtypes:ident => $($dtype:tt),*) => {
        field_addition!        [$dtypes => $($dtype),*];
        field_subtraction!     [$dtypes => $($dtype),*];
        field_multiplication!  [$dtypes => $($dtype),*];
        field_division!        [$dtypes => $($dtype),*];
    }
}

#[cfg(test)]
mod tests {
    use test_utils::*;
    use select::Field;
    use data_types::standard::DataView;
    use field::FieldIdent;

    macro_rules! test_view_op {
        ($dtype:ty, $left:expr, $right:expr, $result:expr, $op:expr, $strop:expr,
            $test_mod:ident
        ) =>
        {{
            let data_vec1 = $left;
            let data_vec2 = $right;
            let dv1 = data_vec1.clone().merged_with_sample_emp_table("Foo");
            let dv2 = data_vec2.clone().merged_with_sample_emp_table("Bar");
            let computed_dv: DataView = ($op(
                dv1.field::<$dtype, _>("Foo").unwrap(),
                dv2.field::<$dtype, _>("Bar").unwrap()
            )).unwrap();
            let target_ident = FieldIdent::Name(format!("Foo {} Bar", $strop));
            $test_mod::assert_dv_eq_vec(&computed_dv, &target_ident, $result);
        }}
    }
    macro_rules! test_add_op {
        ($dtype:ty, $left:expr, $right:expr, $result:expr, $test_mod:ident) => (
            test_view_op!($dtype, $left, $right, $result, |dv1, dv2| dv1  +  dv2, "+", $test_mod);
            test_view_op!($dtype, $left, $right, $result, |dv1, dv2| dv1  + &dv2, "+", $test_mod);
            test_view_op!($dtype, $left, $right, $result, |dv1, dv2| &dv1 +  dv2, "+", $test_mod);
            test_view_op!($dtype, $left, $right, $result, |dv1, dv2| &dv1 + &dv2, "+", $test_mod);
        )
    }
    macro_rules! test_sub_op {
        ($dtype:ty, $left:expr, $right:expr, $result:expr, $test_mod:ident) => (
            test_view_op!($dtype, $left, $right, $result, |dv1, dv2| dv1  -  dv2, "-", $test_mod);
            test_view_op!($dtype, $left, $right, $result, |dv1, dv2| dv1  - &dv2, "-", $test_mod);
            test_view_op!($dtype, $left, $right, $result, |dv1, dv2| &dv1 -  dv2, "-", $test_mod);
            test_view_op!($dtype, $left, $right, $result, |dv1, dv2| &dv1 - &dv2, "-", $test_mod);
        )
    }
    macro_rules! test_mul_op {
        ($dtype:ty, $left:expr, $right:expr, $result:expr, $test_mod:ident) => (
            test_view_op!($dtype, $left, $right, $result, |dv1, dv2| dv1  *  dv2, "*", $test_mod);
            test_view_op!($dtype, $left, $right, $result, |dv1, dv2| dv1  * &dv2, "*", $test_mod);
            test_view_op!($dtype, $left, $right, $result, |dv1, dv2| &dv1 *  dv2, "*", $test_mod);
            test_view_op!($dtype, $left, $right, $result, |dv1, dv2| &dv1 * &dv2, "*", $test_mod);
        )
    }
    macro_rules! test_div_op {
        ($dtype:ty, $left:expr, $right:expr, $result:expr, $test_mod:ident) => (
            test_view_op!($dtype, $left, $right, $result, |dv1, dv2| dv1  /  dv2, "/", $test_mod);
            test_view_op!($dtype, $left, $right, $result, |dv1, dv2| dv1  / &dv2, "/", $test_mod);
            test_view_op!($dtype, $left, $right, $result, |dv1, dv2| &dv1 /  dv2, "/", $test_mod);
            test_view_op!($dtype, $left, $right, $result, |dv1, dv2| &dv1 / &dv2, "/", $test_mod);
        )
    }

    #[test]
    fn add_field() {
        // unsigned data + unsigned data
        test_add_op!(
            u64,
            vec![2u64,  3, 8,  2,  20,  3, 0],
            vec![55u64, 3, 1,  9, 106,  9, 0],
            vec![57u64, 6, 9, 11, 126, 12, 0],
            unsigned
        );

        // signed data + signed data
        test_add_op!(
            i64,
            vec![2i64,   3, -8,  2,   20, -3, 0],
            vec![55i64, -3, -1, -9, -106,  9, 0],
            vec![57i64,  0, -9, -7,  -86,  6, 0],
            signed
        );

        // float data + float data
        test_add_op!(
            f64,
            vec![55.0, -3.0, -1.0,  9.0, -106.0,  9.0, 0.0],
            vec![ 2.0,  3.0, -8.0, -2.0,  -20.0,  3.0, 0.0],
            vec![57.0,  0.0, -9.0,  7.0, -126.0, 12.0, 0.0],
            float
        );
    }


    #[test]
    fn sub_field() {
        // unsigned data - unsigned data
        test_sub_op!(
            u64,
            vec![ 55u64,  3, 8,  9, 200, 13, 0],
            vec![ 52u64,  3, 1,  2, 106,  9, 0],
            vec![  3u64,  0, 7,  7,  94,  4, 0],
            unsigned
        );

        // signed data - signed data
        test_sub_op!(
            i64,
            vec![  2i64,  3, -8,  2,   20, -3, 0],
            vec![ 55i64, -3, -1, -9, -106,  9, 0],
            vec![-53i64,  6, -7, 11,  126,-12, 0],
            signed
        );

        // float data - float data
        test_sub_op!(
            f64,
            vec![55.0, -3.0, -1.0,  9.0, -106.0,  9.0, 0.0],
            vec![ 2.0,  3.0, -8.0, -2.0,  -20.0,  3.0, 0.0],
            vec![53.0, -6.0,  7.0, 11.0,  -86.0,  6.0, 0.0],
            float
        );
    }

    #[test]
    fn mul_field() {
        // unsigned data * unsigned data
        test_mul_op!(
            u64,
            vec![  2u64,  3, 8,  2,   20,  3, 4],
            vec![ 55u64,  3, 1,  9,  106,  9, 0],
            vec![110u64,  9, 8, 18, 2120, 27, 0],
            unsigned
        );

        // signed data * signed data
        test_mul_op!(
            i64,
            vec![  2i64,  3, -8,   2,    20,  -3,  0],
            vec![ 55i64, -3, -1,  -9,  -106,   9, -4],
            vec![110i64, -9,  8, -18, -2120, -27,  0],
            signed
        );

        // float data * float data
        test_mul_op!(
            f64,
            vec![ 55.0, -3.0, -1.0,   9.0, -106.0,  9.0, 0.0],
            vec![  2.0,  3.0, -8.0,  -2.0,  -20.0,  3.0, 4.0],
            vec![110.0, -9.0,  8.0, -18.0, 2120.0, 27.0, 0.0],
            float
        );
    }

    #[test]
    fn div_field() {
        use std::f64::INFINITY as INF;
        use std::f64::NEG_INFINITY as NEGINF;

        // unsigned data / unsigned data
        test_div_op!(
            u64,
            vec![ 55u64,   3,   8,   2,   20,   0,   4],
            vec![ 11u64,   2,   1,   5,  100,   3,   1],
            vec![  5u64,   1,   8,   0,    0,   0,   4],
            unsigned
        );

        // signed data / signed data
        test_div_op!(
            i64,
            vec![ 55i64,   -3,   -8,   2,  -20,   0,   4],
            vec![ 11i64,   -2,    1,   5,  100,  -3,   1],
            vec![  5i64,    1,   -8,   0,   -0,   0,   4],
            signed
        );

        // float data / float data
        test_div_op!(
            f64,
            vec![ 55.0, -3.0, -8.0,  2.0,  -20.0,  0.0, 4.0],
            vec![ 11.0, -2.0, -1.0,  5.0,    0.0, -3.0, 0.0],
            vec![  5.0,  1.5,  8.0,  0.4, NEGINF,  0.0, INF],
            float
        );
    }

}