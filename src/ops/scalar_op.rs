macro_rules! impl_scalar_op {
    (
        $dtypes:ty =>
        $op:tt
        $op_fn:tt
        $op_tt:tt
        $dtype:ty
    ) => {
// START IMPL_SCALAR_OP

use std::ops::$op;

use $crate::access::{DataIndex, DataIterator};
use $crate::select::Selection;
use $crate::view::DataView;
use $crate::store::{DataStore, WithDataFromIter};

/*** Selection <op> Scalar implementations ***/

// &selection <op> &scalar
impl<'a, 'b, DI> $op<&'b $dtype> for &'a Selection<$dtypes, DI, $dtype>
    where DI: DataIndex<$dtypes, DType=$dtype>,
{
    type Output = DataView<$dtypes>;

    fn $op_fn(self, rhs: &'b $dtype) -> DataView<$dtypes> {
        // with_data_from_iter only fails on field collisions, so unwrap is safe.
        WithDataFromIter::<$dtype, $dtypes>::with_data_from_iter(
            DataStore::empty(),
            format!("{} {} {}", self.ident, stringify![$op_tt], rhs),
            DataIterator::new(self)
                .map(|maybe_na| maybe_na.map(|&ref value| $op::$op_fn(value, rhs)))
        ).unwrap().into()
    }
}

// selection <op> scalar
impl<DI> $op<$dtype> for Selection<$dtypes, DI, $dtype>
    where DI: DataIndex<$dtypes, DType=$dtype>,
{
    type Output = DataView<$dtypes>;

    fn $op_fn(self, rhs: $dtype) -> DataView<$dtypes> {
        (&self).$op_fn(&rhs)
    }
}

// selection <op> &scalar
impl<'b, DI> $op<&'b $dtype> for Selection<$dtypes, DI, $dtype>
    where DI: DataIndex<$dtypes, DType=$dtype>,
{
    type Output = DataView<$dtypes>;

    fn $op_fn(self, rhs: &'b $dtype) -> DataView<$dtypes> {
        (&self).$op_fn(rhs)
    }
}

// &selection <op> scalar
impl<'a, DI> $op<$dtype> for &'a Selection<$dtypes, DI, $dtype>
    where DI: DataIndex<$dtypes, DType=$dtype>,
{
    type Output = DataView<$dtypes>;

    fn $op_fn(self, rhs: $dtype) -> DataView<$dtypes> {
        self.$op_fn(&rhs)
    }
}

/*** Scalar <op> Selection implementations ***/

// &scalar <op> &selection
impl<'a, 'b, DI> $op<&'a Selection<$dtypes, DI, $dtype>> for &'b $dtype
    where DI: DataIndex<$dtypes, DType=$dtype>,
{
    type Output = DataView<$dtypes>;

    fn $op_fn(self, rhs: &'a Selection<$dtypes, DI, $dtype>) -> DataView<$dtypes> {
        // with_data_from_iter only fails on field collisions, so unwrap is safe.
        WithDataFromIter::<$dtype, $dtypes>::with_data_from_iter(
            DataStore::empty(),
            format!("{} {} {}", self, stringify![$op_tt], rhs.ident),
            DataIterator::new(rhs)
                .map(|maybe_na| maybe_na.map(|value| $op::$op_fn(self, value)))
        ).unwrap().into()
    }
}

// scalar <op> selection
impl<DI> $op<Selection<$dtypes, DI, $dtype>> for $dtype
    where DI: DataIndex<$dtypes, DType=$dtype>,
{
    type Output = DataView<$dtypes>;

    fn $op_fn(self, rhs: Selection<$dtypes, DI, $dtype>) -> DataView<$dtypes> {
        $op::<&Selection<$dtypes, DI, $dtype>>::$op_fn(&self, &rhs)
    }
}

// &scalar <op> selection
impl<'b, DI> $op<Selection<$dtypes, DI, $dtype>> for &'b $dtype
    where DI: DataIndex<$dtypes, DType=$dtype>,
{
    type Output = DataView<$dtypes>;

    fn $op_fn(self, rhs: Selection<$dtypes, DI, $dtype>) -> DataView<$dtypes> {
        $op::<&Selection<$dtypes, DI, $dtype>>::$op_fn(self, &rhs)
    }
}

// scalar <op> &selection
impl<'a, DI> $op<&'a Selection<$dtypes, DI, $dtype>> for $dtype
    where DI: DataIndex<$dtypes, DType=$dtype>,
{
    type Output = DataView<$dtypes>;

    fn $op_fn(self, rhs: &'a Selection<$dtypes, DI, $dtype>) -> DataView<$dtypes> {
        $op::<&Selection<$dtypes, DI, $dtype>>::$op_fn(&self, rhs)
    }
}

// END IMPL_SCALAR_OP
    }
}

#[macro_export]
macro_rules! scalar_addition {
    // handle end-comma elision
    ($dtypes:ident => $($dtype:tt,)*) => {
        scalar_addition![$dtypes => $($dtype),*]
    };
    ($dtypes:ident => $($dtype:tt),*) => {
        pub mod scalar_addition {$(
            #[allow(non_snake_case)]
            pub mod $dtype {
                use super::super::$dtypes as $dtypes;
                impl_scalar_op![
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
macro_rules! scalar_subtraction {
    // handle end-comma elision
    ($dtypes:ident => $($dtype:tt,)*) => {
        scalar_subtraction![$dtypes => $($dtype),*]
    };
    ($dtypes:ident => $($dtype:tt),*) => {
        pub mod scalar_subtraction {$(
            #[allow(non_snake_case)]
            pub mod $dtype {
                use super::super::$dtypes as $dtypes;
                impl_scalar_op![
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
macro_rules! scalar_multiplication {
    // handle end-comma elision
    ($dtypes:ident => $($dtype:tt,)*) => {
        scalar_multiplication![$dtypes => $($dtype),*]
    };
    ($dtypes:ident => $($dtype:tt),*) => {
        pub mod scalar_multiplication {$(
            #[allow(non_snake_case)]
            pub mod $dtype {
                use super::super::$dtypes as $dtypes;
                impl_scalar_op![
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
macro_rules! scalar_division {
    // handle end-comma elision
    ($dtypes:ident => $($dtype:tt,)*) => {
        scalar_division![$dtypes => $($dtype),*]
    };
    ($dtypes:ident => $($dtype:tt),*) => {
        pub mod scalar_division {$(
            #[allow(non_snake_case)]
            pub mod $dtype {
                use super::super::$dtypes as $dtypes;
                impl_scalar_op![
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
macro_rules! scalar_ops {
    // handle end-comma elision
    ($dtypes:ident => $($dtype:tt,)*) => {
        scalar_ops![$dtypes => $($dtype),*]
    };
    ($dtypes:ident => $($dtype:tt),*) => {
        scalar_addition!        [$dtypes => $($dtype),*];
        scalar_subtraction!     [$dtypes => $($dtype),*];
        scalar_multiplication!  [$dtypes => $($dtype),*];
        scalar_division!        [$dtypes => $($dtype),*];
    }
}


#[cfg(test)]
mod tests {
    use test_utils::*;
    use select::Field;
    use data_types::standard::DataView;

    macro_rules! field_name {
        ($l:expr, $op:tt, $r:expr) => {{
            format!("{} {} {}", $l, stringify!($op), $r)
        }}
    }
    macro_rules! test_op {
        ($dv:ident, $fident:expr, $op:tt, $term:expr, $target_mod:ident, $target_data:expr) => {{
            let computed_dv: DataView = $dv.field($fident).unwrap() $op $term;
            let field_name = field_name![$fident, $op, $term];
            $target_mod::assert_dv_eq_vec(&computed_dv, &field_name.clone().into(), $target_data);
        }};
        (@reverse $dv:ident, $fident:expr, $op:tt, $term:expr, $target_mod:ident, $target_data:expr
        )
            =>
        {{
            let field_name = field_name![$term, $op, $fident];
            let computed_dv: DataView = $term $op $dv.field($fident).unwrap();
            $target_mod::assert_dv_eq_vec(&computed_dv, &field_name.clone().into(), $target_data);
        }};
    }
    macro_rules! test_commutative {
        ($dv:ident, $fident:expr, $op:tt, $term:expr, $target_mod:ident,
            $target_data:expr
        ) => {{
            test_commutative!($dv, $fident, $op, $term, $target_mod, $target_data,
                |&x| x);
        }};
        ($dv:ident, $fident:expr, $op:tt, $term:expr, $target_mod:ident,
            $target_data:expr, $rev_fn:expr
        ) => {{
            // test dv <op> term
            test_op![$dv, $fident, $op, $term, $target_mod, $target_data];

            // test term <op> dv
            let target_vec = $target_data.iter().map($rev_fn).collect::<Vec<_>>();
            test_op![@reverse $dv, $fident, $op, $term, $target_mod, target_vec];
        }}
    }

    #[test]
    fn add_scalar() {
        /* unsigned data */
        let data_vec = vec![2u64, 3, 8, 2, 20, 3, 0];

        // added to unsigned scalar
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        test_commutative!(dv, "Foo", +, 2u64, unsigned,
            vec![4u64, 5, 10, 4, 22, 5, 2]
        );

        /* signed data */
        let data_vec = vec![2i64, -3, -8, 2, -20, 3, 0];

        // added to signed scalar
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        test_commutative!(dv, "Foo", +, -2i64, signed,
            vec![0i64, -5, -10, 0, -22, 1, -2]
        );

        /* floating point data */
        let data_vec = vec![2.0, -3.0, -8.0, 2.0, -20.0, 3.0, 0.0];

        // added to floating point scalar
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        test_commutative!(dv, "Foo", +, 2.0, float,
            vec![4.0, -1.0, -6.0, 4.0, -18.0, 5.0, 2.0]
        );
    }

    #[test]
    fn sub_scalar() {
        /* unsigned data */
        let data_vec = vec![2u64, 3, 8, 2, 20, 3, 2];

        // subtract unsigned scalar
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        test_op!(dv, "Foo", -, 2u64, unsigned,
            vec![0u64, 1, 6, 0, 18, 1, 0]
        );
        test_op!(@reverse dv, "Foo", -, 22u64, unsigned,
            vec![20u64, 19, 14, 20, 2, 19, 20]
        );

        /* signed data */
        let data_vec = vec![2i64, -3, -8, 2, -20, 3, 0];

        // subtract signed scalar
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        test_commutative!(dv, "Foo", -, -2i64, signed,
            vec![4i64, -1, -6, 4, -18, 5, 2], |&x| -x
        );

        /* floating point data */
        let data_vec = vec![2.0, -3.0, -8.0, 2.0, -20.0, 3.0, 0.0];

        // subtract floating point scalar
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        test_commutative!(dv, "Foo", -, 2.0, float,
            vec![0.0, -5.0, -10.0, 0.0, -22.0, 1.0, -2.0], |&x| -x
        );
    }

    #[test]
    #[should_panic]
    fn sub_scalar_panic() {
        /* unsigned data */
        let data_vec = vec![2u64, 3, 8, 2, 20, 3, 0];

        // subtract unsigned scalar; should panic (overflow) on last data point
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        println!("{}", dv);
        test_op!(dv, "Foo", -, 2u64, unsigned,
            vec![0u64, 1, 6, 0, 18, 1, 0]
        );
    }

    #[test]
    fn mul_scalar() {
        /* unsigned data */
        let data_vec = vec![2u64, 3, 8, 2, 20, 3, 0];

        // multiplied by unsigned scalar
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        test_commutative!(dv, "Foo", *, 2u64, unsigned,
            vec![4u64, 6, 16, 4, 40, 6, 0]
        );

        /* signed data */
        let data_vec = vec![2i64, -3, -8, 2, -20, 3, 0];

        // multiplied by signed scalar
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        test_commutative!(dv, "Foo", *, -2i64, signed,
            vec![-4i64, 6, 16, -4, 40, -6, -0]
        );

        /* floating point data */
        let data_vec = vec![2.0, -3.0, -8.0, 2.0, -20.0, 3.0, 0.0];

        // multiplied by floating point scalar
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        test_commutative!(dv, "Foo", *, 2.0, float,
            vec![4.0, -6.0, -16.0, 4.0, -40.0, 6.0, 0.0]
        );
    }


    #[test]
    fn div_scalar() {
        /* unsigned data */
        let data_vec = vec![2u64, 3, 8, 2, 20, 3, 2];

        // divide by unsigned scalar
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        test_op!(dv, "Foo", /, 2u64, unsigned,
            vec![1u64, 1, 4, 1, 10, 1, 1]
        );
        test_op!(@reverse dv, "Foo", /, 120u64, unsigned,
            vec![60u64, 40, 15, 60, 6, 40, 60]
        );

        /* signed data */
        let data_vec = vec![2i64, -3, -8, 2, -20, 3, 2];

        // divide by signed scalar
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        test_op!(dv, "Foo", /, 2i64, signed,
            vec![1i64, -1, -4, 1, -10, 1, 1]
        );
        test_op!(@reverse dv, "Foo", /, 120i64, signed,
            vec![60i64, -40, -15, 60, -6, 40, 60]
        );

        /* floating point data */
        let data_vec = vec![2.0, -3.0, -8.0, 2.0, -20.0, 3.0, 0.0];

        // divide by floating point scalar; should remain a floating point field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        test_commutative!(dv, "Foo", /, 2.0, float,
            vec![1.0, -1.5, -4.0, 1.0, -10.0, 1.5, 0.0], |&x| 1.0 / x
        );

        // extra divide-by-zero check
        use std::f64::INFINITY as INF;
        use std::f64::NEG_INFINITY as NEGINF;
        // use non-zero data vector, since 0 / 0 is NaN
        // TODO: use 0 in data vec if we ever implement NaN-agnostic matching
        let data_vec = vec![2.0, -3.0, -8.0, 2.0, -20.0, 3.0, 1.0];
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        let computed_dv: DataView = dv.field("Foo").unwrap() / 0.0;
        float::assert_dv_eq_vec(&computed_dv, &"Foo / 0".into(),
            vec![INF, NEGINF, NEGINF, INF, NEGINF, INF, INF]
        );

        // divide-by-zero when zero is in the data view
        let data_vec = vec![2.0, -3.0, 0.0, 0.0, -20.0, 3.0, 1.0];
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        let computed_dv: DataView = 60.0 / dv.field("Foo").unwrap();
        float::assert_dv_eq_vec(&computed_dv, &"60 / Foo".into(),
            vec![30.0, -20.0, INF, INF, -3.0, 20.0, 60.0]
        );
        // check negative infinity too
        let data_vec = vec![2.0, -3.0, 0.0, 0.0, -20.0, 3.0, 1.0];
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        let computed_dv: DataView = -60.0 / dv.field("Foo").unwrap();
        float::assert_dv_eq_vec(&computed_dv, &"-60 / Foo".into(),
            vec![-30.0, 20.0, NEGINF, NEGINF, 3.0, -20.0, -60.0]
        );
    }

    #[test]
    #[should_panic]
    fn div_zero_scalar() {
        let data_vec = vec![2u64, 3, 0, 0, 20, 3, 1];
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        let _computed_dv: DataView = dv.field("Foo").unwrap() / 0;
    }

    #[test]
    #[should_panic]
    fn div_zero_datum() {
        let data_vec = vec![2i64, -3, 0, 0, -20, 3, 1];
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        let _computed_dv: DataView = 60 / dv.field("Foo").unwrap();
    }

    #[test]
    fn add_scalar_ref() {
        /* unsigned data */
        let data_vec = vec![2u64, 3, 8, 2, 20, 3, 0];

        // added to unsigned scalar; should remain an unsigned field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");

        let field_name = field_name!["Foo", +, 4u64];

        let computed_dv = &dv.field("Foo").unwrap() + &4u64;
        unsigned::assert_dv_eq_vec(&computed_dv, &field_name.clone().into(),
            vec![6u64, 7, 12, 6, 24, 7, 4]
        );
        let computed_dv = &dv.field::<u64, _>("Foo").unwrap() + 4u64;
        unsigned::assert_dv_eq_vec(&computed_dv, &field_name.clone().into(),
            vec![6u64, 7, 12, 6, 24, 7, 4]
        );
        let computed_dv = dv.field::<u64, _>("Foo").unwrap() + &4u64;
        unsigned::assert_dv_eq_vec(&computed_dv, &field_name.clone().into(),
            vec![6u64, 7, 12, 6, 24, 7, 4]
        );
        let computed_dv = dv.field::<u64, _>("Foo").unwrap() + 4u64;
        unsigned::assert_dv_eq_vec(&computed_dv, &field_name.clone().into(),
            vec![6u64, 7, 12, 6, 24, 7, 4]
        );

        let field_name = field_name![4u64, +, "Foo"];

        let computed_dv = &4u64 + &dv.field::<u64, _>("Foo").unwrap();
        unsigned::assert_dv_eq_vec(&computed_dv, &field_name.clone().into(),
            vec![6u64, 7, 12, 6, 24, 7, 4]
        );
        let computed_dv = 4u64 + &dv.field::<u64, _>("Foo").unwrap();
        unsigned::assert_dv_eq_vec(&computed_dv, &field_name.clone().into(),
            vec![6u64, 7, 12, 6, 24, 7, 4]
        );
        let computed_dv = &4u64 + dv.field::<u64, _>("Foo").unwrap();
        unsigned::assert_dv_eq_vec(&computed_dv, &field_name.clone().into(),
            vec![6u64, 7, 12, 6, 24, 7, 4]
        );
        let computed_dv = 4u64 + dv.field::<u64, _>("Foo").unwrap();
        unsigned::assert_dv_eq_vec(&computed_dv, &field_name.clone().into(),
            vec![6u64, 7, 12, 6, 24, 7, 4]
        );
    }
}
