use std::fmt::Debug;
use std::ops::{Add, Div, Mul, Sub};

use access::{DataIndex, DataIndexMut};
use field::FieldData;
use frame::Framed;
use store::DataRef;

macro_rules! impl_op {
    ($trait_name:tt $trait_fn:tt; $([$($ty_tt:tt)*])*) => {$(

        // &data <op> &scalar
        impl<'a, 'b, T> $trait_name<&'b T>
        for &'a $($ty_tt)*
        where
            $($ty_tt)*: DataIndex<DType=T>,
            &'a T: $trait_name<&'b T>,
            <&'a T as $trait_name<&'b T>>::Output: Debug + Default,
        {
            type Output = FieldData<<&'a T as $trait_name<&'b T>>::Output>;

            fn $trait_fn(self, rhs: &'b T) -> Self::Output
            {
                self.iter().map(|val| val.map(|val| val.$trait_fn(rhs))).collect()
            }
        }

        // &data <op> scalar
        impl<'a, T> $trait_name<T>
        for &'a $($ty_tt)*
        where
            $($ty_tt)*: DataIndex<DType=T>,
            &'a T: $trait_name<T>,
            T: Clone,
            <&'a T as $trait_name<T>>::Output: Debug + Default,
        {
            type Output = FieldData<<&'a T as $trait_name<T>>::Output>;

            fn $trait_fn(self, rhs: T) -> Self::Output {
                self.iter().map(|val| val.map(|val| val.$trait_fn(rhs.clone()))).collect()
            }
        }

        // data <op> &scalar
        impl<'b, T> $trait_name<&'b T>
        for $($ty_tt)*
        where
            $($ty_tt)*: DataIndexMut<DType=T>,
            T: $trait_name<&'b T> + Default,
            <T as $trait_name<&'b T>>::Output: Debug + Default
        {
            type Output = FieldData<<T as $trait_name<&'b T>>::Output>;

            fn $trait_fn(mut self, rhs: &'b T) -> Self::Output {
                self.drain().map(|val| val.map(|val| val.$trait_fn(rhs))).collect()
            }
        }

        // data <op> scalar
        impl<T> $trait_name<T>
        for $($ty_tt)*
        where
            $($ty_tt)*: DataIndexMut<DType=T>,
            T: $trait_name<T> + Default + Clone,
            <T as $trait_name<T>>::Output: Debug + Default
        {
            type Output = FieldData<<T as $trait_name<T>>::Output>;

            fn $trait_fn(mut self, rhs: T) -> Self::Output {
                self.drain().map(|val| val.map(|val| val.$trait_fn(rhs.clone()))).collect()
            }
        }

    )*}
}

impl_op![Add add; [FieldData<T>] [Framed<T>] [DataRef<T>]];
impl_op![Sub sub; [FieldData<T>] [Framed<T>] [DataRef<T>]];
impl_op![Mul mul; [FieldData<T>] [Framed<T>] [DataRef<T>]];
impl_op![Div div; [FieldData<T>] [Framed<T>] [DataRef<T>]];

macro_rules! impl_op_nongeneric {
    ($dtype:ty; $trait_name:tt $trait_fn:tt; $([$($ty_tt:tt)*])*) => {$(

        // &scalar <op> &data
        impl<'a, 'b> $trait_name<&'a $($ty_tt)*<$dtype>>
        for &'b $dtype
        where
            $($ty_tt)*<$dtype>: DataIndex<DType=$dtype>,
            &'b $dtype: $trait_name<&'a $dtype>,
            <&'b $dtype as $trait_name<&'a $dtype>>::Output: Debug + Default,
        {
            type Output = FieldData<<&'b $dtype as $trait_name<&'a $dtype>>::Output>;

            fn $trait_fn(self, rhs: &'a $($ty_tt)*<$dtype>) -> Self::Output
            {
                rhs.iter().map(|val| val.map(|val| self.$trait_fn(val))).collect()
            }
        }

        // scalar <op> &data
        impl<'a> $trait_name<&'a $($ty_tt)*<$dtype>>
        for $dtype
        where
            $($ty_tt)*<$dtype>: DataIndex<DType=$dtype>,
            $dtype: $trait_name<&'a $dtype>,
            <$dtype as $trait_name<&'a $dtype>>::Output: Debug + Default,
        {
            type Output = FieldData<<$dtype as $trait_name<&'a $dtype>>::Output>;

            fn $trait_fn(self, rhs: &'a $($ty_tt)*<$dtype>) -> Self::Output
            {
                rhs.iter().map(|val| val.map(|val| self.$trait_fn(val))).collect()
            }
        }

        // &scalar <op> data
        impl<'b> $trait_name<$($ty_tt)*<$dtype>>
        for &'b $dtype
        where
            $($ty_tt)*<$dtype>: DataIndex<DType=$dtype>,
            &'b $dtype: $trait_name<$dtype>,
            $dtype: Clone,
            <&'b $dtype as $trait_name<$dtype>>::Output: Debug + Default,
        {
            type Output = FieldData<<&'b $dtype as $trait_name<$dtype>>::Output>;

            fn $trait_fn(self, rhs: $($ty_tt)*<$dtype>) -> Self::Output
            {
                rhs.iter().map(|val| val.map(|val| self.$trait_fn(val.clone()))).collect()
            }
        }

        // scalar <op> data
        impl $trait_name<$($ty_tt)*<$dtype>>
        for $dtype
        where
            $($ty_tt)*<$dtype>: DataIndex<DType=$dtype>,
            $dtype: Clone + $trait_name<$dtype>,
            <$dtype as $trait_name<$dtype>>::Output: Debug + Default,
        {
            type Output = FieldData<<$dtype as $trait_name<$dtype>>::Output>;

            fn $trait_fn(self, rhs: $($ty_tt)*<$dtype>) -> Self::Output
            {
                rhs.iter().map(|val| val.map(|val| self.$trait_fn(val.clone()))).collect()
            }
        }
    )*}
}

macro_rules! impl_scalar_ops_nongeneric {
    ($dtype:ty) => {
        impl_op_nongeneric![$dtype; Add add; [FieldData] [Framed] [DataRef]];
        impl_op_nongeneric![$dtype; Sub sub; [FieldData] [Framed] [DataRef]];
        impl_op_nongeneric![$dtype; Mul mul; [FieldData] [Framed] [DataRef]];
        impl_op_nongeneric![$dtype; Div div; [FieldData] [Framed] [DataRef]];
    }
}

macro_rules! impl_scalar_ops_nongeneric_prims {
    ($($dtype:ty)*) => {$(
        impl_scalar_ops_nongeneric![$dtype];
    )*}
}

impl_scalar_ops_nongeneric_prims![f64 f32 u64 u32 usize i64 i32 isize];

#[cfg(test)]
mod tests {
    use access::DataIndex;
    use field::FieldData;
    use frame::Framed;
    use store::DataRef;

    macro_rules! test_op {
        ($data:expr, $op:tt, $term:expr, $expected:expr) => {{
            let data: FieldData<_> = $data.into();
            assert_eq![(&data          $op &$term          ).to_vec(), $expected];
            assert_eq![(&data          $op  $term.clone()  ).to_vec(), $expected];
            assert_eq![( data.clone()  $op &$term          ).to_vec(), $expected];
            assert_eq![( data.clone()  $op  $term.clone()  ).to_vec(), $expected];

            let data: DataRef<_> = data.into();
            assert_eq![(&data          $op &$term          ).to_vec(), $expected];
            assert_eq![(&data          $op  $term.clone()  ).to_vec(), $expected];

            let data: Framed<_> = data.into();
            assert_eq![(&data          $op &$term          ).to_vec(), $expected];
            assert_eq![(&data          $op  $term.clone()  ).to_vec(), $expected];
        }};
    }
    macro_rules! test_oprev {
        ($data:expr, $op:tt, $term:expr, $expected:expr) => {{
            let data: FieldData<_> = $data.into();
            assert_eq![(&$term          $op &data          ).to_vec(), $expected];
            assert_eq![(&$term          $op data.clone()   ).to_vec(), $expected];
            assert_eq![( $term.clone()  $op &data          ).to_vec(), $expected];
            assert_eq![( $term.clone()  $op data.clone()   ).to_vec(), $expected];

            let data: DataRef<_> = data.into();
            assert_eq![(&$term          $op &data          ).to_vec(), $expected];
            assert_eq![( $term.clone()  $op &data          ).to_vec(), $expected];

            let data: Framed<_> = data.into();
            assert_eq![(&$term          $op &data          ).to_vec(), $expected];
            assert_eq![( $term.clone()  $op &data          ).to_vec(), $expected];
        }};
    }
    macro_rules! test_commutative {
        ($data:expr, $op:tt, $term:expr, $expected:expr) => {{
            test_op![$data, $op, $term, $expected];
            test_oprev![$data, $op, $term, $expected];
        }};
    }

    #[test]
    fn add_scalar() {
        test_commutative![vec![2u64, 3, 8, 2, 20, 3, 0], +, 2u64, vec![4u64, 5, 10, 4, 22, 5, 2]];

        test_commutative![vec![2i64, -3, -8, 2, -20, 3, 0], +, -2i64,
            vec![0i64, -5, -10, 0, -22, 1, -2]
        ];

        test_commutative![vec![2.0f64, -3.0, -8.0, 2.0, -20.0, 3.0, 0.0], +, 2.0f64,
            vec![4.0, -1.0, -6.0, 4.0, -18.0, 5.0, 2.0]
        ];
    }

    #[test]
    fn sub_scalar() {
        test_op![vec![2u64, 3, 8, 2, 20, 3, 2], -, 2u64, vec![0u64, 1, 6, 0, 18, 1, 0]];
        test_oprev![vec![2u64, 3, 8, 2, 20, 3, 2], -, 22u64, vec![20u64, 19, 14, 20, 2, 19, 20]];

        test_op![vec![2i64, -3, -8, 2, -20, 3, 0], -, -2i64, vec![4i64, -1, -6, 4, -18, 5, 2]];
        test_oprev![vec![2i64, -3, -8, 2, -20, 3, 0], -, -2i64,
            vec![-4i64, 1, 6, -4, 18, -5, -2]
        ];

        test_op![vec![2.0f64, -3.0, -8.0, 2.0, -20.0, 3.0, 0.0], -, -2.0f64,
            vec![4.0f64, -1.0, -6.0, 4.0, -18.0, 5.0, 2.0]
        ];
        test_oprev![vec![2.0f64, -3.0, -8.0, 2.0, -20.0, 3.0, 0.0], -, -2.0f64,
            vec![-4.0f64, 1.0, 6.0, -4.0, 18.0, -5.0, -2.0]
        ];
    }

    #[test]
    #[should_panic]
    fn sub_scalar_panic() {
        test_op![vec![2u64, 3, 8, 2, 20, 3, 0], -, 2u64, vec![0u64, 1, 6, 0, 18, 1, /*panic!*/]];
    }

    #[test]
    fn mul_scalar() {
        test_commutative![vec![2u64, 3, 8, 2, 20, 3, 0], *, 2u64,
            vec![4u64, 6, 16, 4, 40, 6, 0]
        ];

        test_commutative![vec![2i64, -3, -8, 2, -20, 3, 0], *, -2i64,
            vec![-4i64, 6, 16, -4, 40, -6, -0]
        ];

        test_commutative![vec![2.0f64, -3.0, -8.0, 2.0, -20.0, 3.0, 0.0], *, 2.0f64,
            vec![4.0, -6.0, -16.0, 4.0, -40.0, 6.0, 0.0]
        ];
    }

    #[test]
    fn div_scalar() {
        test_op![vec![2u64, 3, 8, 2, 20, 3, 2], /, 2u64,
            vec![1u64, 1, 4, 1, 10, 1, 1]
        ];
        test_oprev![vec![2u64, 3, 8, 2, 20, 3, 2], /, 120u64,
            vec![60u64, 40, 15, 60, 6, 40, 60]
        ];

        test_op![vec![2i64, -3, -8, 2, -20, 3, 2], /, 2i64,
            vec![1i64, -1, -4, 1, -10, 1, 1]
        ];
        test_oprev![vec![2i64, -3, -8, 2, -20, 3, 2], /, 120i64,
            vec![60i64, -40, -15, 60, -6, 40, 60]
        ];

        test_op![vec![2.0f64, -3.0, -8.0, 2.0, -20.0, 3.0, 0.0], /, 2.0f64,
            vec![1.0, -1.5, -4.0, 1.0, -10.0, 1.5, 0.0]
        ];
        test_oprev![vec![2.0f64, -3.0, -8.0, 2.0, -20.0, 3.0, 0.0], /, 2.0f64,
            vec![1.0, -1.5, -4.0, 1.0, -10.0, 1.5, 0.0].iter().map(|v| 1.0 / v).collect::<Vec<_>>()
        ];

        // divide-by-zero check
        use std::f64::INFINITY as INF;
        use std::f64::NEG_INFINITY as NEGINF;

        test_op![vec![2.0f64, -3.0, -8.0, 2.0, -20.0, 3.0, 1.0], /, 0.0f64,
            vec![INF, NEGINF, NEGINF, INF, NEGINF, INF, INF]
        ];

        // NaN check (NaN != NaN, so we check value specifically to see if NaN or infinite);
        let result =
            (FieldData::<_>::from(vec![2.0f64, -3.0, -8.0, 0.0, -20.0, 3.0, 1.0]) / 0.0).to_vec();
        assert![result[3].is_nan()];
        for i in (0usize..result.len()).filter(|idx| *idx != 3) {
            assert![result[i].is_infinite()];
        }

        // divide-by-zero when zero is in the field data
        test_oprev![vec![2.0f64, -3.0, 0.0, 0.0, -20.0, 3.0, 1.0], /, 60.0f64,
            vec![30.0, -20.0, INF, INF, -3.0, 20.0, 60.0]
        ];
        // check negative infinity too
        test_oprev![vec![2.0f64, -3.0, 0.0, 0.0, -20.0, 3.0, 1.0], /, -60.0f64,
            vec![-30.0, 20.0, NEGINF, NEGINF, 3.0, -20.0, -60.0]
        ];
    }

    #[test]
    #[should_panic]
    fn div_zero_scalar() {
        test_op![vec![2u64, 3, 0, 0, 20, 3, 1], /, 0u64, Vec::<u64>::new(/*panic!*/)];
    }

    #[test]
    #[should_panic]
    fn div_zero_datum() {
        test_oprev![vec![2i64, -3, 0, 0, -20, 3, 1], /, 60i64, Vec::<i64>::new(/*panic!*/)];
    }
}
