use std::fmt::Debug;
use std::ops::{Add, Div, Mul, Sub};

use access::{DataIndex, DataIndexMut};
use error;
use field::FieldData;
use frame::Framed;
use store::DataRef;

pub trait LengthCheckedAdd<RHS> {
    type Output;

    fn add_checked(self, rhs: RHS) -> error::Result<Self::Output>;
}

pub trait LengthCheckedSub<RHS> {
    type Output;

    fn sub_checked(self, rhs: RHS) -> error::Result<Self::Output>;
}

pub trait LengthCheckedMul<RHS> {
    type Output;

    fn mul_checked(self, rhs: RHS) -> error::Result<Self::Output>;
}

pub trait LengthCheckedDiv<RHS> {
    type Output;

    fn div_checked(self, rhs: RHS) -> error::Result<Self::Output>;
}

macro_rules! impl_field_op {
    (
        $trait_name:tt $trait_fn:tt;
        $trait_checked:tt $fn_checked:tt;
        $([[$($lty_tt:tt)*] [$($rty_tt:tt)*]])*
    ) => {$(

        // &left <op> &right
        impl<'a, 'b, T> $trait_name<&'b $($rty_tt)*>
        for &'a $($lty_tt)*
        where
            $($lty_tt)*: DataIndex<DType=T>,
            $($rty_tt)*: DataIndex<DType=T>,
            &'a T: $trait_name<&'b T>,
            <&'a T as $trait_name<&'b T>>::Output: Debug + Default,
        {
            type Output = FieldData<<&'a T as $trait_name<&'b T>>::Output>;

            fn $trait_fn(self, rhs: &'b $($rty_tt)*) -> Self::Output
            {
                self.iter().zip(rhs.iter()).map(|(l, r)| l.$trait_fn(r)).collect()
            }
        }

        impl<'a, 'b, T> $trait_checked<&'b $($rty_tt)*>
        for &'a $($lty_tt)*
        where
            &'a $($lty_tt)*: $trait_name<&'b $($rty_tt)*>,
            $($lty_tt)*: DataIndex<DType=T>,
            $($rty_tt)*: DataIndex<DType=T>,
        {
            type Output = <&'a $($lty_tt)* as $trait_name<&'b $($rty_tt)*>>::Output;

            fn $fn_checked(self, rhs: &'b $($rty_tt)*)
            -> $crate::error::Result<Self::Output>
            {
                if self.len() != rhs.len() {
                    Err($crate::error::AgnesError::LengthMismatch {
                        expected: self.len(),
                        actual: rhs.len()
                    })
                } else {
                    Ok(self.$trait_fn(rhs))
                }
            }
        }

        // &left <op> right
        impl<'a, T> $trait_name<$($rty_tt)*>
        for &'a $($lty_tt)*
        where
            $($lty_tt)*: DataIndex<DType=T>,
            $($rty_tt)*: DataIndexMut<DType=T>,
            &'a T: $trait_name<T>,
            T: Default,
            <&'a T as $trait_name<T>>::Output: Debug + Default,
        {
            type Output = FieldData<<&'a T as $trait_name<T>>::Output>;

            fn $trait_fn(self, mut rhs: $($rty_tt)*) -> Self::Output
            {
                self.iter().zip(rhs.drain()).map(|(l, r)| l.$trait_fn(r)).collect()
            }
        }

        impl<'a, T> $trait_checked<$($rty_tt)*>
        for &'a $($lty_tt)*
        where
            &'a $($lty_tt)*: $trait_name<$($rty_tt)*>,
            $($lty_tt)*: DataIndex<DType=T>,
            $($rty_tt)*: DataIndexMut<DType=T>,
        {
            type Output = <&'a $($lty_tt)* as $trait_name<$($rty_tt)*>>::Output;

            fn $fn_checked(self, rhs: $($rty_tt)*)
            -> $crate::error::Result<Self::Output>
            {
                if self.len() != rhs.len() {
                    Err($crate::error::AgnesError::LengthMismatch {
                        expected: self.len(),
                        actual: rhs.len()
                    })
                } else {
                    Ok(self.$trait_fn(rhs))
                }
            }
        }

        // left <op> &right
        impl<'b, T> $trait_name<&'b $($rty_tt)*>
        for $($lty_tt)*
        where
            $($lty_tt)*: DataIndexMut<DType=T>,
            $($rty_tt)*: DataIndex<DType=T>,
            T: $trait_name<&'b T> + Default,
            <T as $trait_name<&'b T>>::Output: Debug + Default,
        {
            type Output = FieldData<<T as $trait_name<&'b T>>::Output>;

            fn $trait_fn(mut self, rhs: &'b $($rty_tt)*) -> Self::Output
            {
                self.drain().zip(rhs.iter()).map(|(l, r)| l.$trait_fn(r)).collect()
            }
        }

        impl<'b, T> $trait_checked<&'b $($rty_tt)*>
        for $($lty_tt)*
        where
            $($lty_tt)*: $trait_name<&'b $($rty_tt)*>,
            $($lty_tt)*: DataIndexMut<DType=T>,
            $($rty_tt)*: DataIndex<DType=T>,
        {
            type Output = <$($lty_tt)* as $trait_name<&'b $($rty_tt)*>>::Output;
            fn $fn_checked(self, rhs: &'b $($rty_tt)*)
            -> $crate::error::Result<Self::Output>
            {
                if self.len() != rhs.len() {
                    Err($crate::error::AgnesError::LengthMismatch {
                        expected: self.len(),
                        actual: rhs.len()
                    })
                } else {
                    Ok(self.$trait_fn(rhs))
                }
            }
        }

        // left <op> right
        impl<T> $trait_name<$($rty_tt)*>
        for $($lty_tt)*
        where
            $($lty_tt)*: DataIndexMut<DType=T>,
            $($rty_tt)*: DataIndexMut<DType=T>,
            T: $trait_name<T> + Default,
            <T as $trait_name<T>>::Output: Debug + Default,
        {
            type Output = FieldData<<T as $trait_name<T>>::Output>;

            fn $trait_fn(mut self, mut rhs: $($rty_tt)*) -> Self::Output
            {
                self.drain().zip(rhs.drain()).map(|(l, r)| l.$trait_fn(r)).collect()
            }
        }

        impl<T> $trait_checked<$($rty_tt)*>
        for $($lty_tt)*
        where
            $($lty_tt)*: $trait_name<$($rty_tt)*>,
            $($lty_tt)*: DataIndexMut<DType=T>,
            $($rty_tt)*: DataIndexMut<DType=T>,
        {
            type Output = <$($lty_tt)* as $trait_name<$($rty_tt)*>>::Output;

            fn $fn_checked(self, rhs: $($rty_tt)*)
            -> $crate::error::Result<Self::Output>
            {
                if self.len() != rhs.len() {
                    Err($crate::error::AgnesError::LengthMismatch {
                        expected: self.len(),
                        actual: rhs.len()
                    })
                } else {
                    Ok(self.$trait_fn(rhs))
                }
            }
        }

    )*}
}

impl_field_op![
    Add add;
    LengthCheckedAdd add_checked;

    [[FieldData<T>] [FieldData<T>]]
    [[FieldData<T>] [Framed<T>]]
    [[FieldData<T>] [DataRef<T>]]

    [[Framed<T>] [FieldData<T>]]
    [[Framed<T>] [Framed<T>]]
    [[Framed<T>] [DataRef<T>]]

    [[DataRef<T>] [FieldData<T>]]
    [[DataRef<T>] [Framed<T>]]
    [[DataRef<T>] [DataRef<T>]]
];

impl_field_op![
    Sub sub;
    LengthCheckedSub sub_checked;

    [[FieldData<T>] [FieldData<T>]]
    [[FieldData<T>] [Framed<T>]]
    [[FieldData<T>] [DataRef<T>]]

    [[Framed<T>] [FieldData<T>]]
    [[Framed<T>] [Framed<T>]]
    [[Framed<T>] [DataRef<T>]]

    [[DataRef<T>] [FieldData<T>]]
    [[DataRef<T>] [Framed<T>]]
    [[DataRef<T>] [DataRef<T>]]
];

impl_field_op![
    Mul mul;
    LengthCheckedMul mul_checked;

    [[FieldData<T>] [FieldData<T>]]
    [[FieldData<T>] [Framed<T>]]
    [[FieldData<T>] [DataRef<T>]]

    [[Framed<T>] [FieldData<T>]]
    [[Framed<T>] [Framed<T>]]
    [[Framed<T>] [DataRef<T>]]

    [[DataRef<T>] [FieldData<T>]]
    [[DataRef<T>] [Framed<T>]]
    [[DataRef<T>] [DataRef<T>]]
];

impl_field_op![
    Div div;
    LengthCheckedDiv div_checked;

    [[FieldData<T>] [FieldData<T>]]
    [[FieldData<T>] [Framed<T>]]
    [[FieldData<T>] [DataRef<T>]]

    [[Framed<T>] [FieldData<T>]]
    [[Framed<T>] [Framed<T>]]
    [[Framed<T>] [DataRef<T>]]

    [[DataRef<T>] [FieldData<T>]]
    [[DataRef<T>] [Framed<T>]]
    [[DataRef<T>] [DataRef<T>]]
];

#[cfg(test)]
mod tests {
    use field::FieldData;
    use frame::Framed;
    use store::DataRef;

    macro_rules! test_op {
        ($result:expr, $($op:tt)*) =>
        {{
            let result = $($op)*;
            assert_eq!(result, $result);
        }}
    }
    macro_rules! test_field_op {
        (@test_structs
            $left:expr, $right:expr, $lstruct:tt, $rstruct:tt, $result:expr,
            $ileft:ident, $iright:ident, $($op:tt)*
        ) => {{
            let mut $ileft = $left;
            let $ileft: FieldData<_> = $ileft.drain(..).collect();
            let mut $iright = $right;
            let $iright: FieldData<_> = $iright.drain(..).collect();

            let $ileft: $lstruct<_> = $ileft.into();
            let $iright: $rstruct<_> = $iright.into();

            let mut result = $result;
            let result: FieldData<_> = result.drain(..).collect();
            test_op!(result, $($op)*);
        }};
        ($left:expr, $right:expr, $result:expr, $op:tt) => {{

            // &left <op> &right
            test_field_op![@test_structs
                $left, $right, FieldData, FieldData, $result, left, right, &left $op &right
            ];
            test_field_op![@test_structs
                $left, $right, FieldData, DataRef, $result, left, right, &left $op &right
            ];
            test_field_op![@test_structs
                $left, $right, FieldData, Framed, $result, left, right, &left $op &right
            ];

            test_field_op![@test_structs
                $left, $right, DataRef, FieldData, $result, left, right, &left $op &right
            ];
            test_field_op![@test_structs
                $left, $right, DataRef, DataRef, $result, left, right, &left $op &right
            ];
            test_field_op![@test_structs
                $left, $right, DataRef, Framed, $result, left, right, &left $op &right
            ];

            test_field_op![@test_structs
                $left, $right, Framed, FieldData, $result, left, right, &left $op &right
            ];
            test_field_op![@test_structs
                $left, $right, Framed, DataRef, $result, left, right, &left $op &right
            ];
            test_field_op![@test_structs
                $left, $right, Framed, Framed, $result, left, right, &left $op &right
            ];

            // &left <op> right
            test_field_op![@test_structs
                $left, $right, FieldData, FieldData, $result, left, right, &left $op right
            ];
            test_field_op![@test_structs
                $left, $right, DataRef, FieldData, $result, left, right, &left $op right
            ];
            test_field_op![@test_structs
                $left, $right, Framed, FieldData, $result, left, right, &left $op right
            ];

            // left <op> &right
            test_field_op![@test_structs
                $left, $right, FieldData, FieldData, $result, left, right, left $op &right
            ];
            test_field_op![@test_structs
                $left, $right, FieldData, DataRef, $result, left, right, left $op &right
            ];
            test_field_op![@test_structs
                $left, $right, FieldData, Framed, $result, left, right, left $op &right
            ];

            // left <op> right
            test_field_op![@test_structs
                $left, $right, FieldData, FieldData, $result, left, right, left $op right
            ];
        }}
    }

    #[test]
    fn add_field() {
        // unsigned data + unsigned data
        test_field_op!(
            vec![2u64,  3, 8,  2,  20,  3, 0],
            vec![55u64, 3, 1,  9, 106,  9, 0],
            vec![57u64, 6, 9, 11, 126, 12, 0],
            +
        );

        // signed data + signed data
        test_field_op!(
            vec![2i64,   3, -8,  2,   20, -3, 0],
            vec![55i64, -3, -1, -9, -106,  9, 0],
            vec![57i64,  0, -9, -7,  -86,  6, 0],
            +
        );

        // float data + float data
        test_field_op!(
            vec![55.0, -3.0, -1.0,  9.0, -106.0,  9.0, 0.0],
            vec![ 2.0,  3.0, -8.0, -2.0,  -20.0,  3.0, 0.0],
            vec![57.0,  0.0, -9.0,  7.0, -126.0, 12.0, 0.0],
            +
        );
    }

    #[test]
    fn sub_field() {
        // unsigned data - unsigned data
        test_field_op!(
            vec![ 55u64,  3, 8,  9, 200, 13, 0],
            vec![ 52u64,  3, 1,  2, 106,  9, 0],
            vec![  3u64,  0, 7,  7,  94,  4, 0],
            -
        );

        // signed data - signed data
        test_field_op!(
            vec![  2i64,  3, -8,  2,   20, -3, 0],
            vec![ 55i64, -3, -1, -9, -106,  9, 0],
            vec![-53i64,  6, -7, 11,  126,-12, 0],
            -
        );

        // float data - float data
        test_field_op!(
            vec![55.0, -3.0, -1.0,  9.0, -106.0,  9.0, 0.0],
            vec![ 2.0,  3.0, -8.0, -2.0,  -20.0,  3.0, 0.0],
            vec![53.0, -6.0,  7.0, 11.0,  -86.0,  6.0, 0.0],
            -
        );
    }

    #[test]
    fn mul_field() {
        // unsigned data * unsigned data
        test_field_op!(
            vec![  2u64,  3, 8,  2,   20,  3, 4],
            vec![ 55u64,  3, 1,  9,  106,  9, 0],
            vec![110u64,  9, 8, 18, 2120, 27, 0],
            *
        );

        // signed data * signed data
        test_field_op!(
            vec![  2i64,  3, -8,   2,    20,  -3,  0],
            vec![ 55i64, -3, -1,  -9,  -106,   9, -4],
            vec![110i64, -9,  8, -18, -2120, -27,  0],
            *
        );

        // float data * float data
        test_field_op!(
            vec![ 55.0, -3.0, -1.0,   9.0, -106.0,  9.0, 0.0],
            vec![  2.0,  3.0, -8.0,  -2.0,  -20.0,  3.0, 4.0],
            vec![110.0, -9.0,  8.0, -18.0, 2120.0, 27.0, 0.0],
            *
        );
    }

    #[test]
    fn div_field() {
        use std::f64::INFINITY as INF;
        use std::f64::NEG_INFINITY as NEGINF;

        // unsigned data / unsigned data
        test_field_op!(
            vec![ 55u64,   3,   8,   2,   20,   0,   4],
            vec![ 11u64,   2,   1,   5,  100,   3,   1],
            vec![  5u64,   1,   8,   0,    0,   0,   4],
            /
        );

        // signed data / signed data
        test_field_op!(
            vec![ 55i64,   -3,   -8,   2,  -20,   0,   4],
            vec![ 11i64,   -2,    1,   5,  100,  -3,   1],
            vec![  5i64,    1,   -8,   0,   -0,   0,   4],
            /
        );

        // float data / float data
        test_field_op!(
            vec![ 55.0, -3.0, -8.0,  2.0,  -20.0,  0.0, 4.0],
            vec![ 11.0, -2.0, -1.0,  5.0,    0.0, -3.0, 0.0],
            vec![  5.0,  1.5,  8.0,  0.4, NEGINF,  0.0, INF],
            /
        );
    }
}
