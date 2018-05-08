
use field::FieldType;
use ops::{TypeError, FieldTypeError};
use error::*;

macro_rules! impl_infer_ft_result {
    ($fn_name:tt, $infer_ty_fn_name:tt, $err_var:tt) => {

impl FieldType {
    pub(crate) fn $fn_name(self, left: FieldType) -> Result<BinOpTypes> {
        match self {
            FieldType::Unsigned => u64::$infer_ty_fn_name(left)
                .or(Err(FieldTypeError::$err_var(self, left).into())),
            FieldType::Signed   => i64::$infer_ty_fn_name(left)
                .or(Err(FieldTypeError::$err_var(self, left).into())),
            FieldType::Text     => Err(FieldTypeError::$err_var(self, left).into()),
            FieldType::Boolean  => bool::$infer_ty_fn_name(left)
                .or(Err(FieldTypeError::$err_var(self, left).into())),
            FieldType::Float    => f64::$infer_ty_fn_name(left)
                .or(Err(FieldTypeError::$err_var(self, left).into())),
        }
    }
}

    }
}
impl_infer_ft_result!(infer_ft_add_result, infer_add_result, Add);
impl_infer_ft_result!(infer_ft_sub_result, infer_sub_result, Sub);
impl_infer_ft_result!(infer_ft_mul_result, infer_mul_result, Mul);
impl_infer_ft_result!(infer_ft_div_result, infer_div_result, Div);

#[derive(Debug, Clone)]
pub(crate) struct BinOpTypes {
    pub(crate) left: Option<FieldType>,
    pub(crate) right: Option<FieldType>,
    pub(crate) output: FieldType,
}
impl BinOpTypes {
    fn new<L, R>(left: L, right: R, output: FieldType) -> BinOpTypes
        where L: Into<Option<FieldType>>, R: Into<Option<FieldType>>
    {
        BinOpTypes {
            left: left.into(),
            right: right.into(),
            output
        }
    }
}

// infers the result type when adding a `self` to type `ty`. Used for Rust types.
pub(crate) trait InferAddResult {
    fn infer_add_result(left: FieldType) -> Result<BinOpTypes>;
}
impl InferAddResult for u64 {
    fn infer_add_result(left: FieldType) -> Result<BinOpTypes> {
        match left {
            FieldType::Unsigned => Ok(BinOpTypes::new(None, None, FieldType::Unsigned)),
            FieldType::Signed   => Ok(BinOpTypes::new(None, FieldType::Signed, FieldType::Signed)),
            FieldType::Text     => Err(TypeError::Add("u64".into(), FieldType::Text).into()),
            FieldType::Boolean  => Ok(BinOpTypes::new(FieldType::Unsigned, None,
                    FieldType::Unsigned)),
            FieldType::Float    => Ok(BinOpTypes::new(None, FieldType::Float, FieldType::Float))
        }
    }
}
impl InferAddResult for i64 {
    fn infer_add_result(left: FieldType) -> Result<BinOpTypes> {
        match left {
            FieldType::Unsigned => Ok(BinOpTypes::new(FieldType::Signed, None, FieldType::Signed)),
            FieldType::Signed   => Ok(BinOpTypes::new(None, None, FieldType::Signed)),
            FieldType::Text     => Err(TypeError::Add("i64".into(), FieldType::Text).into()),
            FieldType::Boolean  => Ok(BinOpTypes::new(FieldType::Signed, None, FieldType::Signed)),
            FieldType::Float    => Ok(BinOpTypes::new(None, FieldType::Float, FieldType::Float))
        }
    }
}
impl InferAddResult for bool {
    fn infer_add_result(left: FieldType) -> Result<BinOpTypes> {
        match left {
            FieldType::Unsigned  => Ok(BinOpTypes::new(None, FieldType::Unsigned,
                    FieldType::Unsigned)),
            FieldType::Signed  => Ok(BinOpTypes::new(None, FieldType::Signed, FieldType::Signed)),
            FieldType::Text     => Err(TypeError::Add("bool".into(), FieldType::Text).into()),
            FieldType::Boolean  => Ok(BinOpTypes::new(None, None, FieldType::Boolean)),
            FieldType::Float    => Ok(BinOpTypes::new(None, FieldType::Float, FieldType::Float))
        }
    }
}
impl InferAddResult for f64 {
    fn infer_add_result(left: FieldType) -> Result<BinOpTypes> {
        match left {
            FieldType::Unsigned => Ok(BinOpTypes::new(FieldType::Float, None, FieldType::Float)),
            FieldType::Signed   => Ok(BinOpTypes::new(FieldType::Float, None, FieldType::Float)),
            FieldType::Text     => Err(TypeError::Add("f64".into(), FieldType::Text).into()),
            FieldType::Boolean  => Ok(BinOpTypes::new(FieldType::Float, None, FieldType::Float)),
            FieldType::Float    => Ok(BinOpTypes::new(None, None, FieldType::Float))
        }
    }
}

// infers the result type when subtracting a `self` from a type `ty`.
pub(crate) trait InferSubResult {
    fn infer_sub_result(left: FieldType) -> Result<BinOpTypes>;
}
impl InferSubResult for u64 {
    fn infer_sub_result(left: FieldType) -> Result<BinOpTypes> {
        match left {
            // unsigned - u64 can be negative
            FieldType::Unsigned => Ok(BinOpTypes::new(FieldType::Signed, FieldType::Signed,
                FieldType::Signed)),
            FieldType::Signed   => Ok(BinOpTypes::new(None, FieldType::Signed, FieldType::Signed)),
            FieldType::Text     => Err(TypeError::Sub("u64".into(), FieldType::Text).into()),
            // boolean - u64 can be negative
            FieldType::Boolean  => Ok(BinOpTypes::new(FieldType::Signed, FieldType::Signed,
                FieldType::Signed)),
            FieldType::Float    => Ok(BinOpTypes::new(None, FieldType::Float, FieldType::Float))
        }
    }
}
impl InferSubResult for i64 {
    fn infer_sub_result(left: FieldType) -> Result<BinOpTypes> {
        match left {
            FieldType::Unsigned => Ok(BinOpTypes::new(FieldType::Signed, None, FieldType::Signed)),
            FieldType::Signed   => Ok(BinOpTypes::new(None, None, FieldType::Signed)),
            FieldType::Text     => Err(TypeError::Sub("i64".into(), FieldType::Text).into()),
            FieldType::Boolean  => Ok(BinOpTypes::new(FieldType::Signed, None, FieldType::Signed)),
            FieldType::Float    => Ok(BinOpTypes::new(None, FieldType::Float, FieldType::Float))
        }
    }
}
impl InferSubResult for bool {
    fn infer_sub_result(left: FieldType) -> Result<BinOpTypes> {
        match left {
            FieldType::Unsigned  => Ok(BinOpTypes::new(FieldType::Signed, FieldType::Signed,
                    FieldType::Signed)),
            FieldType::Signed  => Ok(BinOpTypes::new(None, FieldType::Signed, FieldType::Signed)),
            FieldType::Text     => Err(TypeError::Add("bool".into(), FieldType::Text).into()),
            FieldType::Boolean  => Ok(BinOpTypes::new(None, None, FieldType::Boolean)),
            FieldType::Float    => Ok(BinOpTypes::new(None, FieldType::Float, FieldType::Float))
        }
    }
}
impl InferSubResult for f64 {
    fn infer_sub_result(left: FieldType) -> Result<BinOpTypes> {
        match left {
            FieldType::Unsigned => Ok(BinOpTypes::new(FieldType::Float, None, FieldType::Float)),
            FieldType::Signed   => Ok(BinOpTypes::new(FieldType::Float, None, FieldType::Float)),
            FieldType::Text     => Err(TypeError::Sub("f64".into(), FieldType::Text).into()),
            FieldType::Boolean  => Ok(BinOpTypes::new(FieldType::Float, None, FieldType::Float)),
            FieldType::Float    => Ok(BinOpTypes::new(None, None, FieldType::Float))
        }
    }
}


// infers the result type when multiplying a type `ty` by `self`.
pub(crate) trait InferMulResult {
    fn infer_mul_result(left: FieldType) -> Result<BinOpTypes>;
}
impl InferMulResult for u64 {
    fn infer_mul_result(left: FieldType) -> Result<BinOpTypes> {
        match left {
            FieldType::Unsigned => Ok(BinOpTypes::new(None, None, FieldType::Unsigned)),
            FieldType::Signed   => Ok(BinOpTypes::new(None, FieldType::Signed, FieldType::Signed)),
            FieldType::Text     => Err(TypeError::Mul("u64".into(), FieldType::Text).into()),
            FieldType::Boolean  => Ok(BinOpTypes::new(FieldType::Unsigned, None,
                FieldType::Unsigned)),
            FieldType::Float    => Ok(BinOpTypes::new(None, FieldType::Float, FieldType::Float))
        }
    }
}
impl InferMulResult for i64 {
    fn infer_mul_result(left: FieldType) -> Result<BinOpTypes> {
        match left {
            FieldType::Unsigned => Ok(BinOpTypes::new(FieldType::Signed, None, FieldType::Signed)),
            FieldType::Signed   => Ok(BinOpTypes::new(None, None, FieldType::Signed)),
            FieldType::Text     => Err(TypeError::Mul("i64".into(), FieldType::Text).into()),
            FieldType::Boolean  => Ok(BinOpTypes::new(FieldType::Signed, None, FieldType::Signed)),
            FieldType::Float    => Ok(BinOpTypes::new(None, FieldType::Float, FieldType::Float))
        }
    }
}
impl InferMulResult for bool {
    fn infer_mul_result(left: FieldType) -> Result<BinOpTypes> {
        match left {
            FieldType::Unsigned  => Ok(BinOpTypes::new(None, FieldType::Unsigned,
                    FieldType::Unsigned)),
            FieldType::Signed  => Ok(BinOpTypes::new(None, FieldType::Signed, FieldType::Signed)),
            FieldType::Text     => Err(TypeError::Add("bool".into(), FieldType::Text).into()),
            FieldType::Boolean  => Ok(BinOpTypes::new(None, None, FieldType::Boolean)),
            FieldType::Float    => Ok(BinOpTypes::new(None, FieldType::Float, FieldType::Float))
        }
    }
}
impl InferMulResult for f64 {
    fn infer_mul_result(left: FieldType) -> Result<BinOpTypes> {
        match left {
            FieldType::Unsigned => Ok(BinOpTypes::new(FieldType::Float, None, FieldType::Float)),
            FieldType::Signed   => Ok(BinOpTypes::new(FieldType::Float, None, FieldType::Float)),
            FieldType::Text     => Err(TypeError::Mul("f64".into(), FieldType::Text).into()),
            FieldType::Boolean  => Ok(BinOpTypes::new(FieldType::Float, None, FieldType::Float)),
            FieldType::Float    => Ok(BinOpTypes::new(None, None, FieldType::Float))
        }
    }
}


// infers the result type when dividing a type `ty` by `self`. always ends up as a floating-point
// (when operation is possible)
pub(crate) trait InferDivResult {
    fn infer_div_result(left: FieldType) -> Result<BinOpTypes>;
}
impl InferDivResult for u64 {
    fn infer_div_result(left: FieldType) -> Result<BinOpTypes> {
        match left {
            FieldType::Unsigned => Ok(BinOpTypes::new(FieldType::Float, FieldType::Float,
                FieldType::Float)),
            FieldType::Signed   => Ok(BinOpTypes::new(FieldType::Float, FieldType::Float,
                FieldType::Float)),
            FieldType::Text     => Err(TypeError::Div("u64".into(), FieldType::Text).into()),
            FieldType::Boolean  => Ok(BinOpTypes::new(FieldType::Float, FieldType::Float,
                FieldType::Float)),
            FieldType::Float    => Ok(BinOpTypes::new(None, FieldType::Float, FieldType::Float))
        }
    }
}
impl InferDivResult for i64 {
    fn infer_div_result(left: FieldType) -> Result<BinOpTypes> {
        match left {
            FieldType::Unsigned => Ok(BinOpTypes::new(FieldType::Float, FieldType::Float,
                FieldType::Float)),
            FieldType::Signed   => Ok(BinOpTypes::new(FieldType::Float, FieldType::Float,
                FieldType::Float)),
            FieldType::Text     => Err(TypeError::Div("i64".into(), FieldType::Text).into()),
            FieldType::Boolean  => Ok(BinOpTypes::new(FieldType::Float, FieldType::Float,
                FieldType::Float)),
            FieldType::Float    => Ok(BinOpTypes::new(None, FieldType::Float,
                FieldType::Float))
        }
    }
}
impl InferDivResult for bool {
    fn infer_div_result(left: FieldType) -> Result<BinOpTypes> {
        match left {
            FieldType::Unsigned  => Ok(BinOpTypes::new(FieldType::Float, FieldType::Float,
                FieldType::Float)),
            FieldType::Signed  => Ok(BinOpTypes::new(FieldType::Float, FieldType::Float,
                FieldType::Float)),
            FieldType::Text     => Err(TypeError::Add("bool".into(), FieldType::Text).into()),
            FieldType::Boolean  => Ok(BinOpTypes::new(None, None, FieldType::Boolean)),
            FieldType::Float    => Ok(BinOpTypes::new(None, FieldType::Float, FieldType::Float))
        }
    }
}
impl InferDivResult for f64 {
    fn infer_div_result(left: FieldType) -> Result<BinOpTypes> {
        match left {
            FieldType::Unsigned => Ok(BinOpTypes::new(FieldType::Float, None, FieldType::Float)),
            FieldType::Signed   => Ok(BinOpTypes::new(FieldType::Float, None, FieldType::Float)),
            FieldType::Text     => Err(TypeError::Div("f64".into(), FieldType::Text).into()),
            FieldType::Boolean  => Ok(BinOpTypes::new(FieldType::Float, None, FieldType::Float)),
            FieldType::Float    => Ok(BinOpTypes::new(None, None, FieldType::Float))
        }
    }
}
