/*!
Mathematical operations for `DataView` objects.
*/
use std::ops::{Add, Sub, Mul, Div};
use std::error::Error;
use std::fmt;

use field::{TypedFieldIdent, FieldType, FieldIdent};
use view::{DataView};
use store::{DataStore, AddData};
use error::*;
use masked::MaybeNa;
use apply::{ElemFn, ApplyToElem, FieldIndexSelector};

/// Error during data operations type inference.
#[derive(Debug)]
pub enum TypeError {
    /// Error during addition between Rust type (first argument, as string) and `FieldType`
    Add(String, FieldType),
    /// Error during subtraction between Rust type (first argument, as string) and `FieldType`
    Sub(String, FieldType),
    /// Error during multiplication between Rust type (first argument, as string) and `FieldType`
    Mul(String, FieldType),
    /// Error during division between Rust type (first argument, as string) and `FieldType`
    Div(String, FieldType),
}
impl fmt::Display for TypeError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            TypeError::Add(ref s, ft) => write!(f,
                "unable to add value of type {} to field of type {}", s, ft),
            TypeError::Sub(ref s, ft) => write!(f,
                "unable to subtract value of type {} from field of type {}", s, ft),
            TypeError::Mul(ref s, ft) => write!(f,
                "unable to multiply field of type {} by value of type {}", ft, s),
            TypeError::Div(ref s, ft) => write!(f,
                "unable to divide field of type {} by value of type {}", ft, s),
        }
    }
}
impl Error for TypeError {
    fn description(&self) -> &str {
        match *self {
            TypeError::Add(..) => "addition error",
            TypeError::Sub(..) => "subtraction error",
            TypeError::Mul(..) => "multiplication error",
            TypeError::Div(..) => "division error"
        }
    }

    fn cause(&self) -> Option<&Error> {
        None
    }
}
impl From<TypeError> for AgnesError {
    fn from(err: TypeError) -> AgnesError {
        AgnesError::Inference(err)
    }
}

macro_rules! impl_op_fn {
    ($($fn_name:tt)*) => {$(

struct $fn_name<'a, 'b, T> {
    target_ds: &'a mut DataStore,
    target_ident: &'b FieldIdent,
    term: T
}
impl<'a, 'b, T: PartialOrd> $fn_name<'a, 'b, T> {
    fn add_to_ds<O: PartialOrd>(&mut self, value: MaybeNa<O>) where DataStore: AddData<O> {
        self.target_ds.add(self.target_ident.clone(), value);
    }
}

    )*}
}

impl_op_fn!(AddFn SubFn MulFn DivFn);

macro_rules! impl_op {
    (
        $op:tt;
        $op_fn:tt;
        $op_str:expr;
        $infer_fn:tt;
        $op_elemfn_ty:tt;
        $dtype:ty;
        unsigned: $unsigned_calc:expr;
        signed: $signed_calc:expr;
        boolean: $bool_calc:expr;
        float: $float_calc:expr;
    ) => {
// START IMPL_OP

impl<'a, 'b> ElemFn for $op_elemfn_ty<'a, 'b, $dtype> {
    type Output = ();
    fn apply_unsigned(&mut self, value: MaybeNa<&u64>) {
        let new_value = value.map(|&val| $unsigned_calc(val, self.term));
        self.add_to_ds(new_value);
    }
    fn apply_signed(&mut self, value: MaybeNa<&i64>) {
        let new_value = value.map(|&val| $signed_calc(val, self.term));
        self.add_to_ds(new_value);
    }
    fn apply_text(&mut self, _: MaybeNa<&String>) { unreachable!() }
    fn apply_boolean(&mut self, value: MaybeNa<&bool>) {
        let new_value = value.map(|&val| $bool_calc(val, self.term));
        self.add_to_ds(new_value);
    }
    fn apply_float(&mut self, value: MaybeNa<&f64>) {
        let new_value = value.map(|&val| $float_calc(val, self.term));
        self.add_to_ds(new_value);
    }
}
impl<'a> $op<$dtype> for &'a DataView {
    type Output = Result<DataView>;
    fn $op_fn(self, rhs: $dtype) -> Result<DataView> {
        let mut fields = vec![];
        for &TypedFieldIdent { ref ident, ty } in self.field_types().iter() {
            fields.push(TypedFieldIdent {
                ident: FieldIdent::Name(format!("{} {} {}", ident.clone(), $op_str, rhs)),
                ty: <$dtype>::$infer_fn(ty)?
            });
        }
        if fields.is_empty() {
            return Err(AgnesError::DimensionMismatch(
                "unable to apply arithmetic operation to an empty dataview".into()));
        }
        let mut store = DataStore::with_fields(fields);
        for ((ident, vf), target_ident) in self.fields.iter().zip(store.fieldnames().iter()) {
            let frame = &self.frames[vf.frame_idx];
            for i in 0..frame.nrows() {
                self.apply_to_elem($op_elemfn_ty {
                    target_ds: &mut store,
                    target_ident: &target_ident.clone().into(),
                    term: rhs
                }, FieldIndexSelector(&ident, i))?;
            }
        }
        Ok(store.into())
    }
}
impl $op<$dtype> for DataView {
    type Output = Result<DataView>;
    fn $op_fn(self, rhs: $dtype) -> Result<DataView> {
        (&self).$op_fn(rhs)
    }
}

// END IMPL_OP
        }
}

macro_rules! impl_add {
    ($($t:tt)*) => (
        impl_op!(Add; add; "+"; infer_add_result; AddFn;$($t)*);
    )
}
macro_rules! impl_sub {
    ($($t:tt)*) => (
        impl_op!(Sub; sub; "-"; infer_sub_result; SubFn;$($t)*);
    )
}
macro_rules! impl_mul {
    ($($t:tt)*) => (
        impl_op!(Mul; mul; "*"; infer_mul_result; MulFn;$($t)*);
    )
}
macro_rules! impl_div {
    ($($t:tt)*) => (
        impl_op!(Div; div; "/"; infer_div_result; DivFn;$($t)*);
    )
}

// bool to float
fn btf(x: bool) -> f64 { if x { 1.0 } else { 0.0 } }

impl_add!(
    u64;
    unsigned: |x: u64, addend: u64| -> u64 { x + addend }; // unsigned + u64 -> u64
    signed: |x: i64, addend: u64| -> i64 { x + addend as i64 }; // signed + u64 -> i64
    boolean: |x: bool, addend: u64| -> u64 { x as u64 + addend }; // bool + u64 -> u64
    float: |x: f64, addend: u64| -> f64 { x + addend as f64 }; // float + u64 -> f64
);
impl_add!(
    i64;
    unsigned: |x: u64, addend: i64| -> i64 { x as i64 + addend }; // unsigned + i64 -> i64
    signed: |x: i64, addend: i64| -> i64 { x + addend }; // signed + i64 -> i64
    boolean: |x: bool, addend: i64| -> i64 { x as i64 + addend }; // boolean + i64 -> i64
    float: |x: f64, addend: i64| -> f64 { x + addend as f64 }; // float + i64 -> f64
);
impl_add!(
    f64;
    unsigned: |x: u64, addend: f64| -> f64 { x as f64 + addend }; // unsigned + f64 -> f64
    signed: |x: i64, addend: f64| -> f64 { x as f64 + addend }; // signed + f64 -> f64
    boolean: |x: bool, addend: f64| -> f64 { btf(x) + addend }; // boolean + f64 -> f64
    float: |x: f64, addend: f64| -> f64 { x + addend }; // float + f64 -> f64
);

impl_sub!(
    u64;
    unsigned: |x: u64, subend: u64| -> i64 { x as i64 - subend as i64 }; // unsigned - u64 -> i64
    signed: |x: i64, subend: u64| -> i64 { x - subend as i64 }; // signed - u64 -> i64
    boolean: |x: bool, subend: u64| -> i64 { x as i64 - subend as i64 }; // bool - u64 -> i64
    float: |x: f64, subend: u64| -> f64 { x - subend as f64 }; // float - u64 -> f64
);
impl_sub!(
    i64;
    unsigned: |x: u64, subend: i64| -> i64 { x as i64 - subend }; // unsigned - i64 -> i64
    signed: |x: i64, subend: i64| -> i64 { x - subend }; // signed - i64 -> i64
    boolean: |x: bool, subend: i64| -> i64 { x as i64 - subend }; // boolean - i64 -> i64
    float: |x: f64, subend: i64| -> f64 { x - subend as f64 }; // float - i64 -> f64
);
impl_sub!(
    f64;
    unsigned: |x: u64, subend: f64| -> f64 { x as f64 - subend }; // unsigned - f64 -> f64
    signed: |x: i64, subend: f64| -> f64 { x as f64 - subend }; // signed - f64 -> f64
    boolean: |x: bool, subend: f64| -> f64 { btf(x) - subend } ;// boolean - f64 -> f64
    float: |x: f64, subend: f64| -> f64 { x - subend }; // float - f64 -> f64
);

impl_mul!(
    u64;
    unsigned: |x: u64, mult: u64| -> u64 { x * mult }; // unsigned * u64 -> u64
    signed: |x: i64, mult: u64| -> i64 { x * mult as i64 }; // signed * u64 -> i64
    boolean: |x: bool, mult: u64| -> u64 { x as u64 * mult }; // bool * u64 -> u64
    float: |x: f64, mult: u64| -> f64 { x * mult as f64 }; // float * u64 -> f64
);
impl_mul!(
    i64;
    unsigned: |x: u64, mult: i64| -> i64 { x as i64 * mult }; // unsigned * i64 -> i64
    signed: |x: i64, mult: i64| -> i64 { x * mult }; // signed * i64 -> i64
    boolean: |x: bool, mult: i64| -> i64 { x as i64 * mult }; // boolean * i64 -> i64
    float: |x: f64, mult: i64| -> f64 { x * mult as f64 }; // float * i64 -> f64
);
impl_mul!(
    f64;
    unsigned: |x: u64, mult: f64| -> f64 { x as f64 * mult }; // unsigned * f64 -> f64
    signed: |x: i64, mult: f64| -> f64 { x as f64 * mult }; // signed * f64 -> f64
    boolean: |x: bool, mult: f64| -> f64 { btf(x) * mult }; // boolean * f64 -> f64
    float: |x: f64, mult: f64| -> f64 { x * mult }; // float * f64 -> f64
);

impl_div!(
    u64;
    unsigned: |x: u64, divisor: u64| -> f64 { x as f64 / divisor as f64 }; // unsigned / u64 -> f64
    signed: |x: i64, divisor: u64| -> f64 { x as f64 / divisor as f64 }; // signed / u64 -> f64
    boolean: |x: bool, divisor: u64| -> f64 { btf(x) / divisor as f64 }; // bool / u64 -> f64
    float: |x: f64, divisor: u64| -> f64 { x / divisor as f64 }; // float / u64 -> f64
);
impl_div!(
    i64;
    unsigned: |x: u64, divisor: i64| -> f64 { x as f64 / divisor as f64 }; // unsigned / i64 -> f64
    signed: |x: i64, divisor: i64| -> f64 { x as f64 / divisor as f64 }; // signed / i64 -> f64
    boolean: |x: bool, divisor: i64| -> f64 { btf(x) / divisor as f64 }; // boolean / i64 -> f64
    float: |x: f64, divisor: i64| -> f64 { x / divisor as f64 }; // float / i64 -> f64
);
impl_div!(
    f64;
    unsigned: |x: u64, divisor: f64| -> f64 { x as f64 / divisor }; // unsigned / f64 -> f64
    signed: |x: i64, divisor: f64| -> f64 { x as f64 / divisor }; // signed / f64 -> f64
    boolean: |x: bool, divisor: f64| -> f64 { btf(x) / divisor }; // boolean / f64 -> f64
    float: |x: f64, divisor: f64| -> f64 { x / divisor }; // float * f64 -> f64
);

// infers the result type when adding a `self` to type `ty`.
trait InferAddResult {
    fn infer_add_result(ty: FieldType) -> Result<FieldType>;
}
impl InferAddResult for u64 {
    fn infer_add_result(ty: FieldType) -> Result<FieldType> {
        match ty {
            FieldType::Unsigned => Ok(FieldType::Unsigned),
            FieldType::Signed   => Ok(FieldType::Signed),
            FieldType::Text     => Err(TypeError::Add("u64".into(), FieldType::Text).into()),
            FieldType::Boolean  => Ok(FieldType::Unsigned),
            FieldType::Float    => Ok(FieldType::Float)
        }
    }
}
impl InferAddResult for i64 {
    fn infer_add_result(ty: FieldType) -> Result<FieldType> {
        match ty {
            FieldType::Unsigned => Ok(FieldType::Signed),
            FieldType::Signed   => Ok(FieldType::Signed),
            FieldType::Text     => Err(TypeError::Add("i64".into(), FieldType::Text).into()),
            FieldType::Boolean  => Ok(FieldType::Signed),
            FieldType::Float    => Ok(FieldType::Float)
        }
    }
}
impl InferAddResult for f64 {
    fn infer_add_result(ty: FieldType) -> Result<FieldType> {
        match ty {
            FieldType::Unsigned => Ok(FieldType::Float),
            FieldType::Signed   => Ok(FieldType::Float),
            FieldType::Text     => Err(TypeError::Add("f64".into(), FieldType::Text).into()),
            FieldType::Boolean  => Ok(FieldType::Float),
            FieldType::Float    => Ok(FieldType::Float)
        }
    }
}

// infers the result type when subtracting a `self` from a type `ty`.
trait InferSubResult {
    fn infer_sub_result(ty: FieldType) -> Result<FieldType>;
}
impl InferSubResult for u64 {
    fn infer_sub_result(ty: FieldType) -> Result<FieldType> {
        match ty {
            FieldType::Unsigned => Ok(FieldType::Signed), // unsigned - u64 can be negative
            FieldType::Signed   => Ok(FieldType::Signed),
            FieldType::Text     => Err(TypeError::Sub("u64".into(), FieldType::Text).into()),
            FieldType::Boolean  => Ok(FieldType::Signed), // boolean - u64 can be negative
            FieldType::Float    => Ok(FieldType::Float)
        }
    }
}
impl InferSubResult for i64 {
    fn infer_sub_result(ty: FieldType) -> Result<FieldType> {
        match ty {
            FieldType::Unsigned => Ok(FieldType::Signed),
            FieldType::Signed   => Ok(FieldType::Signed),
            FieldType::Text     => Err(TypeError::Sub("i64".into(), FieldType::Text).into()),
            FieldType::Boolean  => Ok(FieldType::Signed),
            FieldType::Float    => Ok(FieldType::Float)
        }
    }
}
impl InferSubResult for f64 {
    fn infer_sub_result(ty: FieldType) -> Result<FieldType> {
        match ty {
            FieldType::Unsigned => Ok(FieldType::Float),
            FieldType::Signed   => Ok(FieldType::Float),
            FieldType::Text     => Err(TypeError::Sub("f64".into(), FieldType::Text).into()),
            FieldType::Boolean  => Ok(FieldType::Float),
            FieldType::Float    => Ok(FieldType::Float)
        }
    }
}


// infers the result type when multiplying a type `ty` by `self`.
trait InferMulResult {
    fn infer_mul_result(ty: FieldType) -> Result<FieldType>;
}
impl InferMulResult for u64 {
    fn infer_mul_result(ty: FieldType) -> Result<FieldType> {
        match ty {
            FieldType::Unsigned => Ok(FieldType::Unsigned),
            FieldType::Signed   => Ok(FieldType::Signed),
            FieldType::Text     => Err(TypeError::Mul("u64".into(), FieldType::Text).into()),
            FieldType::Boolean  => Ok(FieldType::Unsigned),
            FieldType::Float    => Ok(FieldType::Float)
        }
    }
}
impl InferMulResult for i64 {
    fn infer_mul_result(ty: FieldType) -> Result<FieldType> {
        match ty {
            FieldType::Unsigned => Ok(FieldType::Signed),
            FieldType::Signed   => Ok(FieldType::Signed),
            FieldType::Text     => Err(TypeError::Mul("i64".into(), FieldType::Text).into()),
            FieldType::Boolean  => Ok(FieldType::Signed),
            FieldType::Float    => Ok(FieldType::Float)
        }
    }
}
impl InferMulResult for f64 {
    fn infer_mul_result(ty: FieldType) -> Result<FieldType> {
        match ty {
            FieldType::Unsigned => Ok(FieldType::Float),
            FieldType::Signed   => Ok(FieldType::Float),
            FieldType::Text     => Err(TypeError::Mul("f64".into(), FieldType::Text).into()),
            FieldType::Boolean  => Ok(FieldType::Float),
            FieldType::Float    => Ok(FieldType::Float)
        }
    }
}


// infers the result type when dividing a type `ty` by `self`. always ends up as a floating-point
// (when operation is possible)
trait InferDivResult {
    fn infer_div_result(ty: FieldType) -> Result<FieldType>;
}
impl InferDivResult for u64 {
    fn infer_div_result(ty: FieldType) -> Result<FieldType> {
        match ty {
            FieldType::Unsigned => Ok(FieldType::Float),
            FieldType::Signed   => Ok(FieldType::Float),
            FieldType::Text     => Err(TypeError::Div("u64".into(), FieldType::Text).into()),
            FieldType::Boolean  => Ok(FieldType::Float),
            FieldType::Float    => Ok(FieldType::Float)
        }
    }
}
impl InferDivResult for i64 {
    fn infer_div_result(ty: FieldType) -> Result<FieldType> {
        match ty {
            FieldType::Unsigned => Ok(FieldType::Float),
            FieldType::Signed   => Ok(FieldType::Float),
            FieldType::Text     => Err(TypeError::Div("i64".into(), FieldType::Text).into()),
            FieldType::Boolean  => Ok(FieldType::Float),
            FieldType::Float    => Ok(FieldType::Float)
        }
    }
}
impl InferDivResult for f64 {
    fn infer_div_result(ty: FieldType) -> Result<FieldType> {
        match ty {
            FieldType::Unsigned => Ok(FieldType::Float),
            FieldType::Signed   => Ok(FieldType::Float),
            FieldType::Text     => Err(TypeError::Div("f64".into(), FieldType::Text).into()),
            FieldType::Boolean  => Ok(FieldType::Float),
            FieldType::Float    => Ok(FieldType::Float)
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use test_utils::*;

    #[test]
    fn add_scalar() {
        /* unsigned data */
        let data_vec = vec![2u64, 3, 8, 2, 20, 3, 0];

        // added to unsigned scalar; should remain an unsigned field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        let computed_dv: DataView = (dv.v("Foo") + 2u64).unwrap();
        assert_eq!(computed_dv.get_field_type(&"Foo + 2".into()).unwrap(), FieldType::Unsigned);
        unsigned::assert_vec_eq(&computed_dv, &"Foo + 2".into(),
            vec![4u64, 5, 10, 4, 22, 5, 2]
        );

        // added to signed scalar; should become a signed field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        let computed_dv: DataView = (dv.v("Foo") + -2i64).unwrap();
        assert_eq!(computed_dv.get_field_type(&"Foo + -2".into()).unwrap(), FieldType::Signed);
        signed::assert_vec_eq(&computed_dv, &"Foo + -2".into(),
            vec![0i64, 1, 6, 0, 18, 1, -2]
        );

        // added to floating point scalar; should become a floating point field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        let computed_dv: DataView = (dv.v("Foo") + 2.0).unwrap();
        assert_eq!(computed_dv.get_field_type(&"Foo + 2".into()).unwrap(), FieldType::Float);
        float::assert_vec_eq(&computed_dv, &"Foo + 2".into(),
            vec![4.0, 5.0, 10.0, 4.0, 22.0, 5.0, 2.0]
        );

        /* signed data */
        let data_vec = vec![2i64, -3, -8, 2, -20, 3, 0];

        // added to unsigned scalar; should remain a signed field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        let computed_dv: DataView = (dv.v("Foo") + 2u64).unwrap();
        assert_eq!(computed_dv.get_field_type(&"Foo + 2".into()).unwrap(), FieldType::Signed);
        signed::assert_vec_eq(&computed_dv, &"Foo + 2".into(),
            vec![4i64, -1, -6, 4, -18, 5, 2]
        );

        // added to signed scalar; should remain a signed field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        let computed_dv: DataView = (dv.v("Foo") + -2i64).unwrap();
        assert_eq!(computed_dv.get_field_type(&"Foo + -2".into()).unwrap(), FieldType::Signed);
        signed::assert_vec_eq(&computed_dv, &"Foo + -2".into(),
            vec![0i64, -5, -10, 0, -22, 1, -2]
        );

        // added to floating point scalar; should become a floating point field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        let computed_dv: DataView = (dv.v("Foo") + 2.0).unwrap();
        assert_eq!(computed_dv.get_field_type(&"Foo + 2".into()).unwrap(), FieldType::Float);
        float::assert_vec_eq(&computed_dv, &"Foo + 2".into(),
            vec![4.0, -1.0, -6.0, 4.0, -18.0, 5.0, 2.0]
        );

        /* boolean data */
        let data_vec = vec![true, false, false, true, false, true, true];

        // added to unsigned scalar; should become an unsigned field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        let computed_dv: DataView = (dv.v("Foo") + 2u64).unwrap();
        assert_eq!(computed_dv.get_field_type(&"Foo + 2".into()).unwrap(), FieldType::Unsigned);
        unsigned::assert_vec_eq(&computed_dv, &"Foo + 2".into(),
            vec![3u64, 2, 2, 3, 2, 3, 3]
        );

        // added to signed scalar; should become signed field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        let computed_dv: DataView = (dv.v("Foo") + -2i64).unwrap();
        assert_eq!(computed_dv.get_field_type(&"Foo + -2".into()).unwrap(), FieldType::Signed);
        signed::assert_vec_eq(&computed_dv, &"Foo + -2".into(),
            vec![-1i64, -2, -2, -1, -2, -1, -1]
        );

        // added to floating point scalar; should become a floating point field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        let computed_dv: DataView = (dv.v("Foo") + 2.0).unwrap();
        assert_eq!(computed_dv.get_field_type(&"Foo + 2".into()).unwrap(), FieldType::Float);
        float::assert_vec_eq(&computed_dv, &"Foo + 2".into(),
            vec![3.0, 2.0, 2.0, 3.0, 2.0, 3.0, 3.0]
        );

        /* floating point data */
        let data_vec = vec![2.0, -3.0, -8.0, 2.0, -20.0, 3.0, 0.0];

        // added to unsigned scalar; should remain a floating point field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        let computed_dv: DataView = (dv.v("Foo") + 2u64).unwrap();
        assert_eq!(computed_dv.get_field_type(&"Foo + 2".into()).unwrap(), FieldType::Float);
        float::assert_vec_eq(&computed_dv, &"Foo + 2".into(),
            vec![4.0, -1.0, -6.0, 4.0, -18.0, 5.0, 2.0]
        );

        // added to signed scalar; should remain a floating point field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        let computed_dv: DataView = (dv.v("Foo") + -2i64).unwrap();
        assert_eq!(computed_dv.get_field_type(&"Foo + -2".into()).unwrap(), FieldType::Float);
        float::assert_vec_eq(&computed_dv, &"Foo + -2".into(),
            vec![0.0, -5.0, -10.0, 0.0, -22.0, 1.0, -2.0]
        );

        // added to floating point scalar; should remain a floating point field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        let computed_dv: DataView = (dv.v("Foo") + 2.0).unwrap();
        assert_eq!(computed_dv.get_field_type(&"Foo + 2".into()).unwrap(), FieldType::Float);
        float::assert_vec_eq(&computed_dv, &"Foo + 2".into(),
            vec![4.0, -1.0, -6.0, 4.0, -18.0, 5.0, 2.0]
        );
    }

    #[test]
    fn sub_scalar() {
        /* unsigned data */
        let data_vec = vec![2u64, 3, 8, 2, 20, 3, 0];

        // subtract unsigned scalar; should become a signed field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        let computed_dv: DataView = (dv.v("Foo") - 2u64).unwrap();
        assert_eq!(computed_dv.get_field_type(&"Foo - 2".into()).unwrap(), FieldType::Signed);
        signed::assert_vec_eq(&computed_dv, &"Foo - 2".into(),
            vec![0i64, 1, 6, 0, 18, 1, -2]
        );

        // subtract signed scalar; should become a signed field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        let computed_dv: DataView = (dv.v("Foo") - -2i64).unwrap();
        assert_eq!(computed_dv.get_field_type(&"Foo - -2".into()).unwrap(), FieldType::Signed);
        signed::assert_vec_eq(&computed_dv, &"Foo - -2".into(),
            vec![4i64, 5, 10, 4, 22, 5, 2]
        );

        // subtract floating point scalar; should become a floating point field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        let computed_dv: DataView = (dv.v("Foo") - 2.0).unwrap();
        assert_eq!(computed_dv.get_field_type(&"Foo - 2".into()).unwrap(), FieldType::Float);
        float::assert_vec_eq(&computed_dv, &"Foo - 2".into(),
            vec![0.0, 1.0, 6.0, 0.0, 18.0, 1.0, -2.0]
        );

        /* signed data */
        let data_vec = vec![2i64, -3, -8, 2, -20, 3, 0];

        // subtract unsigned scalar; should remain a signed field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        let computed_dv: DataView = (dv.v("Foo") - 2u64).unwrap();
        assert_eq!(computed_dv.get_field_type(&"Foo - 2".into()).unwrap(), FieldType::Signed);
        signed::assert_vec_eq(&computed_dv, &"Foo - 2".into(),
            vec![0i64, -5, -10, 0, -22, 1, -2]
        );

        // subtract signed scalar; should remain a signed field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        let computed_dv: DataView = (dv.v("Foo") - -2i64).unwrap();
        assert_eq!(computed_dv.get_field_type(&"Foo - -2".into()).unwrap(), FieldType::Signed);
        signed::assert_vec_eq(&computed_dv, &"Foo - -2".into(),
            vec![4i64, -1, -6, 4, -18, 5, 2]
        );

        // subtract floating point scalar; should become a floating point field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        let computed_dv: DataView = (dv.v("Foo") - 2.0).unwrap();
        assert_eq!(computed_dv.get_field_type(&"Foo - 2".into()).unwrap(), FieldType::Float);
        float::assert_vec_eq(&computed_dv, &"Foo - 2".into(),
            vec![0.0, -5.0, -10.0, 0.0, -22.0, 1.0, -2.0]
        );

        /* boolean data */
        let data_vec = vec![true, false, false, true, false, true, true];

        // subtract unsigned scalar; should become a signed field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        let computed_dv: DataView = (dv.v("Foo") - 2u64).unwrap();
        assert_eq!(computed_dv.get_field_type(&"Foo - 2".into()).unwrap(), FieldType::Signed);
        signed::assert_vec_eq(&computed_dv, &"Foo - 2".into(),
            vec![-1i64, -2, -2, -1, -2, -1, -1]
        );

        // subtract signed scalar; should become signed field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        let computed_dv: DataView = (dv.v("Foo") - -2i64).unwrap();
        assert_eq!(computed_dv.get_field_type(&"Foo - -2".into()).unwrap(), FieldType::Signed);
        signed::assert_vec_eq(&computed_dv, &"Foo - -2".into(),
            vec![3i64, 2, 2, 3, 2, 3, 3]
        );

        // subtract floating point scalar; should become a floating point field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        let computed_dv: DataView = (dv.v("Foo") - 2.0).unwrap();
        assert_eq!(computed_dv.get_field_type(&"Foo - 2".into()).unwrap(), FieldType::Float);
        float::assert_vec_eq(&computed_dv, &"Foo - 2".into(),
            vec![-1.0, -2.0, -2.0, -1.0, -2.0, -1.0, -1.0]
        );

        /* floating point data */
        let data_vec = vec![2.0, -3.0, -8.0, 2.0, -20.0, 3.0, 0.0];

        // subtract unsigned scalar; should remain a floating point field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        let computed_dv: DataView = (dv.v("Foo") - 2u64).unwrap();
        assert_eq!(computed_dv.get_field_type(&"Foo - 2".into()).unwrap(), FieldType::Float);
        float::assert_vec_eq(&computed_dv, &"Foo - 2".into(),
            vec![0.0, -5.0, -10.0, 0.0, -22.0, 1.0, -2.0]
        );

        // subtract signed scalar; should remain a floating point field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        let computed_dv: DataView = (dv.v("Foo") - -2i64).unwrap();
        assert_eq!(computed_dv.get_field_type(&"Foo - -2".into()).unwrap(), FieldType::Float);
        float::assert_vec_eq(&computed_dv, &"Foo - -2".into(),
            vec![4.0, -1.0, -6.0, 4.0, -18.0, 5.0, 2.0]
        );

        // subtract floating point scalar; should remain a floating point field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        let computed_dv: DataView = (dv.v("Foo") - 2.0).unwrap();
        assert_eq!(computed_dv.get_field_type(&"Foo - 2".into()).unwrap(), FieldType::Float);
        float::assert_vec_eq(&computed_dv, &"Foo - 2".into(),
            vec![0.0, -5.0, -10.0, 0.0, -22.0, 1.0, -2.0]
        );
    }


    #[test]
    fn multiply_scalar() {
        /* unsigned data */
        let data_vec = vec![2u64, 3, 8, 2, 20, 3, 0];

        // multiplied by unsigned scalar; should remain an unsigned field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        let computed_dv: DataView = (dv.v("Foo") * 2u64).unwrap();
        assert_eq!(computed_dv.get_field_type(&"Foo * 2".into()).unwrap(), FieldType::Unsigned);
        unsigned::assert_vec_eq(&computed_dv, &"Foo * 2".into(),
            vec![4u64, 6, 16, 4, 40, 6, 0]
        );

        // multiplied by signed scalar; should become a signed field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        let computed_dv: DataView = (dv.v("Foo") * -2i64).unwrap();
        assert_eq!(computed_dv.get_field_type(&"Foo * -2".into()).unwrap(), FieldType::Signed);
        signed::assert_vec_eq(&computed_dv, &"Foo * -2".into(),
            vec![-4i64, -6, -16, -4, -40, -6, -0]
        );

        // multiplied by floating point scalar; should become a floating point field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        let computed_dv: DataView = (dv.v("Foo") * 2.0).unwrap();
        assert_eq!(computed_dv.get_field_type(&"Foo * 2".into()).unwrap(), FieldType::Float);
        float::assert_vec_eq(&computed_dv, &"Foo * 2".into(),
            vec![4.0, 6.0, 16.0, 4.0, 40.0, 6.0, 0.0]
        );

        /* signed data */
        let data_vec = vec![2i64, -3, -8, 2, -20, 3, 0];

        // multiplied by unsigned scalar; should remain a signed field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        let computed_dv: DataView = (dv.v("Foo") * 2u64).unwrap();
        assert_eq!(computed_dv.get_field_type(&"Foo * 2".into()).unwrap(), FieldType::Signed);
        signed::assert_vec_eq(&computed_dv, &"Foo * 2".into(),
            vec![4i64, -6, -16, 4, -40, 6, 0]
        );

        // multiplied by signed scalar; should remain a signed field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        let computed_dv: DataView = (dv.v("Foo") * -2i64).unwrap();
        assert_eq!(computed_dv.get_field_type(&"Foo * -2".into()).unwrap(), FieldType::Signed);
        signed::assert_vec_eq(&computed_dv, &"Foo * -2".into(),
            vec![-4i64, 6, 16, -4, 40, -6, -0]
        );

        // multiplied by floating point scalar; should become a floating point field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        let computed_dv: DataView = (dv.v("Foo") * 2.0).unwrap();
        assert_eq!(computed_dv.get_field_type(&"Foo * 2".into()).unwrap(), FieldType::Float);
        float::assert_vec_eq(&computed_dv, &"Foo * 2".into(),
            vec![4.0, -6.0, -16.0, 4.0, -40.0, 6.0, 0.0]
        );

        /* boolean data */
        let data_vec = vec![true, false, false, true, false, true, true];

        // multiplied by unsigned scalar; should become an unsigned field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        let computed_dv: DataView = (dv.v("Foo") * 2u64).unwrap();
        assert_eq!(computed_dv.get_field_type(&"Foo * 2".into()).unwrap(), FieldType::Unsigned);
        unsigned::assert_vec_eq(&computed_dv, &"Foo * 2".into(),
            vec![2u64, 0, 0, 2, 0, 2, 2]
        );

        // multiplied by signed scalar; should become signed field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        let computed_dv: DataView = (dv.v("Foo") * -2i64).unwrap();
        assert_eq!(computed_dv.get_field_type(&"Foo * -2".into()).unwrap(), FieldType::Signed);
        signed::assert_vec_eq(&computed_dv, &"Foo * -2".into(),
            vec![-2i64, 0, 0, -2, 0, -2, -2]
        );

        // multiplied by floating point scalar; should become a floating point field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        let computed_dv: DataView = (dv.v("Foo") * 2.0).unwrap();
        assert_eq!(computed_dv.get_field_type(&"Foo * 2".into()).unwrap(), FieldType::Float);
        float::assert_vec_eq(&computed_dv, &"Foo * 2".into(),
            vec![2.0, 0.0, 0.0, 2.0, 0.0, 2.0, 2.0]
        );

        /* floating point data */
        let data_vec = vec![2.0, -3.0, -8.0, 2.0, -20.0, 3.0, 0.0];

        // multiplied by unsigned scalar; should remain a floating point field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        let computed_dv: DataView = (dv.v("Foo") * 2u64).unwrap();
        assert_eq!(computed_dv.get_field_type(&"Foo * 2".into()).unwrap(), FieldType::Float);
        float::assert_vec_eq(&computed_dv, &"Foo * 2".into(),
            vec![4.0, -6.0, -16.0, 4.0, -40.0, 6.0, 0.0]
        );

        // multiplied by signed scalar; should remain a floating point field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        let computed_dv: DataView = (dv.v("Foo") * -2i64).unwrap();
        assert_eq!(computed_dv.get_field_type(&"Foo * -2".into()).unwrap(), FieldType::Float);
        float::assert_vec_eq(&computed_dv, &"Foo * -2".into(),
            vec![-4.0, 6.0, 16.0, -4.0, 40.0, -6.0, 0.0]
        );

        // multiplied by floating point scalar; should remain a floating point field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        let computed_dv: DataView = (dv.v("Foo") * 2.0).unwrap();
        assert_eq!(computed_dv.get_field_type(&"Foo * 2".into()).unwrap(), FieldType::Float);
        float::assert_vec_eq(&computed_dv, &"Foo * 2".into(),
            vec![4.0, -6.0, -16.0, 4.0, -40.0, 6.0, 0.0]
        );
    }


    #[test]
    fn div_scalar() {

        /* unsigned data */
        let data_vec = vec![2u64, 3, 8, 2, 20, 3, 0];

        // divide by unsigned scalar; should become a floating point field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        let computed_dv: DataView = (dv.v("Foo") / 2u64).unwrap();
        assert_eq!(computed_dv.get_field_type(&"Foo / 2".into()).unwrap(), FieldType::Float);
        float::assert_vec_eq(&computed_dv, &"Foo / 2".into(),
            vec![1.0, 1.5, 4.0, 1.0, 10.0, 1.5, 0.0]
        );

        // divide by signed scalar; should become a floating point field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        let computed_dv: DataView = (dv.v("Foo") / -2i64).unwrap();
        assert_eq!(computed_dv.get_field_type(&"Foo / -2".into()).unwrap(), FieldType::Float);
        float::assert_vec_eq(&computed_dv, &"Foo / -2".into(),
            vec![-1.0, -1.5, -4.0, -1.0, -10.0, -1.5, 0.0]
        );

        // divide by floating point scalar; should become a floating point field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        let computed_dv: DataView = (dv.v("Foo") / 2.0).unwrap();
        assert_eq!(computed_dv.get_field_type(&"Foo / 2".into()).unwrap(), FieldType::Float);
        float::assert_vec_eq(&computed_dv, &"Foo / 2".into(),
            vec![1.0, 1.5, 4.0, 1.0, 10.0, 1.5, 0.0]
        );

        /* signed data */
        let data_vec = vec![2i64, -3, -8, 2, -20, 3, 0];

        // divide by unsigned scalar; should become a floating point field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        let computed_dv: DataView = (dv.v("Foo") / 2u64).unwrap();
        assert_eq!(computed_dv.get_field_type(&"Foo / 2".into()).unwrap(), FieldType::Float);
        float::assert_vec_eq(&computed_dv, &"Foo / 2".into(),
            vec![1.0, -1.5, -4.0, 1.0, -10.0, 1.5, 0.0]
        );

        // divide by signed scalar; should become a floating point field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        let computed_dv: DataView = (dv.v("Foo") / -2i64).unwrap();
        assert_eq!(computed_dv.get_field_type(&"Foo / -2".into()).unwrap(), FieldType::Float);
        float::assert_vec_eq(&computed_dv, &"Foo / -2".into(),
            vec![-1.0, 1.5, 4.0, -1.0, 10.0, -1.5, 0.0]
        );

        // divide by floating point scalar; should become a floating point field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        let computed_dv: DataView = (dv.v("Foo") / 2.0).unwrap();
        assert_eq!(computed_dv.get_field_type(&"Foo / 2".into()).unwrap(), FieldType::Float);
        float::assert_vec_eq(&computed_dv, &"Foo / 2".into(),
            vec![1.0, -1.5, -4.0, 1.0, -10.0, 1.5, 0.0]
        );

        /* boolean data */
        let data_vec = vec![true, false, false, true, false, true, true];

        // divide by unsigned scalar; should become a floating point field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        let computed_dv: DataView = (dv.v("Foo") / 2u64).unwrap();
        assert_eq!(computed_dv.get_field_type(&"Foo / 2".into()).unwrap(), FieldType::Float);
        float::assert_vec_eq(&computed_dv, &"Foo / 2".into(),
            vec![0.5, 0.0, 0.0, 0.5, 0.0, 0.5, 0.5]
        );

        // divide by signed scalar; should become a floating point field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        let computed_dv: DataView = (dv.v("Foo") / -2i64).unwrap();
        assert_eq!(computed_dv.get_field_type(&"Foo / -2".into()).unwrap(), FieldType::Float);
        float::assert_vec_eq(&computed_dv, &"Foo / -2".into(),
            vec![-0.5, 0.0, 0.0, -0.5, 0.0, -0.5, -0.5]
        );

        // divide by floating point scalar; should become a floating point field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        let computed_dv: DataView = (dv.v("Foo") / 2.0).unwrap();
        assert_eq!(computed_dv.get_field_type(&"Foo / 2".into()).unwrap(), FieldType::Float);
        float::assert_vec_eq(&computed_dv, &"Foo / 2".into(),
            vec![0.5, 0.0, 0.0, 0.5, 0.0, 0.5, 0.5]
        );

        /* floating point data */
        let data_vec = vec![2.0, -3.0, -8.0, 2.0, -20.0, 3.0, 0.0];

        // divide by unsigned scalar; should remain a floating point field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        let computed_dv: DataView = (dv.v("Foo") / 2u64).unwrap();
        assert_eq!(computed_dv.get_field_type(&"Foo / 2".into()).unwrap(), FieldType::Float);
        float::assert_vec_eq(&computed_dv, &"Foo / 2".into(),
            vec![1.0, -1.5, -4.0, 1.0, -10.0, 1.5, 0.0]
        );

        // divide by signed scalar; should remain a floating point field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        let computed_dv: DataView = (dv.v("Foo") / -2i64).unwrap();
        assert_eq!(computed_dv.get_field_type(&"Foo / -2".into()).unwrap(), FieldType::Float);
        float::assert_vec_eq(&computed_dv, &"Foo / -2".into(),
            vec![-1.0, 1.5, 4.0, -1.0, 10.0, -1.5, -0.0]
        );

        // divide by floating point scalar; should remain a floating point field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        let computed_dv: DataView = (dv.v("Foo") / 2.0).unwrap();
        assert_eq!(computed_dv.get_field_type(&"Foo / 2".into()).unwrap(), FieldType::Float);
        float::assert_vec_eq(&computed_dv, &"Foo / 2".into(),
            vec![1.0, -1.5, -4.0, 1.0, -10.0, 1.5, 0.0]
        );

        // extra divide-by-zero check
        use std::f64::INFINITY as INF;
        use std::f64::NEG_INFINITY as NEGINF;
        // use non-zero data vector, since 0 / 0 is NaN
        // TODO: use 0 in data vec if we ever implement NaN-agnostic matching
        let data_vec = vec![2i64, -3, -8, 2, -20, 3, 1];
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        let computed_dv: DataView = (dv.v("Foo") / 0u64).unwrap();
        assert_eq!(computed_dv.get_field_type(&"Foo / 0".into()).unwrap(), FieldType::Float);
        float::assert_vec_eq(&computed_dv, &"Foo / 0".into(),
            vec![INF, NEGINF, NEGINF, INF, NEGINF, INF, INF]
        );

    }
}
