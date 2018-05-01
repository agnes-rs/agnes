/*!
Mathematical operations for `DataView` objects.
*/
use std::ops::{Add, Sub, Mul, Div};
use std::error::Error;
use std::fmt;

use field::{TypedFieldIdent, DataType, FieldType, FieldIdent};
use view::{DataView};
use store::{DataStore, AddData};
use error::*;
use masked::MaybeNa;
use apply::{DataIndex, FieldSelector};

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
        AgnesError::TypeInference(err)
    }
}

/// Error during data operations type inference between fields.
#[derive(Debug)]
pub enum FieldTypeError {
    /// Error during addition between two `FieldType`s
    Add(FieldType, FieldType),
    /// Error during subtraction between two `FieldType`s
    Sub(FieldType, FieldType),
    /// Error during multiplication between two `FieldType`s
    Mul(FieldType, FieldType),
    /// Error during division between two `FieldType`s
    Div(FieldType, FieldType),
}
impl fmt::Display for FieldTypeError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            FieldTypeError::Add(left, right) => write!(f,
                "unable to add field of type {} to field of type {}", left, right),
            FieldTypeError::Sub(left, right) => write!(f,
                "unable to subtract field of type {} from field of type {}", right, left),
            FieldTypeError::Mul(left, right) => write!(f,
                "unable to multiply field of type {} by field of type {}", left, right),
            FieldTypeError::Div(left, right) => write!(f,
                "unable to divide field of type {} by field of type {}", left, right),
        }
    }
}
impl Error for FieldTypeError {
    fn description(&self) -> &str {
        match *self {
            FieldTypeError::Add(..) => "field addition error",
            FieldTypeError::Sub(..) => "field subtraction error",
            FieldTypeError::Mul(..) => "field multiplication error",
            FieldTypeError::Div(..) => "field division error"
        }
    }

    fn cause(&self) -> Option<&Error> {
        None
    }
}
impl From<FieldTypeError> for AgnesError {
    fn from(err: FieldTypeError) -> AgnesError {
        AgnesError::FieldTypeInference(err)
    }
}



macro_rules! impl_op_fn {
    ($($fn_name:tt)*) => {$(

struct $fn_name<'a, 'b, T> {
    target_ds: &'a mut DataStore,
    target_ident: &'b FieldIdent,
    term: T
}
impl<'a, 'b, T: DataType> $fn_name<'a, 'b, T> {
    fn add_to_ds<O: DataType>(&mut self, value: MaybeNa<O>) where DataStore: AddData<O> {
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
        $op_fieldfn_ty:tt;
        $dtype:ty;
        unsigned: $unsigned_calc:expr;
        signed: $signed_calc:expr;
        boolean: $bool_calc:expr;
        float: $float_calc:expr;
    ) => {
// START IMPL_OP

impl<'a, 'b> FieldFn for $op_fieldfn_ty<'a, 'b, $dtype> {

    type Output = ();
    fn apply_unsigned<T: DataIndex<u64>>(&mut self, field: &T) {
        for i in 0..field.len() {
            let new_value = field.get_data(i).unwrap().map(|&val| $unsigned_calc(val, self.term));
            self.add_to_ds(new_value);
        }
    }
    fn apply_signed<T: DataIndex<i64>>(&mut self, field: &T) {
        for i in 0..field.len() {
            let new_value = field.get_data(i).unwrap().map(|&val| $signed_calc(val, self.term));
            self.add_to_ds(new_value);
        }
    }
    fn apply_text<T: DataIndex<String>>(&mut self, _: &T) { unreachable!() }
    fn apply_boolean<T: DataIndex<bool>>(&mut self, field: &T) {
        for i in 0..field.len() {
            let new_value = field.get_data(i).unwrap().map(|&val| $bool_calc(val, self.term));
            self.add_to_ds(new_value);
        }
    }
    fn apply_float<T: DataIndex<f64>>(&mut self, field: &T) {
        for i in 0..field.len() {
            let new_value = field.get_data(i).unwrap().map(|&val| $float_calc(val, self.term));
            self.add_to_ds(new_value);
        }
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
        for ((ident, _), target_ident) in self.fields.iter().zip(store.fieldnames().iter()) {
            // let frame = &self.frames[vf.frame_idx];
            self.apply_to_field(
                $op_fieldfn_ty {
                    target_ds: &mut store,
                    target_ident: &target_ident.clone().into(),
                    term: rhs
                },
                FieldSelector(&ident)
            )?;
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

struct Add2Fn<'a, 'b> {
    target_ds: &'a mut DataStore,
    target_ident: &'b FieldIdent,
}
impl<'a, 'b> Add2Fn<'a, 'b> {
    fn add_to_ds<O: DataType>(&mut self, value: MaybeNa<O>) where DataStore: AddData<O> {
        self.target_ds.add(self.target_ident.clone(), value);
    }
}
impl<'a, 'b> Field2Fn for Add2Fn<'a, 'b> {
    type Output = ();
    fn apply_unsigned<T: DataIndex<u64>>(&mut self, field: &(&T, &T)) {
        debug_assert_eq!(field.0.len(), field.1.len());
        for i in 0..field.0.len() {
            let new_value = match (field.0.get_data(i).unwrap(), field.1.get_data(i).unwrap()) {
                (MaybeNa::Exists(l), MaybeNa::Exists(r)) => MaybeNa::Exists(l + r),
                _ => MaybeNa::Na
            };
            self.add_to_ds(new_value);
        }
    }
    fn apply_signed<T: DataIndex<i64>>(&mut self, field: &(&T, &T)) {
        debug_assert_eq!(field.0.len(), field.1.len());
        for i in 0..field.0.len() {
            let new_value = match (field.0.get_data(i).unwrap(), field.1.get_data(i).unwrap()) {
                (MaybeNa::Exists(l), MaybeNa::Exists(r)) => MaybeNa::Exists(l + r),
                _ => MaybeNa::Na
            };
            self.add_to_ds(new_value);
        }
    }
    fn apply_text<T: DataIndex<String>>(&mut self, _: &(&T, &T)) { unreachable!() }
    fn apply_boolean<T: DataIndex<bool>>(&mut self, field: &(&T, &T)) {
        debug_assert_eq!(field.0.len(), field.1.len());
        for i in 0..field.0.len() {
            let new_value = match (field.0.get_data(i).unwrap(), field.1.get_data(i).unwrap()) {
                (MaybeNa::Exists(l), MaybeNa::Exists(r)) => MaybeNa::Exists(l | r),
                _ => MaybeNa::Na
            };
            self.add_to_ds(new_value);
        }
    }
    fn apply_float<T: DataIndex<f64>>(&mut self, field: &(&T, &T)) {
        debug_assert_eq!(field.0.len(), field.1.len());
        for i in 0..field.0.len() {
            let new_value = match (field.0.get_data(i).unwrap(), field.1.get_data(i).unwrap()) {
                (MaybeNa::Exists(l), MaybeNa::Exists(r)) => MaybeNa::Exists(l + r),
                _ => MaybeNa::Na
            };
            self.add_to_ds(new_value);
        }
    }
}
impl<'a, 'b> Add<&'b DataView> for &'a DataView {
    type Output = Result<DataView>;
    fn add(self, rhs: &'b DataView) -> Result<DataView> {
        // check dimensions
        if self.nrows() != rhs.nrows() {
            return Err(AgnesError::DimensionMismatch(
                "unable to apply arithmetic operation between dataviews of different number \
                of records".into()
            ));
        }
        if self.nfields() == 0 || rhs.nfields() == 0 {
            return Err(AgnesError::DimensionMismatch(
                "unable to apply arithmetic operation to an empty dataview".into()
            ));
        }
        if self.nfields() > 1 && rhs.nfields() > 1 && self.nfields() != rhs.nfields() {
            return Err(AgnesError::DimensionMismatch(
                "unable to apply arithmetic operation between non-single-field dataviews unless \
                each has the same number of fields".into()
            ));
        }

        struct FieldInfo {
            target_field: TypedFieldIdent,
            left_ident: FieldIdent,
            rght_ident: FieldIdent,
        }
        let mut fields: Vec<FieldInfo> = vec![];
        if self.nfields() > 1 && self.nfields() == rhs.nfields() {
            // n x n
            for ((left_ident, left_vf), (rght_ident, rght_vf))
                in self.fields.iter().zip(rhs.fields.iter())
            {
                // idents exist by construction, unwrap is safe
                let left_ty = self.frames[left_vf.frame_idx].get_field_type(&left_vf.rident.ident)
                    .unwrap();
                let rght_ty = self.frames[rght_vf.frame_idx].get_field_type(&rght_vf.rident.ident)
                    .unwrap();
                fields.push(FieldInfo {
                    target_field: TypedFieldIdent {
                        ident: FieldIdent::Name(format!("{} {} {}", left_ident.clone(), "+",
                            rght_ident.clone())),
                        ty: left_ty.infer_ft_add_result(rght_ty)?
                    },
                    left_ident: left_ident.clone(),
                    rght_ident: rght_ident.clone(),
                })
            }
        } else {
            // due to above dimension checking, this is either n x 1, 1 x n, or 1 x 1
            for TypedFieldIdent { ident: left_ident, ty: left_ty } in self.field_types() {
                for TypedFieldIdent { ident: rght_ident, ty: rght_ty } in rhs.field_types() {
                    fields.push(FieldInfo {
                        target_field: TypedFieldIdent {
                            ident: FieldIdent::Name(format!("{} {} {}", left_ident.clone(), "+",
                                rght_ident.clone())),
                            ty: left_ty.infer_ft_add_result(rght_ty)?
                        },
                        left_ident: left_ident.clone(),
                        rght_ident,
                    })
                }
            }
        }
        let mut store = DataStore::with_field_iter(fields.iter().map(|f| f.target_field.clone()));

        for FieldInfo { target_field, left_ident, rght_ident } in fields {
            (self, rhs).apply_to_field2(
                Add2Fn {
                    target_ds: &mut store,
                    target_ident: &target_field.ident,
                },
                (
                    FieldSelector(&left_ident),
                    FieldSelector(&rght_ident),
                )
            )?;
        }


        // for (ident, vf) in self.fields.iter() {
        //     let frame = &self.frames[vf.frame_idx];
        //     for i in 0..frame.nrows() {
        //         self.apply_to_elem($op_elemfn_ty {
        //             target_ds: &mut store,
        //             target_ident: &ident,
        //             term: rhs
        //         }, FieldIndexSelector(&ident, i))?;
        //     }
        // }
        Ok(store.into())
    }
}
impl Add<DataView> for DataView {
    type Output = Result<DataView>;
    fn add(self, rhs: DataView) -> Result<DataView> {
        (&self).add(&rhs)
    }
}
impl<'a> Add<&'a DataView> for DataView {
    type Output = Result<DataView>;
    fn add(self, rhs: &'a DataView) -> Result<DataView> {
        (&self).add(rhs)
    }
}
impl<'a> Add<DataView> for &'a DataView {
    type Output = Result<DataView>;
    fn add(self, rhs: DataView) -> Result<DataView> {
        self.add(&rhs)
    }
}
// impl $op<$dtype> for DataView {
//     type Output = Result<DataView>;
//     fn $op_fn(self, rhs: $dtype) -> Result<DataView> {
//         (&self).$op_fn(rhs)
//     }
// }

macro_rules! impl_infer_ft_result {
    ($fn_name:tt, $infer_ty_fn_name:tt, $err_var:tt) => {

impl FieldType {
    fn $fn_name(self, other: FieldType) -> Result<FieldType> {
        match self {
            FieldType::Unsigned => u64::$infer_ty_fn_name(other)
                .or(Err(FieldTypeError::$err_var(self, other).into())),
            FieldType::Signed   => i64::$infer_ty_fn_name(other)
                .or(Err(FieldTypeError::$err_var(self, other).into())),
            FieldType::Text     => Err(FieldTypeError::$err_var(self, other).into()),
            FieldType::Boolean  => Ok(other),
            FieldType::Float    => f64::$infer_ty_fn_name(other)
                .or(Err(FieldTypeError::$err_var(self, other).into())),
        }
    }
}

    }
}
impl_infer_ft_result!(infer_ft_add_result, infer_add_result, Add);
impl_infer_ft_result!(infer_ft_sub_result, infer_sub_result, Sub);
impl_infer_ft_result!(infer_ft_mul_result, infer_mul_result, Mul);
impl_infer_ft_result!(infer_ft_div_result, infer_div_result, Div);


// infers the result type when adding a `self` to type `ty`. Used for Rust types.
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

    macro_rules! test_view_op {
        ($left:expr, $right:expr, $result:expr, $op:expr, $strop:expr, $result_ty:expr,
                $test_mod:ident) =>
        {{
            let data_vec1 = $left;
            let data_vec2 = $right;
            let dv1 = data_vec1.clone().merged_with_sample_emp_table("Foo");
            let dv2 = data_vec2.clone().merged_with_sample_emp_table("Bar");
            let computed_dv: DataView = ($op(dv1.v("Foo"), dv2.v("Bar"))).unwrap();
            let target_ident = FieldIdent::Name(format!("Foo {} Bar", $strop));
            assert_eq!(computed_dv.get_field_type(&target_ident).unwrap(), $result_ty);
            $test_mod::assert_vec_eq(&computed_dv, &target_ident, $result);
        }}
    }

    #[test]
    fn add_field() {
        // unsigned data + unsigned data -> unsigned
        test_view_op!(
            vec![2u64,  3, 8,  2,  20,  3, 0],
            vec![55u64, 3, 1,  9, 106,  9, 0],
            vec![57u64, 6, 9, 11, 126, 12, 0],
            |dv1, dv2| dv1 + dv2, "+", FieldType::Unsigned, unsigned
        );

        // unsigned data + signed data -> signed
        // test_view_op!(
        //     vec![2u64,   3,  8,  2,   20,  3, 0],
        //     vec![55i64, -3, -1,  9, -106,  9, 0],
        //     vec![57i64,  0,  7, 11,  -86, 12, 0],
        //     |dv1, dv2| dv1 + dv2, "+", FieldType::Signed, signed
        // );


    }
}
