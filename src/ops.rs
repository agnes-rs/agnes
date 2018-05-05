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
use masked::{MaybeNa};
use apply::{DataIndex, ReduceDataIndex, ApplyFieldReduce, FieldApplyTo, FieldReduceFn, FieldMapFn,
        Select, OwnedOrRef};

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

impl<'a, 'b> FieldMapFn for $op_fieldfn_ty<'a, 'b, $dtype> {

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
            let bin_op_types = <$dtype>::$infer_fn(ty)?;
            fields.push(TypedFieldIdent {
                ident: FieldIdent::Name(format!("{} {} {}", ident.clone(), $op_str, rhs)),
                ty: bin_op_types.output
            });
        }
        if fields.is_empty() {
            return Err(AgnesError::DimensionMismatch(
                "unable to apply arithmetic operation to an empty dataview".into()));
        }
        let mut store = DataStore::with_fields(fields);
        for ((ident, _), target_ident) in self.fields.iter().zip(store.fieldnames().iter()) {
            // let frame = &self.frames[vf.frame_idx];
            self.field_apply_to(
                &mut $op_fieldfn_ty {
                    target_ds: &mut store,
                    target_ident: &target_ident.clone().into(),
                    term: rhs
                },
                &ident
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

// unsigned to bool
fn utb(x: u64) -> bool { if x > 0 { true } else { false } }
// signed to bool
fn itb(x: i64) -> bool { if x == 0 { false } else { true } }
// float to bool
fn ftb(x: f64) -> bool { if x == 0.0 { false } else { true } }

// bool to unsigned
fn btu(x: bool) -> u64 { if x { 1 } else { 0 } }
// bool to signed
fn bti(x: bool) -> i64 { if x { 1 } else { 0 } }
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

trait Convert: Sized {
    fn convert(&self, conversion: Option<FieldType>) -> Option<Self>;
}

impl<'a> Convert for ReduceDataIndex<'a> {
    fn convert(&self, conversion: Option<FieldType>) -> Option<ReduceDataIndex<'a>> {
        match (self, conversion) {
            // unsigned -> ?? conversions
            (&ReduceDataIndex::Unsigned(_), Some(FieldType::Unsigned))
                | (&ReduceDataIndex::Unsigned(_), None) =>
            {
                None
            },
            (&ReduceDataIndex::Unsigned(ref data), Some(FieldType::Signed)) => {
                Some(ReduceDataIndex::Signed(OwnedOrRef::Owned(Box::new(
                    (0..data.len()).map(|idx| {
                        data.get_data(idx).unwrap().map(|&x| x as i64)
                    }).collect::<Vec<_>>()
                ))))
            },
            (&ReduceDataIndex::Unsigned(ref data), Some(FieldType::Text)) => {
                Some(ReduceDataIndex::Text(OwnedOrRef::Owned(Box::new(
                    (0..data.len()).map(|idx| {
                        data.get_data(idx).unwrap().map(|&x| format!("{}", x))
                    }).collect::<Vec<_>>()
                ))))
            },
            (&ReduceDataIndex::Unsigned(ref data), Some(FieldType::Boolean)) => {
                Some(ReduceDataIndex::Boolean(OwnedOrRef::Owned(Box::new(
                    (0..data.len()).map(|idx| {
                        data.get_data(idx).unwrap().map(|&x| utb(x))
                    }).collect::<Vec<_>>()
                ))))
            },
            (&ReduceDataIndex::Unsigned(ref data), Some(FieldType::Float)) => {
                Some(ReduceDataIndex::Float(OwnedOrRef::Owned(Box::new(
                    (0..data.len()).map(|idx| {
                        data.get_data(idx).unwrap().map(|&x| x as f64)
                    }).collect::<Vec<_>>()
                ))))
            },

            // signed -> ?? conversions
            (&ReduceDataIndex::Signed(_), Some(FieldType::Signed))
                | (&ReduceDataIndex::Signed(_), None) =>
            {
                None
            },
            (&ReduceDataIndex::Signed(ref data), Some(FieldType::Unsigned)) => {
                Some(ReduceDataIndex::Unsigned(OwnedOrRef::Owned(Box::new(
                    (0..data.len()).map(|idx| {
                        data.get_data(idx).unwrap().map(|&x| if x > 0 { x as u64 } else { 0 })
                    }).collect::<Vec<_>>()
                ))))
            },
            (&ReduceDataIndex::Signed(ref data), Some(FieldType::Text)) => {
                Some(ReduceDataIndex::Text(OwnedOrRef::Owned(Box::new(
                    (0..data.len()).map(|idx| {
                        data.get_data(idx).unwrap().map(|&x| format!("{}", x))
                    }).collect::<Vec<_>>()
                ))))
            },
            (&ReduceDataIndex::Signed(ref data), Some(FieldType::Boolean)) => {
                Some(ReduceDataIndex::Boolean(OwnedOrRef::Owned(Box::new(
                    (0..data.len()).map(|idx| {
                        data.get_data(idx).unwrap().map(|&x| itb(x))
                    }).collect::<Vec<_>>()
                ))))
            },
            (&ReduceDataIndex::Signed(ref data), Some(FieldType::Float)) => {
                Some(ReduceDataIndex::Float(OwnedOrRef::Owned(Box::new(
                    (0..data.len()).map(|idx| {
                        data.get_data(idx).unwrap().map(|&x| x as f64)
                    }).collect::<Vec<_>>()
                ))))
            },

            // no text -> ?? operations
            (&ReduceDataIndex::Text(_), _) => { unreachable![] },

            // bool -> ?? conversions
            (&ReduceDataIndex::Boolean(_), Some(FieldType::Boolean))
                | (&ReduceDataIndex::Boolean(_), None) =>
            {
                None
            },
            (&ReduceDataIndex::Boolean(ref data), Some(FieldType::Unsigned)) => {
                Some(ReduceDataIndex::Unsigned(OwnedOrRef::Owned(Box::new(
                    (0..data.len()).map(|idx| {
                        data.get_data(idx).unwrap().map(|&x| btu(x))
                    }).collect::<Vec<_>>()
                ))))
            },
            (&ReduceDataIndex::Boolean(ref data), Some(FieldType::Signed)) => {
                Some(ReduceDataIndex::Signed(OwnedOrRef::Owned(Box::new(
                    (0..data.len()).map(|idx| {
                        data.get_data(idx).unwrap().map(|&x| bti(x))
                    }).collect::<Vec<_>>()
                ))))
            },
            (&ReduceDataIndex::Boolean(ref data), Some(FieldType::Text)) => {
                Some(ReduceDataIndex::Text(OwnedOrRef::Owned(Box::new(
                    (0..data.len()).map(|idx| {
                        data.get_data(idx).unwrap().map(|&x| format!("{}", x))
                    }).collect::<Vec<_>>()
                ))))
            },
            (&ReduceDataIndex::Boolean(ref data), Some(FieldType::Float)) => {
                Some(ReduceDataIndex::Float(OwnedOrRef::Owned(Box::new(
                    (0..data.len()).map(|idx| {
                        data.get_data(idx).unwrap().map(|&x| btf(x))
                    }).collect::<Vec<_>>()
                ))))
            },

            // float -> ?? conversions
            (&ReduceDataIndex::Float(_), Some(FieldType::Float))
                | (&ReduceDataIndex::Float(_), None) =>
            {
                None
            },
            (&ReduceDataIndex::Float(ref data), Some(FieldType::Unsigned)) => {
                Some(ReduceDataIndex::Unsigned(OwnedOrRef::Owned(Box::new(
                    (0..data.len()).map(|idx| {
                        data.get_data(idx).unwrap().map(|&x| if x < 0.0 { 0 } else { x as u64 })
                    }).collect::<Vec<_>>()
                ))))
            },
            (&ReduceDataIndex::Float(ref data), Some(FieldType::Signed)) => {
                Some(ReduceDataIndex::Signed(OwnedOrRef::Owned(Box::new(
                    (0..data.len()).map(|idx| {
                        data.get_data(idx).unwrap().map(|&x| x as i64)
                    }).collect::<Vec<_>>()
                ))))
            },
            (&ReduceDataIndex::Float(ref data), Some(FieldType::Text)) => {
                Some(ReduceDataIndex::Text(OwnedOrRef::Owned(Box::new(
                    (0..data.len()).map(|idx| {
                        data.get_data(idx).unwrap().map(|&x| format!("{}", x))
                    }).collect::<Vec<_>>()
                ))))
            },
            (&ReduceDataIndex::Float(ref data), Some(FieldType::Boolean)) => {
                Some(ReduceDataIndex::Boolean(OwnedOrRef::Owned(Box::new(
                    (0..data.len()).map(|idx| {
                        data.get_data(idx).unwrap().map(|&x| ftb(x))
                    }).collect::<Vec<_>>()
                ))))
            },
        }
    }
}


macro_rules! impl_dv_dv_op {
    ($reducefn_name:ident, $opname:ident, $opfn:ident, $opstr:expr, $nonbool_fn:ident,
        $bool_fn:ident, $infer_fn:ident) => {


struct $reducefn_name<'a, 'b> {
    target_ds: &'a mut DataStore,
    target_ident: &'b FieldIdent,
    left_convert: Option<FieldType>,
    right_convert: Option<FieldType>
}
impl<'a, 'b> $reducefn_name<'a, 'b> {
    fn add_to_ds<O: DataType>(&mut self, value: MaybeNa<O>) where DataStore: AddData<O> {
        self.target_ds.add(self.target_ident.clone(), value);
    }
}
impl<'a, 'b, 'c> FieldReduceFn<'c> for $reducefn_name<'a, 'b> {
    type Output = ();
    fn reduce(&mut self, fields: Vec<ReduceDataIndex<'c>>) -> () {
        debug_assert_eq!(fields.len(), 2);
        let left_converted = fields[0].convert(self.left_convert);
        let right_converted = fields[1].convert(self.right_convert);
        let left = left_converted.as_ref().unwrap_or(&fields[0]);
        let right = right_converted.as_ref().unwrap_or(&fields[1]);
        match (left, right) {
            (&ReduceDataIndex::Unsigned(ref left), &ReduceDataIndex::Unsigned(ref right)) => {
                debug_assert_eq!(left.len(), right.len());
                for i in 0..left.len() {
                    let new_value = match (left.get_data(i).unwrap(),
                                           right.get_data(i).unwrap())
                    {
                        (MaybeNa::Exists(l), MaybeNa::Exists(r)) =>
                            MaybeNa::Exists($nonbool_fn(l, r)),
                        _ => MaybeNa::Na
                    };
                    self.add_to_ds(new_value);
                }
            },
            (&ReduceDataIndex::Signed(ref left), &ReduceDataIndex::Signed(ref right)) => {
                debug_assert_eq!(left.len(), right.len());
                for i in 0..left.len() {
                    let new_value = match (left.get_data(i).unwrap(),
                                           right.get_data(i).unwrap())
                    {
                        (MaybeNa::Exists(l), MaybeNa::Exists(r)) =>
                            MaybeNa::Exists($nonbool_fn(l, r)),
                        _ => MaybeNa::Na
                    };
                    self.add_to_ds(new_value);
                }
            },
            (&ReduceDataIndex::Text(_), &ReduceDataIndex::Text(_)) => {
                unreachable![]
            },
            (&ReduceDataIndex::Boolean(ref left), &ReduceDataIndex::Boolean(ref right)) => {
                debug_assert_eq!(left.len(), right.len());
                for i in 0..left.len() {
                    let new_value = match (left.get_data(i).unwrap(),
                                           right.get_data(i).unwrap())
                    {
                        (MaybeNa::Exists(l), MaybeNa::Exists(r)) =>
                            MaybeNa::Exists($bool_fn(l, r)),
                        _ => MaybeNa::Na
                    };
                    self.add_to_ds(new_value);
                }
            },
            (&ReduceDataIndex::Float(ref left), &ReduceDataIndex::Float(ref right)) => {
                debug_assert_eq!(left.len(), right.len());
                for i in 0..left.len() {
                    let new_value = match (left.get_data(i).unwrap(),
                                           right.get_data(i).unwrap())
                    {
                        (MaybeNa::Exists(l), MaybeNa::Exists(r)) =>
                            MaybeNa::Exists($nonbool_fn(l, r)),
                        _ => MaybeNa::Na
                    };
                    self.add_to_ds(new_value);
                }
            },
            (_, _) => { unreachable![] }
        }
    }
}
impl<'a, 'b> $opname<&'b DataView> for &'a DataView {
    type Output = Result<DataView>;
    fn $opfn(self, rhs: &'b DataView) -> Result<DataView> {
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
        let opstr = $opstr;

        struct FieldInfo {
            target_field: TypedFieldIdent,
            left_ident: FieldIdent,
            rght_ident: FieldIdent,
            bin_op_types: BinOpTypes,
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
                let bin_op_types = rght_ty.$infer_fn(left_ty)?;
                fields.push(FieldInfo {
                    target_field: TypedFieldIdent {
                        ident: FieldIdent::Name(format!("{} {} {}", left_ident.clone(), opstr,
                            rght_ident.clone())),
                        ty: bin_op_types.output
                    },
                    left_ident: left_ident.clone(),
                    rght_ident: rght_ident.clone(),
                    bin_op_types
                })
            }
        } else {
            // due to above dimension checking, this is either n x 1, 1 x n, or 1 x 1
            for TypedFieldIdent { ident: left_ident, ty: left_ty } in self.field_types() {
                for TypedFieldIdent { ident: rght_ident, ty: rght_ty } in rhs.field_types() {
                    let bin_op_types = rght_ty.$infer_fn(left_ty)?;
                    fields.push(FieldInfo {
                        target_field: TypedFieldIdent {
                            ident: FieldIdent::Name(format!("{} {} {}", left_ident.clone(), opstr,
                                rght_ident.clone())),
                            ty: bin_op_types.output
                        },
                        left_ident: left_ident.clone(),
                        rght_ident,
                        bin_op_types
                    })
                }
            }
        }
        let mut store = DataStore::with_field_iter(fields.iter().map(|f| f.target_field.clone()));
        for FieldInfo { target_field, left_ident, rght_ident, bin_op_types } in fields {
            vec![
                self.select(&left_ident),
                rhs.select(&rght_ident),
            ].apply_field_reduce(
                &mut $reducefn_name {
                    target_ds: &mut store,
                    target_ident: &target_field.ident,
                    left_convert: bin_op_types.left,
                    right_convert: bin_op_types.right,
                },
            )?;
        }
        Ok(store.into())
    }
}
impl $opname<DataView> for DataView {
    type Output = Result<DataView>;
    fn $opfn(self, rhs: DataView) -> Result<DataView> {
        (&self).$opfn(&rhs)
    }
}
impl<'a> $opname<&'a DataView> for DataView {
    type Output = Result<DataView>;
    fn $opfn(self, rhs: &'a DataView) -> Result<DataView> {
        (&self).$opfn(rhs)
    }
}
impl<'a> $opname<DataView> for &'a DataView {
    type Output = Result<DataView>;
    fn $opfn(self, rhs: DataView) -> Result<DataView> {
        self.$opfn(&rhs)
    }
}

// END IMPL_DV_DV_OP
    }
}

#[inline]
fn add<T: DataType + Copy + Add<T, Output=T>>(l: &T, r: &T) -> T { *l + *r }
#[inline]
fn booladd(l: &bool, r: &bool) -> bool { *l | *r }
impl_dv_dv_op!(Add2Fn, Add, add, "+", add, booladd, infer_ft_add_result);

#[inline]
fn sub<T: DataType + Copy + Sub<T, Output=T>>(l: &T, r: &T) -> T { *l - *r }
#[inline]
fn boolsub(l: &bool, r: &bool) -> bool { *l | !*r }
impl_dv_dv_op!(Sub2Fn, Sub, sub, "-", sub, boolsub, infer_ft_sub_result);

#[inline]
fn mul<T: DataType + Copy + Mul<T, Output=T>>(l: &T, r: &T) -> T { *l * *r }
#[inline]
fn boolmul(l: &bool, r: &bool) -> bool { *l & *r }
impl_dv_dv_op!(Mul2Fn, Mul, mul, "*", mul, boolmul, infer_ft_mul_result);

#[inline]
fn div<T: DataType + Copy + Div<T, Output=T>>(l: &T, r: &T) -> T { *l / *r }
#[inline]
fn booldiv(l: &bool, r: &bool) -> bool { *l & !*r }
impl_dv_dv_op!(Div2Fn, Div, div, "/", div, booldiv, infer_ft_div_result);



macro_rules! impl_infer_ft_result {
    ($fn_name:tt, $infer_ty_fn_name:tt, $err_var:tt) => {

impl FieldType {
    fn $fn_name(self, left: FieldType) -> Result<BinOpTypes> {
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
struct BinOpTypes {
    left: Option<FieldType>,
    right: Option<FieldType>,
    output: FieldType,
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
trait InferAddResult {
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
trait InferSubResult {
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
trait InferMulResult {
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
trait InferDivResult {
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
        unsigned::assert_dv_eq_vec(&computed_dv, &"Foo + 2".into(),
            vec![4u64, 5, 10, 4, 22, 5, 2]
        );

        // added to signed scalar; should become a signed field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        let computed_dv: DataView = (dv.v("Foo") + -2i64).unwrap();
        assert_eq!(computed_dv.get_field_type(&"Foo + -2".into()).unwrap(), FieldType::Signed);
        signed::assert_dv_eq_vec(&computed_dv, &"Foo + -2".into(),
            vec![0i64, 1, 6, 0, 18, 1, -2]
        );

        // added to floating point scalar; should become a floating point field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        let computed_dv: DataView = (dv.v("Foo") + 2.0).unwrap();
        assert_eq!(computed_dv.get_field_type(&"Foo + 2".into()).unwrap(), FieldType::Float);
        float::assert_dv_eq_vec(&computed_dv, &"Foo + 2".into(),
            vec![4.0, 5.0, 10.0, 4.0, 22.0, 5.0, 2.0]
        );

        /* signed data */
        let data_vec = vec![2i64, -3, -8, 2, -20, 3, 0];

        // added to unsigned scalar; should remain a signed field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        let computed_dv: DataView = (dv.v("Foo") + 2u64).unwrap();
        assert_eq!(computed_dv.get_field_type(&"Foo + 2".into()).unwrap(), FieldType::Signed);
        signed::assert_dv_eq_vec(&computed_dv, &"Foo + 2".into(),
            vec![4i64, -1, -6, 4, -18, 5, 2]
        );

        // added to signed scalar; should remain a signed field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        let computed_dv: DataView = (dv.v("Foo") + -2i64).unwrap();
        assert_eq!(computed_dv.get_field_type(&"Foo + -2".into()).unwrap(), FieldType::Signed);
        signed::assert_dv_eq_vec(&computed_dv, &"Foo + -2".into(),
            vec![0i64, -5, -10, 0, -22, 1, -2]
        );

        // added to floating point scalar; should become a floating point field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        let computed_dv: DataView = (dv.v("Foo") + 2.0).unwrap();
        assert_eq!(computed_dv.get_field_type(&"Foo + 2".into()).unwrap(), FieldType::Float);
        float::assert_dv_eq_vec(&computed_dv, &"Foo + 2".into(),
            vec![4.0, -1.0, -6.0, 4.0, -18.0, 5.0, 2.0]
        );

        /* boolean data */
        let data_vec = vec![true, false, false, true, false, true, true];

        // added to unsigned scalar; should become an unsigned field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        let computed_dv: DataView = (dv.v("Foo") + 2u64).unwrap();
        assert_eq!(computed_dv.get_field_type(&"Foo + 2".into()).unwrap(), FieldType::Unsigned);
        unsigned::assert_dv_eq_vec(&computed_dv, &"Foo + 2".into(),
            vec![3u64, 2, 2, 3, 2, 3, 3]
        );

        // added to signed scalar; should become signed field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        let computed_dv: DataView = (dv.v("Foo") + -2i64).unwrap();
        assert_eq!(computed_dv.get_field_type(&"Foo + -2".into()).unwrap(), FieldType::Signed);
        signed::assert_dv_eq_vec(&computed_dv, &"Foo + -2".into(),
            vec![-1i64, -2, -2, -1, -2, -1, -1]
        );

        // added to floating point scalar; should become a floating point field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        let computed_dv: DataView = (dv.v("Foo") + 2.0).unwrap();
        assert_eq!(computed_dv.get_field_type(&"Foo + 2".into()).unwrap(), FieldType::Float);
        float::assert_dv_eq_vec(&computed_dv, &"Foo + 2".into(),
            vec![3.0, 2.0, 2.0, 3.0, 2.0, 3.0, 3.0]
        );

        /* floating point data */
        let data_vec = vec![2.0, -3.0, -8.0, 2.0, -20.0, 3.0, 0.0];

        // added to unsigned scalar; should remain a floating point field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        let computed_dv: DataView = (dv.v("Foo") + 2u64).unwrap();
        assert_eq!(computed_dv.get_field_type(&"Foo + 2".into()).unwrap(), FieldType::Float);
        float::assert_dv_eq_vec(&computed_dv, &"Foo + 2".into(),
            vec![4.0, -1.0, -6.0, 4.0, -18.0, 5.0, 2.0]
        );

        // added to signed scalar; should remain a floating point field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        let computed_dv: DataView = (dv.v("Foo") + -2i64).unwrap();
        assert_eq!(computed_dv.get_field_type(&"Foo + -2".into()).unwrap(), FieldType::Float);
        float::assert_dv_eq_vec(&computed_dv, &"Foo + -2".into(),
            vec![0.0, -5.0, -10.0, 0.0, -22.0, 1.0, -2.0]
        );

        // added to floating point scalar; should remain a floating point field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        let computed_dv: DataView = (dv.v("Foo") + 2.0).unwrap();
        assert_eq!(computed_dv.get_field_type(&"Foo + 2".into()).unwrap(), FieldType::Float);
        float::assert_dv_eq_vec(&computed_dv, &"Foo + 2".into(),
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
        signed::assert_dv_eq_vec(&computed_dv, &"Foo - 2".into(),
            vec![0i64, 1, 6, 0, 18, 1, -2]
        );

        // subtract signed scalar; should become a signed field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        let computed_dv: DataView = (dv.v("Foo") - -2i64).unwrap();
        assert_eq!(computed_dv.get_field_type(&"Foo - -2".into()).unwrap(), FieldType::Signed);
        signed::assert_dv_eq_vec(&computed_dv, &"Foo - -2".into(),
            vec![4i64, 5, 10, 4, 22, 5, 2]
        );

        // subtract floating point scalar; should become a floating point field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        let computed_dv: DataView = (dv.v("Foo") - 2.0).unwrap();
        assert_eq!(computed_dv.get_field_type(&"Foo - 2".into()).unwrap(), FieldType::Float);
        float::assert_dv_eq_vec(&computed_dv, &"Foo - 2".into(),
            vec![0.0, 1.0, 6.0, 0.0, 18.0, 1.0, -2.0]
        );

        /* signed data */
        let data_vec = vec![2i64, -3, -8, 2, -20, 3, 0];

        // subtract unsigned scalar; should remain a signed field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        let computed_dv: DataView = (dv.v("Foo") - 2u64).unwrap();
        assert_eq!(computed_dv.get_field_type(&"Foo - 2".into()).unwrap(), FieldType::Signed);
        signed::assert_dv_eq_vec(&computed_dv, &"Foo - 2".into(),
            vec![0i64, -5, -10, 0, -22, 1, -2]
        );

        // subtract signed scalar; should remain a signed field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        let computed_dv: DataView = (dv.v("Foo") - -2i64).unwrap();
        assert_eq!(computed_dv.get_field_type(&"Foo - -2".into()).unwrap(), FieldType::Signed);
        signed::assert_dv_eq_vec(&computed_dv, &"Foo - -2".into(),
            vec![4i64, -1, -6, 4, -18, 5, 2]
        );

        // subtract floating point scalar; should become a floating point field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        let computed_dv: DataView = (dv.v("Foo") - 2.0).unwrap();
        assert_eq!(computed_dv.get_field_type(&"Foo - 2".into()).unwrap(), FieldType::Float);
        float::assert_dv_eq_vec(&computed_dv, &"Foo - 2".into(),
            vec![0.0, -5.0, -10.0, 0.0, -22.0, 1.0, -2.0]
        );

        /* boolean data */
        let data_vec = vec![true, false, false, true, false, true, true];

        // subtract unsigned scalar; should become a signed field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        let computed_dv: DataView = (dv.v("Foo") - 2u64).unwrap();
        assert_eq!(computed_dv.get_field_type(&"Foo - 2".into()).unwrap(), FieldType::Signed);
        signed::assert_dv_eq_vec(&computed_dv, &"Foo - 2".into(),
            vec![-1i64, -2, -2, -1, -2, -1, -1]
        );

        // subtract signed scalar; should become signed field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        let computed_dv: DataView = (dv.v("Foo") - -2i64).unwrap();
        assert_eq!(computed_dv.get_field_type(&"Foo - -2".into()).unwrap(), FieldType::Signed);
        signed::assert_dv_eq_vec(&computed_dv, &"Foo - -2".into(),
            vec![3i64, 2, 2, 3, 2, 3, 3]
        );

        // subtract floating point scalar; should become a floating point field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        let computed_dv: DataView = (dv.v("Foo") - 2.0).unwrap();
        assert_eq!(computed_dv.get_field_type(&"Foo - 2".into()).unwrap(), FieldType::Float);
        float::assert_dv_eq_vec(&computed_dv, &"Foo - 2".into(),
            vec![-1.0, -2.0, -2.0, -1.0, -2.0, -1.0, -1.0]
        );

        /* floating point data */
        let data_vec = vec![2.0, -3.0, -8.0, 2.0, -20.0, 3.0, 0.0];

        // subtract unsigned scalar; should remain a floating point field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        let computed_dv: DataView = (dv.v("Foo") - 2u64).unwrap();
        assert_eq!(computed_dv.get_field_type(&"Foo - 2".into()).unwrap(), FieldType::Float);
        float::assert_dv_eq_vec(&computed_dv, &"Foo - 2".into(),
            vec![0.0, -5.0, -10.0, 0.0, -22.0, 1.0, -2.0]
        );

        // subtract signed scalar; should remain a floating point field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        let computed_dv: DataView = (dv.v("Foo") - -2i64).unwrap();
        assert_eq!(computed_dv.get_field_type(&"Foo - -2".into()).unwrap(), FieldType::Float);
        float::assert_dv_eq_vec(&computed_dv, &"Foo - -2".into(),
            vec![4.0, -1.0, -6.0, 4.0, -18.0, 5.0, 2.0]
        );

        // subtract floating point scalar; should remain a floating point field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        let computed_dv: DataView = (dv.v("Foo") - 2.0).unwrap();
        assert_eq!(computed_dv.get_field_type(&"Foo - 2".into()).unwrap(), FieldType::Float);
        float::assert_dv_eq_vec(&computed_dv, &"Foo - 2".into(),
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
        unsigned::assert_dv_eq_vec(&computed_dv, &"Foo * 2".into(),
            vec![4u64, 6, 16, 4, 40, 6, 0]
        );

        // multiplied by signed scalar; should become a signed field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        let computed_dv: DataView = (dv.v("Foo") * -2i64).unwrap();
        assert_eq!(computed_dv.get_field_type(&"Foo * -2".into()).unwrap(), FieldType::Signed);
        signed::assert_dv_eq_vec(&computed_dv, &"Foo * -2".into(),
            vec![-4i64, -6, -16, -4, -40, -6, -0]
        );

        // multiplied by floating point scalar; should become a floating point field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        let computed_dv: DataView = (dv.v("Foo") * 2.0).unwrap();
        assert_eq!(computed_dv.get_field_type(&"Foo * 2".into()).unwrap(), FieldType::Float);
        float::assert_dv_eq_vec(&computed_dv, &"Foo * 2".into(),
            vec![4.0, 6.0, 16.0, 4.0, 40.0, 6.0, 0.0]
        );

        /* signed data */
        let data_vec = vec![2i64, -3, -8, 2, -20, 3, 0];

        // multiplied by unsigned scalar; should remain a signed field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        let computed_dv: DataView = (dv.v("Foo") * 2u64).unwrap();
        assert_eq!(computed_dv.get_field_type(&"Foo * 2".into()).unwrap(), FieldType::Signed);
        signed::assert_dv_eq_vec(&computed_dv, &"Foo * 2".into(),
            vec![4i64, -6, -16, 4, -40, 6, 0]
        );

        // multiplied by signed scalar; should remain a signed field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        let computed_dv: DataView = (dv.v("Foo") * -2i64).unwrap();
        assert_eq!(computed_dv.get_field_type(&"Foo * -2".into()).unwrap(), FieldType::Signed);
        signed::assert_dv_eq_vec(&computed_dv, &"Foo * -2".into(),
            vec![-4i64, 6, 16, -4, 40, -6, -0]
        );

        // multiplied by floating point scalar; should become a floating point field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        let computed_dv: DataView = (dv.v("Foo") * 2.0).unwrap();
        assert_eq!(computed_dv.get_field_type(&"Foo * 2".into()).unwrap(), FieldType::Float);
        float::assert_dv_eq_vec(&computed_dv, &"Foo * 2".into(),
            vec![4.0, -6.0, -16.0, 4.0, -40.0, 6.0, 0.0]
        );

        /* boolean data */
        let data_vec = vec![true, false, false, true, false, true, true];

        // multiplied by unsigned scalar; should become an unsigned field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        let computed_dv: DataView = (dv.v("Foo") * 2u64).unwrap();
        assert_eq!(computed_dv.get_field_type(&"Foo * 2".into()).unwrap(), FieldType::Unsigned);
        unsigned::assert_dv_eq_vec(&computed_dv, &"Foo * 2".into(),
            vec![2u64, 0, 0, 2, 0, 2, 2]
        );

        // multiplied by signed scalar; should become signed field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        let computed_dv: DataView = (dv.v("Foo") * -2i64).unwrap();
        assert_eq!(computed_dv.get_field_type(&"Foo * -2".into()).unwrap(), FieldType::Signed);
        signed::assert_dv_eq_vec(&computed_dv, &"Foo * -2".into(),
            vec![-2i64, 0, 0, -2, 0, -2, -2]
        );

        // multiplied by floating point scalar; should become a floating point field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        let computed_dv: DataView = (dv.v("Foo") * 2.0).unwrap();
        assert_eq!(computed_dv.get_field_type(&"Foo * 2".into()).unwrap(), FieldType::Float);
        float::assert_dv_eq_vec(&computed_dv, &"Foo * 2".into(),
            vec![2.0, 0.0, 0.0, 2.0, 0.0, 2.0, 2.0]
        );

        /* floating point data */
        let data_vec = vec![2.0, -3.0, -8.0, 2.0, -20.0, 3.0, 0.0];

        // multiplied by unsigned scalar; should remain a floating point field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        let computed_dv: DataView = (dv.v("Foo") * 2u64).unwrap();
        assert_eq!(computed_dv.get_field_type(&"Foo * 2".into()).unwrap(), FieldType::Float);
        float::assert_dv_eq_vec(&computed_dv, &"Foo * 2".into(),
            vec![4.0, -6.0, -16.0, 4.0, -40.0, 6.0, 0.0]
        );

        // multiplied by signed scalar; should remain a floating point field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        let computed_dv: DataView = (dv.v("Foo") * -2i64).unwrap();
        assert_eq!(computed_dv.get_field_type(&"Foo * -2".into()).unwrap(), FieldType::Float);
        float::assert_dv_eq_vec(&computed_dv, &"Foo * -2".into(),
            vec![-4.0, 6.0, 16.0, -4.0, 40.0, -6.0, 0.0]
        );

        // multiplied by floating point scalar; should remain a floating point field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        let computed_dv: DataView = (dv.v("Foo") * 2.0).unwrap();
        assert_eq!(computed_dv.get_field_type(&"Foo * 2".into()).unwrap(), FieldType::Float);
        float::assert_dv_eq_vec(&computed_dv, &"Foo * 2".into(),
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
        float::assert_dv_eq_vec(&computed_dv, &"Foo / 2".into(),
            vec![1.0, 1.5, 4.0, 1.0, 10.0, 1.5, 0.0]
        );

        // divide by signed scalar; should become a floating point field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        let computed_dv: DataView = (dv.v("Foo") / -2i64).unwrap();
        assert_eq!(computed_dv.get_field_type(&"Foo / -2".into()).unwrap(), FieldType::Float);
        float::assert_dv_eq_vec(&computed_dv, &"Foo / -2".into(),
            vec![-1.0, -1.5, -4.0, -1.0, -10.0, -1.5, 0.0]
        );

        // divide by floating point scalar; should become a floating point field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        let computed_dv: DataView = (dv.v("Foo") / 2.0).unwrap();
        assert_eq!(computed_dv.get_field_type(&"Foo / 2".into()).unwrap(), FieldType::Float);
        float::assert_dv_eq_vec(&computed_dv, &"Foo / 2".into(),
            vec![1.0, 1.5, 4.0, 1.0, 10.0, 1.5, 0.0]
        );

        /* signed data */
        let data_vec = vec![2i64, -3, -8, 2, -20, 3, 0];

        // divide by unsigned scalar; should become a floating point field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        let computed_dv: DataView = (dv.v("Foo") / 2u64).unwrap();
        assert_eq!(computed_dv.get_field_type(&"Foo / 2".into()).unwrap(), FieldType::Float);
        float::assert_dv_eq_vec(&computed_dv, &"Foo / 2".into(),
            vec![1.0, -1.5, -4.0, 1.0, -10.0, 1.5, 0.0]
        );

        // divide by signed scalar; should become a floating point field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        let computed_dv: DataView = (dv.v("Foo") / -2i64).unwrap();
        assert_eq!(computed_dv.get_field_type(&"Foo / -2".into()).unwrap(), FieldType::Float);
        float::assert_dv_eq_vec(&computed_dv, &"Foo / -2".into(),
            vec![-1.0, 1.5, 4.0, -1.0, 10.0, -1.5, 0.0]
        );

        // divide by floating point scalar; should become a floating point field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        let computed_dv: DataView = (dv.v("Foo") / 2.0).unwrap();
        assert_eq!(computed_dv.get_field_type(&"Foo / 2".into()).unwrap(), FieldType::Float);
        float::assert_dv_eq_vec(&computed_dv, &"Foo / 2".into(),
            vec![1.0, -1.5, -4.0, 1.0, -10.0, 1.5, 0.0]
        );

        /* boolean data */
        let data_vec = vec![true, false, false, true, false, true, true];

        // divide by unsigned scalar; should become a floating point field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        let computed_dv: DataView = (dv.v("Foo") / 2u64).unwrap();
        assert_eq!(computed_dv.get_field_type(&"Foo / 2".into()).unwrap(), FieldType::Float);
        float::assert_dv_eq_vec(&computed_dv, &"Foo / 2".into(),
            vec![0.5, 0.0, 0.0, 0.5, 0.0, 0.5, 0.5]
        );

        // divide by signed scalar; should become a floating point field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        let computed_dv: DataView = (dv.v("Foo") / -2i64).unwrap();
        assert_eq!(computed_dv.get_field_type(&"Foo / -2".into()).unwrap(), FieldType::Float);
        float::assert_dv_eq_vec(&computed_dv, &"Foo / -2".into(),
            vec![-0.5, 0.0, 0.0, -0.5, 0.0, -0.5, -0.5]
        );

        // divide by floating point scalar; should become a floating point field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        let computed_dv: DataView = (dv.v("Foo") / 2.0).unwrap();
        assert_eq!(computed_dv.get_field_type(&"Foo / 2".into()).unwrap(), FieldType::Float);
        float::assert_dv_eq_vec(&computed_dv, &"Foo / 2".into(),
            vec![0.5, 0.0, 0.0, 0.5, 0.0, 0.5, 0.5]
        );

        /* floating point data */
        let data_vec = vec![2.0, -3.0, -8.0, 2.0, -20.0, 3.0, 0.0];

        // divide by unsigned scalar; should remain a floating point field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        let computed_dv: DataView = (dv.v("Foo") / 2u64).unwrap();
        assert_eq!(computed_dv.get_field_type(&"Foo / 2".into()).unwrap(), FieldType::Float);
        float::assert_dv_eq_vec(&computed_dv, &"Foo / 2".into(),
            vec![1.0, -1.5, -4.0, 1.0, -10.0, 1.5, 0.0]
        );

        // divide by signed scalar; should remain a floating point field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        let computed_dv: DataView = (dv.v("Foo") / -2i64).unwrap();
        assert_eq!(computed_dv.get_field_type(&"Foo / -2".into()).unwrap(), FieldType::Float);
        float::assert_dv_eq_vec(&computed_dv, &"Foo / -2".into(),
            vec![-1.0, 1.5, 4.0, -1.0, 10.0, -1.5, -0.0]
        );

        // divide by floating point scalar; should remain a floating point field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        let computed_dv: DataView = (dv.v("Foo") / 2.0).unwrap();
        assert_eq!(computed_dv.get_field_type(&"Foo / 2".into()).unwrap(), FieldType::Float);
        float::assert_dv_eq_vec(&computed_dv, &"Foo / 2".into(),
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
        float::assert_dv_eq_vec(&computed_dv, &"Foo / 0".into(),
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
            println!("{}\n{}", dv1, dv2);
            let computed_dv: DataView = ($op(dv1.v("Foo"), dv2.v("Bar"))).unwrap();
            println!("{}", computed_dv);
            let target_ident = FieldIdent::Name(format!("Foo {} Bar", $strop));
            assert_eq!(computed_dv.get_field_type(&target_ident).unwrap(), $result_ty);
            $test_mod::assert_dv_eq_vec(&computed_dv, &target_ident, $result);
        }}
    }
    macro_rules! test_add_op {
        ($left:expr, $right:expr, $result:expr, $result_ty:expr, $test_mod:ident) => (
            test_view_op!($left, $right, $result, |dv1, dv2| dv1 + dv2, "+", $result_ty, $test_mod)
        )
    }
    macro_rules! test_sub_op {
        ($left:expr, $right:expr, $result:expr, $result_ty:expr, $test_mod:ident) => (
            test_view_op!($left, $right, $result, |dv1, dv2| dv1 - dv2, "-", $result_ty, $test_mod)
        )
    }
    macro_rules! test_mul_op {
        ($left:expr, $right:expr, $result:expr, $result_ty:expr, $test_mod:ident) => (
            test_view_op!($left, $right, $result, |dv1, dv2| dv1 * dv2, "*", $result_ty, $test_mod)
        )
    }
    macro_rules! test_div_op {
        ($left:expr, $right:expr, $result:expr, $result_ty:expr, $test_mod:ident) => (
            test_view_op!($left, $right, $result, |dv1, dv2| dv1 / dv2, "/", $result_ty, $test_mod)
        )
    }

    #[test]
    fn add_field() {
        // unsigned data + unsigned data -> unsigned
        test_add_op!(
            vec![2u64,  3, 8,  2,  20,  3, 0],
            vec![55u64, 3, 1,  9, 106,  9, 0],
            vec![57u64, 6, 9, 11, 126, 12, 0],
            FieldType::Unsigned, unsigned
        );

        // unsigned data + signed data -> signed
        test_add_op!(
            vec![2u64,   3,  8,  2,   20,  3, 0],
            vec![55i64, -3, -1,  9, -106,  9, 0],
            vec![57i64,  0,  7, 11,  -86, 12, 0],
            FieldType::Signed, signed
        );

        // unsigned data + boolean data -> unsigned
        test_add_op!(
            vec![2u64,     3,     8,    2,    20,     3,     0],
            vec![true, false, false, true,  true, false, false],
            vec![3u64,     3,     8,    3,    21,     3,     0],
            FieldType::Unsigned, unsigned
        );

        // unsigned data + float data -> float
        test_add_op!(
            vec![2u64,    3,    8,    2,     20,    3,   0],
            vec![55.0, -3.0, -1.0,  9.0, -106.0,  9.0, 0.0],
            vec![57.0,  0.0,  7.0, 11.0,  -86.0, 12.0, 0.0],
            FieldType::Float, float
        );

        // signed data + unsigned data -> signed
        test_add_op!(
            vec![55i64, -3, -1,  9, -106,  9, 0],
            vec![2u64,   3,  8,  2,   20,  3, 0],
            vec![57i64,  0,  7, 11,  -86, 12, 0],
            FieldType::Signed, signed
        );

        // signed data + signed data -> signed
        test_add_op!(
            vec![2i64,   3, -8,  2,   20, -3, 0],
            vec![55i64, -3, -1, -9, -106,  9, 0],
            vec![57i64,  0, -9, -7,  -86,  6, 0],
            FieldType::Signed, signed
        );

        // signed data + boolean data -> signed
        test_add_op!(
            vec![2i64,    -3,    -8,   -2,    20,     3,     0],
            vec![true, false, false, true,  true, false, false],
            vec![3i64,    -3,    -8,   -1,    21,     3,     0],
            FieldType::Signed, signed
        );

        // signed data + float data -> float
        test_add_op!(
            vec![2i64,    3,   -8,   -2,    -20,    3,   0],
            vec![55.0, -3.0, -1.0,  9.0, -106.0,  9.0, 0.0],
            vec![57.0,  0.0, -9.0,  7.0, -126.0, 12.0, 0.0],
            FieldType::Float, float
        );

        // boolean data + unsigned data -> unsigned
        test_add_op!(
            vec![true, false, false, true,  true, false, false],
            vec![2u64,     3,     8,    2,    20,     3,     0],
            vec![3u64,     3,     8,    3,    21,     3,     0],
            FieldType::Unsigned, unsigned
        );

        // boolean data + signed data -> signed
        test_add_op!(
            vec![true, false, false, true,  true, false, false],
            vec![2i64,    -3,    -8,   -2,    20,     3,     0],
            vec![3i64,    -3,    -8,   -1,    21,     3,     0],
            FieldType::Signed, signed
        );

        // boolean data + boolean data -> boolean (OR)
        test_add_op!(
            vec![true,  true, false, false,  true, false, false],
            vec![true, false, false,  true,  true, false,  true],
            vec![true,  true, false,  true,  true, false,  true],
            FieldType::Boolean, boolean
        );

        // boolean data + float data -> float
        test_add_op!(
            vec![true, false, false, true,  true, false, false],
            vec![ 2.0,  -3.0,  -8.0, -2.0,  20.0,   3.0,   0.0],
            vec![ 3.0,  -3.0,  -8.0, -1.0,  21.0,   3.0,   0.0],
            FieldType::Float, float
        );

        // float data + unsigned data -> float
        test_add_op!(
            vec![55.0, -3.0, -1.0,  9.0, -106.0,  9.0, 0.0],
            vec![2u64,    3,    8,    2,     20,    3,   0],
            vec![57.0,  0.0,  7.0, 11.0,  -86.0, 12.0, 0.0],
            FieldType::Float, float
        );

        // float data + signed data -> float
        test_add_op!(
            vec![55.0, -3.0, -1.0,  9.0, -106.0,  9.0, 0.0],
            vec![2i64,    3,   -8,   -2,    -20,    3,   0],
            vec![57.0,  0.0, -9.0,  7.0, -126.0, 12.0, 0.0],
            FieldType::Float, float
        );

        // float data + boolean data -> float
        test_add_op!(
            vec![ 2.0,  -3.0,  -8.0, -2.0,  20.0,   3.0,   0.0],
            vec![true, false, false, true,  true, false, false],
            vec![ 3.0,  -3.0,  -8.0, -1.0,  21.0,   3.0,   0.0],
            FieldType::Float, float
        );

        // float data + float data -> float
        test_add_op!(
            vec![55.0, -3.0, -1.0,  9.0, -106.0,  9.0, 0.0],
            vec![ 2.0,  3.0, -8.0, -2.0,  -20.0,  3.0, 0.0],
            vec![57.0,  0.0, -9.0,  7.0, -126.0, 12.0, 0.0],
            FieldType::Float, float
        );
    }

    #[test]
    fn sub_field() {
        // unsigned data - unsigned data -> unsigned
        test_sub_op!(
            vec![  2u64,  3, 8,  2,  20,  3, 0],
            vec![ 55u64,  3, 1,  9, 106,  9, 0],
            vec![-53i64,  0, 7, -7, -86, -6, 0],
            FieldType::Signed, signed
        );

        // unsigned data - signed data -> signed
        test_sub_op!(
            vec![  2u64,  3,  8,  2,   20,  3, 0],
            vec![ 55i64, -3, -1,  9, -106,  9, 0],
            vec![-53i64,  6,  9, -7,  126, -6, 0],
            FieldType::Signed, signed
        );

        // unsigned data - boolean data -> signed
        test_sub_op!(
            vec![2u64,     3,     8,    2,    20,     3,     0],
            vec![true, false, false, true,  true, false,  true],
            vec![1i64,     3,     8,    1,    19,     3,    -1],
            FieldType::Signed, signed
        );

        // unsigned data - float data -> float
        test_sub_op!(
            vec![ 2u64,    3,    8,    2,     20,    3,   0],
            vec![ 55.0, -3.0, -1.0,  9.0, -106.0,  9.0, 0.0],
            vec![-53.0,  6.0,  9.0, -7.0,  126.0, -6.0, 0.0],
            FieldType::Float, float
        );

        // signed data - unsigned data -> signed
        test_sub_op!(
            vec![55i64, -3, -1,  9, -106,  9, 0],
            vec![2u64,   3,  8,  2,   20,  3, 0],
            vec![53i64, -6, -9,  7, -126,  6, 0],
            FieldType::Signed, signed
        );

        // signed data - signed data -> signed
        test_sub_op!(
            vec![ 2i64,   3, -8,  2,   20, -3, 0],
            vec![ 55i64, -3, -1, -9, -106,  9, 0],
            vec![-53i64,  6, -7, 11,  126,-12, 0],
            FieldType::Signed, signed
        );

        // signed data - boolean data -> signed
        test_sub_op!(
            vec![2i64,    -3,    -8,   -2,    20,     3,     0],
            vec![true, false, false, true,  true, false, false],
            vec![1i64,    -3,    -8,   -3,    19,     3,     0],
            FieldType::Signed, signed
        );

        // signed data - float data -> float
        test_sub_op!(
            vec![ 2i64,    3,   -8,    -2,    -20,    3,   0],
            vec![ 55.0, -3.0, -1.0,   9.0, -106.0,  9.0, 0.0],
            vec![-53.0,  6.0, -7.0, -11.0,   86.0, -6.0, 0.0],
            FieldType::Float, float
        );

        // boolean data - unsigned data -> signed
        test_sub_op!(
            vec![ true, false, false, true,  true, false, false],
            vec![ 2u64,     3,     8,    2,    20,     3,     1],
            vec![-1i64,    -3,    -8,   -1,   -19,    -3,    -1],
            FieldType::Signed, signed
        );

        // boolean data - signed data -> signed
        test_sub_op!(
            vec![ true, false, false, true,  true, false, false],
            vec![ 2i64,    -3,    -8,   -2,    20,     3,     0],
            vec![-1i64,     3,     8,    3,   -19,    -3,     0],
            FieldType::Signed, signed
        );

        // boolean data - boolean data -> boolean (l | ~r)
        test_sub_op!(
            vec![true,  true, false, false,  true, false, false],
            vec![true, false, false,  true,  true, false,  true],
            vec![true,  true,  true, false,  true,  true, false],
            FieldType::Boolean, boolean
        );

        // boolean data - float data -> float
        test_sub_op!(
            vec![true, false, false, true,  true, false, false],
            vec![ 2.0,  -3.0,  -8.0, -2.0,  20.0,   3.0,   0.0],
            vec![-1.0,   3.0,   8.0,  3.0, -19.0,  -3.0,   0.0],
            FieldType::Float, float
        );

        // float data - unsigned data -> float
        test_sub_op!(
            vec![55.0, -3.0, -1.0,  9.0, -106.0,  9.0, 0.0],
            vec![2u64,    3,    8,    2,     20,    3,   0],
            vec![53.0, -6.0, -9.0,  7.0, -126.0,  6.0, 0.0],
            FieldType::Float, float
        );

        // float data - signed data -> float
        test_sub_op!(
            vec![55.0, -3.0, -1.0,  9.0, -106.0,  9.0, 0.0],
            vec![2i64,    3,   -8,   -2,    -20,    3,   0],
            vec![53.0, -6.0,  7.0, 11.0,  -86.0,  6.0, 0.0],
            FieldType::Float, float
        );

        // float data - boolean data -> float
        test_sub_op!(
            vec![ 2.0,  -3.0,  -8.0, -2.0,  20.0,   3.0,   0.0],
            vec![true, false, false, true,  true, false, false],
            vec![ 1.0,  -3.0,  -8.0, -3.0,  19.0,   3.0,   0.0],
            FieldType::Float, float
        );

        // float data - float data -> float
        test_sub_op!(
            vec![55.0, -3.0, -1.0,  9.0, -106.0,  9.0, 0.0],
            vec![ 2.0,  3.0, -8.0, -2.0,  -20.0,  3.0, 0.0],
            vec![53.0, -6.0,  7.0, 11.0,  -86.0,  6.0, 0.0],
            FieldType::Float, float
        );
    }

    #[test]
    fn mul_field() {
        // unsigned data * unsigned data -> unsigned
        test_mul_op!(
            vec![  2u64,  3, 8,  2,   20,  3, 4],
            vec![ 55u64,  3, 1,  9,  106,  9, 0],
            vec![110u64,  9, 8, 18, 2120, 27, 0],
            FieldType::Unsigned, unsigned
        );

        // unsigned data * signed data -> signed
        test_mul_op!(
            vec![  2u64,  3,  8,  2,    20,  3,  0],
            vec![ 55i64, -3, -1,  9,  -106,  9, -4],
            vec![110i64, -9, -8, 18, -2120, 27,  0],
            FieldType::Signed, signed
        );

        // unsigned data * boolean data -> unsigned
        test_mul_op!(
            vec![2u64,     3,     8,    2,    20,     3,     0],
            vec![true, false, false, true,  true, false,  true],
            vec![2u64,     0,     0,    2,    20,     0,     0],
            FieldType::Unsigned, unsigned
        );

        // unsigned data * float data -> float
        test_mul_op!(
            vec![ 2u64,    3,    8,    2,      20,    3,    0],
            vec![ 55.0, -3.0, -1.0,  9.0,  -106.0,  9.0, -4.0],
            vec![110.0, -9.0, -8.0, 18.0, -2120.0, 27.0,  0.0],
            FieldType::Float, float
        );

        // signed data * unsigned data -> signed
        test_mul_op!(
            vec![ 55i64,  -3, -1,  9,  -106,  9, -4],
            vec![  2u64,   3,  8,  2,    20,  3,  0],
            vec![110i64,  -9, -8, 18, -2120, 27,  0],
            FieldType::Signed, signed
        );

        // signed data * signed data -> signed
        test_mul_op!(
            vec![  2i64,  3, -8,   2,    20,  -3,  0],
            vec![ 55i64, -3, -1,  -9,  -106,   9, -4],
            vec![110i64, -9,  8, -18, -2120, -27,  0],
            FieldType::Signed, signed
        );

        // signed data * boolean data -> signed
        test_mul_op!(
            vec![2i64,    -3,    -8,   -2,    20,     3,     0],
            vec![true, false, false, true,  true, false,  true],
            vec![2i64,     0,     0,   -2,    20,     0,     0],
            FieldType::Signed, signed
        );

        // signed data * float data -> float
        test_mul_op!(
            vec![ 2i64,    3,   -8,    -2,    -20,    3,   0],
            vec![ 55.0, -3.0, -1.0,   9.0, -106.0,  9.0, 4.0],
            vec![110.0, -9.0,  8.0, -18.0, 2120.0, 27.0, 0.0],
            FieldType::Float, float
        );

        // boolean data * unsigned data -> unsigned
        test_mul_op!(
            vec![true, false, false, true,  true, false, true],
            vec![2u64,     3,     8,    2,    20,     3,    0],
            vec![2u64,     0,     0,    2,    20,     0,    0],
            FieldType::Unsigned, unsigned
        );

        // boolean data * signed data -> signed
        test_mul_op!(
            vec![true, false, false, true,  true, false, true],
            vec![2i64,    -3,    -8,   -2,    20,     3,    0],
            vec![2i64,     0,     0,   -2,    20,     0,    0],
            FieldType::Signed, signed
        );

        // boolean data * boolean data -> boolean (AND)
        test_mul_op!(
            vec![true,  true, false, false,  true, false, false],
            vec![true, false, false,  true,  true, false,  true],
            vec![true, false, false, false,  true, false, false],
            FieldType::Boolean, boolean
        );

        // boolean data * float data -> float
        test_mul_op!(
            vec![true, false, false, true,  true, false, true],
            vec![ 2.0,  -3.0,  -8.0, -2.0,  20.0,   3.0,  0.0],
            vec![ 2.0,   0.0,   0.0, -2.0,  20.0,   0.0,  0.0],
            FieldType::Float, float
        );

        // float data * unsigned data -> float
        test_mul_op!(
            vec![ 55.0, -3.0, -1.0,  9.0, - 106.0,  9.0, 0.0],
            vec![ 2u64,    3,    8,    2,      20,    3,   4],
            vec![110.0, -9.0, -8.0, 18.0, -2120.0, 27.0, 0.0],
            FieldType::Float, float
        );

        // float data * signed data -> float
        test_mul_op!(
            vec![ 55.0, -3.0, -1.0,   9.0, -106.0,  9.0, 0.0],
            vec![ 2i64,    3,   -8,    -2,    -20,    3,   4],
            vec![110.0, -9.0,  8.0, -18.0, 2120.0, 27.0, 0.0],
            FieldType::Float, float
        );

        // float data * boolean data -> float
        test_mul_op!(
            vec![ 2.0,  -3.0,  -8.0, -2.0,  20.0,   3.0,  0.0],
            vec![true, false, false, true,  true, false, true],
            vec![ 2.0,   0.0,   0.0, -2.0,  20.0,   0.0,  0.0],
            FieldType::Float, float
        );

        // float data * float data -> float
        test_mul_op!(
            vec![ 55.0, -3.0, -1.0,   9.0, -106.0,  9.0, 0.0],
            vec![  2.0,  3.0, -8.0,  -2.0,  -20.0,  3.0, 4.0],
            vec![110.0, -9.0,  8.0, -18.0, 2120.0, 27.0, 0.0],
            FieldType::Float, float
        );
    }

    #[test]
    fn div_field() {
        use std::f64::INFINITY as INF;
        use std::f64::NEG_INFINITY as NEGINF;

        // unsigned data / unsigned data -> float
        test_div_op!(
            vec![ 55u64,   3,   8,   2,   20,   0,   4],
            vec![ 11u64,   2,   1,   5,  100,   3,   0],
            vec![   5.0, 1.5, 8.0, 0.4,  0.2, 0.0, INF],
            FieldType::Float, float
        );

        // unsigned data / signed data -> float
        test_div_op!(
            vec![ 55u64,    3,    8,   2,   20,   0,   4],
            vec![ 11i64,   -2,   -1,   5, -100,  -3,   0],
            vec![   5.0, -1.5, -8.0, 0.4, -0.2, 0.0, INF],
            FieldType::Float, float
        );

        // unsigned data / boolean data -> float
        test_div_op!(
            vec![2u64,     3,     8,    2,    20,     3,     0],
            vec![true, false, false, true,  true, false,  true],
            vec![ 2.0,   INF,   INF,  2.0,  20.0,   INF,   0.0],
            FieldType::Float, float
        );

        // unsigned data / float data -> float
        test_div_op!(
            vec![55u64,    3,    8,   2,     20,    0,   4],
            vec![ 11.0, -2.0, -1.0, 5.0, -100.0, -3.0, 0.0],
            vec![  5.0, -1.5, -8.0, 0.4,   -0.2,  0.0, INF],
            FieldType::Float, float
        );

        // signed data / unsigned data -> float
        test_div_op!(
            vec![ 55i64,   -3,   -8,   2,  -20,   0,   4],
            vec![ 11u64,    2,    1,   5,  100,   3,   0],
            vec![   5.0, -1.5, -8.0, 0.4, -0.2, 0.0, INF],
            FieldType::Float, float
        );

        // signed data / signed data -> float
        test_div_op!(
            vec![ 55i64,   -3,   -8,   2,  -20,   0,   4],
            vec![ 11i64,   -2,    1,   5,  100,  -3,   0],
            vec![   5.0,  1.5, -8.0, 0.4, -0.2, 0.0, INF],
            FieldType::Float, float
        );

        // signed data / boolean data -> float
        test_div_op!(
            vec![2i64,     -3,     -8,   -2,    20,      -3,     0],
            vec![true,  false,  false, true,  true,   false,  true],
            vec![ 2.0, NEGINF, NEGINF, -2.0,  20.0,  NEGINF,   0.0],
            FieldType::Float, float
        );

        // signed data / float data -> float
        test_div_op!(
            vec![55i64,   -3,   -8,   2,    -20,    0,     -4],
            vec![ 11.0, -2.0, -1.0, 5.0, -100.0, -3.0,    0.0],
            vec![  5.0,  1.5,  8.0, 0.4,    0.2,  0.0, NEGINF],
            FieldType::Float, float
        );

        // boolean data / unsigned data -> float
        test_div_op!(
            vec![true, false, false, true,  true, false, true],
            vec![2u64,     3,     8,    4,    20,     3,    0],
            vec![ 0.5,   0.0,   0.0, 0.25,  0.05,   0.0,  INF],
            FieldType::Float, float
        );

        // boolean data / signed data -> float
        test_div_op!(
            vec![true, false, false, true,   true, false, true],
            vec![2i64,    -3,    -8,    4,    -20,    -3,    0],
            vec![ 0.5,   0.0,   0.0, 0.25,  -0.05,   0.0,  INF],
            FieldType::Float, float
        );

        // boolean data / boolean data -> boolean (left & ~right)
        test_div_op!(
            vec![ true,  true, false, false,  true, false, false],
            vec![ true, false, false,  true,  true, false,  true],
            vec![false,  true, false, false, false, false, false],
            FieldType::Boolean, boolean
        );

        // boolean data / float data -> float
        test_div_op!(
            vec![true, false, false, true,  true, false, true],
            vec![ 2.0,  -3.0,  -8.0, -2.0,  20.0,   3.0,  0.0],
            vec![ 0.5,   0.0,   0.0, -0.5,  0.05,   0.0,  INF],
            FieldType::Float, float
        );

        // float data / unsigned data -> float
        test_div_op!(
            vec![ 55.0, -3.0, -8.0,  2.0,  -20.0, 0.0, 4.0],
            vec![11u64,    2,    1,    5,    100,   3,   0],
            vec![  5.0, -1.5, -8.0,  0.4,   -0.2, 0.0, INF],
            FieldType::Float, float
        );

        // float data / signed data -> float
        test_div_op!(
            vec![ 55.0, -3.0, -8.0,  2.0,  -20.0, 0.0, 4.0],
            vec![11i64,   -2,   -1,    5,   -100,  -3,   0],
            vec![  5.0,  1.5,  8.0,  0.4,    0.2, 0.0, INF],
            FieldType::Float, float
        );

        // float data / boolean data -> float
        test_div_op!(
            vec![ 2.0,   -3.0,   -8.0, -2.0,  20.0,   3.0,  0.0],
            vec![true,  false,  false, true,  true, false, true],
            vec![ 2.0, NEGINF, NEGINF, -2.0,  20.0,   INF,  0.0],
            FieldType::Float, float
        );

        // float data / float data -> float
        test_div_op!(
            vec![ 55.0, -3.0, -8.0,  2.0,  -20.0,  0.0, 4.0],
            vec![ 11.0, -2.0, -1.0,  5.0, -100.0, -3.0, 0.0],
            vec![  5.0,  1.5,  8.0,  0.4,    0.2,  0.0, INF],
            FieldType::Float, float
        );
    }
}
