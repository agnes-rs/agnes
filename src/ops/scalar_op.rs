use std::ops::{Add, Sub, Mul, Div};

use field::{TypedFieldIdent, DataType, FieldType, FieldIdent};
use view::{DataView};
use store::{DataStore, AddData};
use error::*;
use masked::{MaybeNa};
use apply::{DataIndex, FieldApplyTo, FieldMapFn};
use ops::infer::*;

macro_rules! impl_op_fn {
    ($($fn_name:tt)*) => {$(

struct $fn_name<'a, 'b, T> {
    target_ds: &'a mut DataStore,
    target_ident: &'b FieldIdent,
    term: T,
}
impl<'a, 'b, T: DataType> $fn_name<'a, 'b, T> {
    fn add_to_ds<O: DataType>(&mut self, value: MaybeNa<O>) where DataStore: AddData<O> {
        self.target_ds.add(self.target_ident.clone(), value);
    }
}

    )*}
}
 impl_op_fn!(AddFn AddReverseFn SubFn SubReverseFn MulFn MulReverseFn DivFn DivReverseFn);

macro_rules! impl_op_fieldmap_fn {
    (
        $op:tt;
        $op_fn:tt;
        $op_str:expr;
        $infer_fn:tt;
        $op_fieldfn_ty:tt;
        $op_revfieldfn_ty:tt;
        $dtype:ty;
        unsigned: $unsigned_calc:expr;
        unsigned_rev: $unsigned_rev_calc:expr;
        signed: $signed_calc:expr;
        signed_rev: $signed_rev_calc:expr;
        boolean: $bool_calc:expr;
        boolean_rev: $bool_rev_calc:expr;
        float: $float_calc:expr;
        float_rev: $float_rev_calc:expr;
    ) => {
// START IMPL_OP_FIELDMAP_FN

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

impl<'a, 'b> FieldMapFn for $op_revfieldfn_ty<'a, 'b, $dtype> {
    type Output = ();
    fn apply_unsigned<T: DataIndex<u64>>(&mut self, field: &T) {
        for i in 0..field.len() {
            let new_value = field.get_data(i).unwrap()
                .map(|&val| $unsigned_rev_calc(val, self.term));
            self.add_to_ds(new_value);
        }
    }
    fn apply_signed<T: DataIndex<i64>>(&mut self, field: &T) {
        for i in 0..field.len() {
            let new_value = field.get_data(i).unwrap()
                .map(|&val| $signed_rev_calc(val, self.term));
            self.add_to_ds(new_value);
        }
    }
    fn apply_text<T: DataIndex<String>>(&mut self, _: &T) { unreachable!() }
    fn apply_boolean<T: DataIndex<bool>>(&mut self, field: &T) {
        for i in 0..field.len() {
            let new_value = field.get_data(i).unwrap()
                .map(|&val| $bool_rev_calc(val, self.term));
            self.add_to_ds(new_value);
        }
    }
    fn apply_float<T: DataIndex<f64>>(&mut self, field: &T) {
        for i in 0..field.len() {
            let new_value = field.get_data(i).unwrap()
                .map(|&val| $float_rev_calc(val, self.term));
            self.add_to_ds(new_value);
        }
    }
}

// END IMPL_OP_FIELDMAP_FN
        }
}

macro_rules! impl_add_fieldmap_fn {
    ($($t:tt)*) => (
        impl_op_fieldmap_fn!(Add; add; "+"; infer_add_result; AddFn; AddReverseFn; $($t)*);
    )
}
macro_rules! impl_sub_fieldmap_fn {
    ($($t:tt)*) => (
        impl_op_fieldmap_fn!(Sub; sub; "-"; infer_sub_result; SubFn; SubReverseFn; $($t)*);
    )
}
macro_rules! impl_mul_fieldmap_fn {
    ($($t:tt)*) => (
        impl_op_fieldmap_fn!(Mul; mul; "*"; infer_mul_result; MulFn; MulReverseFn; $($t)*);
    )
}
macro_rules! impl_div_fieldmap_fn {
    ($($t:tt)*) => (
        impl_op_fieldmap_fn!(Div; div; "/"; infer_div_result; DivFn; DivReverseFn; $($t)*);
    )
}

// unsigned to bool
pub(crate) fn utb(x: u64) -> bool { if x > 0 { true } else { false } }
// signed to bool
pub(crate) fn itb(x: i64) -> bool { if x == 0 { false } else { true } }
// float to bool
pub(crate) fn ftb(x: f64) -> bool { if x == 0.0 { false } else { true } }

// bool to unsigned
pub(crate) fn btu(x: bool) -> u64 { if x { 1 } else { 0 } }
// bool to signed
pub(crate) fn bti(x: bool) -> i64 { if x { 1 } else { 0 } }
// bool to float
pub(crate) fn btf(x: bool) -> f64 { if x { 1.0 } else { 0.0 } }


impl_add_fieldmap_fn!(
    u64;
    unsigned: |x: u64, addend: u64| -> u64 { x + addend }; // unsigned + u64 -> u64
    unsigned_rev: |x:u64, addend:u64| -> u64 { addend + x };
    signed: |x: i64, addend: u64| -> i64 { x + addend as i64 }; // signed + u64 -> i64
    signed_rev: |x: i64, addend: u64| -> i64 { addend as i64 + x };
    boolean: |x: bool, addend: u64| -> u64 { x as u64 + addend }; // bool + u64 -> u64
    boolean_rev: |x: bool, addend: u64| -> u64 { addend + x as u64 };
    float: |x: f64, addend: u64| -> f64 { x + addend as f64 }; // float + u64 -> f64
    float_rev: |x: f64, addend: u64| -> f64 { addend as f64 + x };
);
impl_add_fieldmap_fn!(
    i64;
    unsigned: |x: u64, addend: i64| -> i64 { x as i64 + addend }; // unsigned + i64 -> i64
    unsigned_rev: |x: u64, addend: i64| -> i64 { addend + x as i64 };
    signed: |x: i64, addend: i64| -> i64 { x + addend }; // signed + i64 -> i64
    signed_rev: |x: i64, addend: i64| -> i64 { addend + x };
    boolean: |x: bool, addend: i64| -> i64 { x as i64 + addend }; // boolean + i64 -> i64
    boolean_rev: |x: bool, addend: i64| -> i64 { addend + x as i64 };
    float: |x: f64, addend: i64| -> f64 { x + addend as f64 }; // float + i64 -> f64
    float_rev: |x: f64, addend: i64| -> f64 { addend as f64 + x };
);
impl_add_fieldmap_fn!(
    f64;
    unsigned: |x: u64, addend: f64| -> f64 { x as f64 + addend }; // unsigned + f64 -> f64
    unsigned_rev: |x: u64, addend: f64| -> f64 { addend + x as f64 };
    signed: |x: i64, addend: f64| -> f64 { x as f64 + addend }; // signed + f64 -> f64
    signed_rev: |x: i64, addend: f64| -> f64 { addend + x as f64 };
    boolean: |x: bool, addend: f64| -> f64 { btf(x) + addend }; // boolean + f64 -> f64
    boolean_rev: |x: bool, addend: f64| -> f64 { addend + btf(x) };
    float: |x: f64, addend: f64| -> f64 { x + addend }; // float + f64 -> f64
    float_rev: |x: f64, addend: f64| -> f64 { addend + x };
);

impl_sub_fieldmap_fn!(
    u64;
    unsigned: |x: u64, subend: u64| -> i64 { x as i64 - subend as i64 }; // unsigned - u64 -> i64
    unsigned_rev: |x: u64, subend: u64| -> i64 { subend as i64 - x as i64 };
    signed: |x: i64, subend: u64| -> i64 { x - subend as i64 }; // signed - u64 -> i64
    signed_rev: |x: i64, subend: u64| -> i64 { subend as i64 - x };
    boolean: |x: bool, subend: u64| -> i64 { x as i64 - subend as i64 }; // bool - u64 -> i64
    boolean_rev: |x: bool, subend: u64| -> i64 { subend as i64 - x as i64 };
    float: |x: f64, subend: u64| -> f64 { x - subend as f64 }; // float - u64 -> f64
    float_rev: |x: f64, subend: u64| -> f64 { subend as f64 - x };
);
impl_sub_fieldmap_fn!(
    i64;
    unsigned: |x: u64, subend: i64| -> i64 { x as i64 - subend }; // unsigned - i64 -> i64
    unsigned_rev: |x: u64, subend: i64| -> i64 { subend - x as i64 };
    signed: |x: i64, subend: i64| -> i64 { x - subend }; // signed - i64 -> i64
    signed_rev: |x: i64, subend: i64| -> i64 { subend - x };
    boolean: |x: bool, subend: i64| -> i64 { x as i64 - subend }; // boolean - i64 -> i64
    boolean_rev: |x: bool, subend: i64| -> i64 { subend - x as i64 };
    float: |x: f64, subend: i64| -> f64 { x - subend as f64 }; // float - i64 -> f64
    float_rev: |x: f64, subend: i64| -> f64 { subend as f64 - x };
);
impl_sub_fieldmap_fn!(
    f64;
    unsigned: |x: u64, subend: f64| -> f64 { x as f64 - subend }; // unsigned - f64 -> f64
    unsigned_rev: |x: u64, subend: f64| -> f64 { subend - x as f64 };
    signed: |x: i64, subend: f64| -> f64 { x as f64 - subend }; // signed - f64 -> f64
    signed_rev: |x: i64, subend: f64| -> f64 { subend - x as f64 };
    boolean: |x: bool, subend: f64| -> f64 { btf(x) - subend }; // boolean - f64 -> f64
    boolean_rev: |x: bool, subend: f64| -> f64 { subend - btf(x) };
    float: |x: f64, subend: f64| -> f64 { x - subend }; // float - f64 -> f64
    float_rev: |x: f64, subend: f64| -> f64 { subend - x };
);

impl_mul_fieldmap_fn!(
    u64;
    unsigned: |x: u64, mult: u64| -> u64 { x * mult }; // unsigned * u64 -> u64
    unsigned_rev: |x: u64, mult: u64| -> u64 { mult * x };
    signed: |x: i64, mult: u64| -> i64 { x * mult as i64 }; // signed * u64 -> i64
    signed_rev: |x: i64, mult: u64| -> i64 { mult as i64 * x };
    boolean: |x: bool, mult: u64| -> u64 { x as u64 * mult }; // bool * u64 -> u64
    boolean_rev: |x: bool, mult: u64| -> u64 { mult * x as u64 };
    float: |x: f64, mult: u64| -> f64 { x * mult as f64 }; // float * u64 -> f64
    float_rev: |x: f64, mult: u64| -> f64 { mult as f64 * x };
);
impl_mul_fieldmap_fn!(
    i64;
    unsigned: |x: u64, mult: i64| -> i64 { x as i64 * mult }; // unsigned * i64 -> i64
    unsigned_rev: |x: u64, mult: i64| -> i64 { mult * x as i64 };
    signed: |x: i64, mult: i64| -> i64 { x * mult }; // signed * i64 -> i64
    signed_rev: |x: i64, mult: i64| -> i64 { mult * x };
    boolean: |x: bool, mult: i64| -> i64 { x as i64 * mult }; // boolean * i64 -> i64
    boolean_rev: |x: bool, mult: i64| -> i64 { mult * x as i64 };
    float: |x: f64, mult: i64| -> f64 { x * mult as f64 }; // float * i64 -> f64
    float_rev: |x: f64, mult: i64| -> f64 { mult as f64 * x };
);
impl_mul_fieldmap_fn!(
    f64;
    unsigned: |x: u64, mult: f64| -> f64 { x as f64 * mult }; // unsigned * f64 -> f64
    unsigned_rev: |x: u64, mult: f64| -> f64 { mult * x as f64 };
    signed: |x: i64, mult: f64| -> f64 { x as f64 * mult }; // signed * f64 -> f64
    signed_rev: |x: i64, mult: f64| -> f64 { mult * x as f64 };
    boolean: |x: bool, mult: f64| -> f64 { btf(x) * mult }; // boolean * f64 -> f64
    boolean_rev: |x: bool, mult: f64| -> f64 { mult * btf(x) };
    float: |x: f64, mult: f64| -> f64 { x * mult }; // float * f64 -> f64
    float_rev: |x: f64, mult: f64| -> f64 { mult * x };
);

impl_div_fieldmap_fn!(
    u64;
    unsigned: |x: u64, divisor: u64| -> f64 { x as f64 / divisor as f64 }; // unsigned / u64 -> f64
    unsigned_rev: |x: u64, divisor: u64| -> f64 { divisor as f64 / x as f64 };
    signed: |x: i64, divisor: u64| -> f64 { x as f64 / divisor as f64 }; // signed / u64 -> f64
    signed_rev: |x: i64, divisor: u64| -> f64 { divisor as f64 / x as f64 };
    boolean: |x: bool, divisor: u64| -> f64 { btf(x) / divisor as f64 }; // bool / u64 -> f64
    boolean_rev: |x: bool, divisor: u64| -> f64 { divisor as f64 / btf(x) };
    float: |x: f64, divisor: u64| -> f64 { x / divisor as f64 }; // float / u64 -> f64
    float_rev: |x: f64, divisor: u64| -> f64 { divisor as f64 / x };
);
impl_div_fieldmap_fn!(
    i64;
    unsigned: |x: u64, divisor: i64| -> f64 { x as f64 / divisor as f64 }; // unsigned / i64 -> f64
    unsigned_rev: |x: u64, divisor: i64| -> f64 { divisor as f64 / x as f64 };
    signed: |x: i64, divisor: i64| -> f64 { x as f64 / divisor as f64 }; // signed / i64 -> f64
    signed_rev: |x: i64, divisor: i64| -> f64 { divisor as f64 / x as f64 };
    boolean: |x: bool, divisor: i64| -> f64 { btf(x) / divisor as f64 }; // boolean / i64 -> f64
    boolean_rev: |x: bool, divisor: i64| -> f64 { divisor as f64 / btf(x) };
    float: |x: f64, divisor: i64| -> f64 { x / divisor as f64 }; // float / i64 -> f64
    float_rev: |x: f64, divisor: i64| -> f64 { divisor as f64 / x };
);
impl_div_fieldmap_fn!(
    f64;
    unsigned: |x: u64, divisor: f64| -> f64 { x as f64 / divisor }; // unsigned / f64 -> f64
    unsigned_rev: |x: u64, divisor: f64| -> f64 { divisor / x as f64 };
    signed: |x: i64, divisor: f64| -> f64 { x as f64 / divisor }; // signed / f64 -> f64
    signed_rev: |x: i64, divisor: f64| -> f64 { divisor / x as f64 };
    boolean: |x: bool, divisor: f64| -> f64 { btf(x) / divisor }; // boolean / f64 -> f64
    boolean_rev: |x: bool, divisor: f64| -> f64 { divisor / btf(x) };
    float: |x: f64, divisor: f64| -> f64 { x / divisor }; // float * f64 -> f64
    float_rev: |x: f64, divisor: f64| -> f64 { divisor / x };
);



struct FieldIdents {
    src_ident: FieldIdent,
    target_ident: TypedFieldIdent,
}
impl DataView {
    fn get_field_idents<F, G>(
            &self,
            infer_fn: F,
            target_name: G,
        ) -> Result<Vec<FieldIdents>>
        where F: Fn(FieldType) -> Result<BinOpTypes>,
              G: Fn(FieldIdent) -> String
    {
        let mut fields = vec![];
        for (ident, vf) in self.fields.iter() {
            let src_ty = self.frames[vf.frame_idx].get_field_type(&vf.rident.ident).unwrap();
            let bin_op_types = infer_fn(src_ty)?;
            fields.push(FieldIdents {
                src_ident: ident.clone(),
                target_ident: TypedFieldIdent {
                    ident: FieldIdent::Name(target_name(ident.clone())),
                    ty: bin_op_types.output,
                },
            });
        }
        if fields.is_empty() {
            return Err(AgnesError::DimensionMismatch(
                "unable to apply arithmetic operation to an empty dataview".into()));
        }
        Ok(fields)
    }
}

macro_rules! impl_op {
    (
        $op:tt;
        $op_fn:tt;
        $op_str:expr;
        $infer_fn:tt;
        $op_fieldfn_ty:tt;
        $op_revfieldfn_ty:tt;
        $dtype:ty
    ) => {
// START IMPL_OP

impl<'a> $op<$dtype> for &'a DataView {
    type Output = Result<DataView>;
    fn $op_fn(self, rhs: $dtype) -> Result<DataView> {
        let fields = self.get_field_idents(|src_ty| <$dtype>::$infer_fn(src_ty),
            |ident| format!("{} {} {}", ident, $op_str, rhs))?;
        let mut store = DataStore::with_field_iter(fields.iter().map(|fi| fi.target_ident.clone()));
        for field_info in fields {
            self.field_apply_to(
                &mut $op_fieldfn_ty {
                    target_ds: &mut store,
                    target_ident: &field_info.target_ident.ident.clone(),
                    term: rhs,
                },
                &field_info.src_ident
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
impl<'a> $op<&'a DataView> for $dtype {
    type Output = Result<DataView>;
    fn $op_fn(self, rhs: &'a DataView) -> Result<DataView> {
        let fields = rhs.get_field_idents(|src_ty| <$dtype>::$infer_fn(src_ty),
            |ident| format!("{} {} {}", self, $op_str, ident))?;
        let mut store = DataStore::with_field_iter(fields.iter().map(|fi| fi.target_ident.clone()));
        for field_info in fields {
            rhs.field_apply_to(
                &mut $op_revfieldfn_ty {
                    target_ds: &mut store,
                    target_ident: &field_info.target_ident.ident.clone(),
                    term: self,
                },
                &field_info.src_ident
            )?;
        }
        Ok(store.into())
    }
}
impl $op<DataView> for $dtype {
    type Output = Result<DataView>;
    fn $op_fn(self, rhs: DataView) -> Result<DataView> {
        self.$op_fn(&rhs)
    }
}

// END IMPL_OP
    }
}

macro_rules! impl_add {
    ($($dtype:ty)*) => ($(
        impl_op!(Add; add; "+"; infer_add_result; AddFn; AddReverseFn; $dtype);
    )*)
}
macro_rules! impl_sub {
    ($($dtype:ty)*) => ($(
        impl_op!(Sub; sub; "-"; infer_sub_result; SubFn; SubReverseFn; $dtype);
    )*)
}
macro_rules! impl_mul {
    ($($dtype:ty)*) => ($(
        impl_op!(Mul; mul; "*"; infer_mul_result; MulFn; MulReverseFn; $dtype);
    )*)
}
macro_rules! impl_div {
    ($($dtype:ty)*) => ($(
        impl_op!(Div; div; "/"; infer_div_result; DivFn; DivReverseFn; $dtype);
    )*)
}
impl_add!(u64 i64 f64);
impl_sub!(u64 i64 f64);
impl_mul!(u64 i64 f64);
impl_div!(u64 i64 f64);

#[cfg(test)]
mod tests {
    use super::*;
    use test_utils::*;

    macro_rules! test_commutative {
        ($dv:ident, $fident:expr, $op:tt, $term:expr, $target_ty:expr, $target_mod:ident,
            $target_data:expr
        ) => {{
            test_commutative!($dv, $fident, $op, $term, $target_ty, $target_mod, $target_data,
                |&x| x);
        }};
        ($dv:ident, $fident:expr, $op:tt, $term:expr, $target_ty:expr, $target_mod:ident,
            $target_data:expr, $rev_fn:expr
        ) => {{
            // test dv <op> term
            let computed_dv: DataView = ($dv.v($fident) $op $term).unwrap();
            let field_name = format!("{} {} {}", $fident, stringify!($op), $term);
            assert_eq!(computed_dv.get_field_type(&field_name.clone().into()).unwrap(), $target_ty);
            $target_mod::assert_dv_eq_vec(&computed_dv, &field_name.clone().into(), $target_data);

            // test term <op> dv
            let computed_dv: DataView = ($term $op $dv.v($fident)).unwrap();
            let field_name = format!("{} {} {}", $term, stringify!($op), $fident);
            assert_eq!(computed_dv.get_field_type(&field_name.clone().into()).unwrap(), $target_ty);
            let target_vec = $target_data.iter().map($rev_fn).collect::<Vec<_>>();
            $target_mod::assert_dv_eq_vec(&computed_dv, &field_name.clone().into(), target_vec);
        }}
    }

    #[test]
    fn add_scalar() {
        /* unsigned data */
        let data_vec = vec![2u64, 3, 8, 2, 20, 3, 0];

        // added to unsigned scalar; should remain an unsigned field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        test_commutative!(dv, "Foo", +, 2u64, FieldType::Unsigned, unsigned,
            vec![4u64, 5, 10, 4, 22, 5, 2]
        );

        // added to signed scalar; should become a signed field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        test_commutative!(dv, "Foo", +, -2i64, FieldType::Signed, signed,
            vec![0i64, 1, 6, 0, 18, 1, -2]
        );

        // added to floating point scalar; should become a floating point field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        test_commutative!(dv, "Foo", +, 2.0, FieldType::Float, float,
            vec![4.0, 5.0, 10.0, 4.0, 22.0, 5.0, 2.0]
        );

        /* signed data */
        let data_vec = vec![2i64, -3, -8, 2, -20, 3, 0];

        // added to unsigned scalar; should remain a signed field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        test_commutative!(dv, "Foo", +, 2u64, FieldType::Signed, signed,
            vec![4i64, -1, -6, 4, -18, 5, 2]
        );

        // added to signed scalar; should remain a signed field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        test_commutative!(dv, "Foo", +, -2i64, FieldType::Signed, signed,
            vec![0i64, -5, -10, 0, -22, 1, -2]
        );

        // added to floating point scalar; should become a floating point field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        test_commutative!(dv, "Foo", +, 2.0, FieldType::Float, float,
            vec![4.0, -1.0, -6.0, 4.0, -18.0, 5.0, 2.0]
        );

        /* boolean data */
        let data_vec = vec![true, false, false, true, false, true, true];

        // added to unsigned scalar; should become an unsigned field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        test_commutative!(dv, "Foo", +, 2u64, FieldType::Unsigned, unsigned,
            vec![3u64, 2, 2, 3, 2, 3, 3]
        );

        // added to signed scalar; should become signed field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        test_commutative!(dv, "Foo", +, -2i64, FieldType::Signed, signed,
            vec![-1i64, -2, -2, -1, -2, -1, -1]
        );

        // added to floating point scalar; should become a floating point field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        test_commutative!(dv, "Foo", +, 2.0, FieldType::Float, float,
            vec![3.0, 2.0, 2.0, 3.0, 2.0, 3.0, 3.0]
        );

        /* floating point data */
        let data_vec = vec![2.0, -3.0, -8.0, 2.0, -20.0, 3.0, 0.0];

        // added to unsigned scalar; should remain a floating point field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        test_commutative!(dv, "Foo", +, 2u64, FieldType::Float, float,
            vec![4.0, -1.0, -6.0, 4.0, -18.0, 5.0, 2.0]
        );

        // added to signed scalar; should remain a floating point field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        test_commutative!(dv, "Foo", +, -2i64, FieldType::Float, float,
            vec![0.0, -5.0, -10.0, 0.0, -22.0, 1.0, -2.0]
        );

        // added to floating point scalar; should remain a floating point field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        test_commutative!(dv, "Foo", +, 2.0, FieldType::Float, float,
            vec![4.0, -1.0, -6.0, 4.0, -18.0, 5.0, 2.0]
        );
    }

    #[test]
    fn sub_scalar() {
        /* unsigned data */
        let data_vec = vec![2u64, 3, 8, 2, 20, 3, 0];

        // subtract unsigned scalar; should become a signed field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        test_commutative!(dv, "Foo", -, 2u64, FieldType::Signed, signed,
            vec![0i64, 1, 6, 0, 18, 1, -2], |&x| -x
        );

        // subtract signed scalar; should become a signed field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        test_commutative!(dv, "Foo", -, -2i64, FieldType::Signed, signed,
            vec![4i64, 5, 10, 4, 22, 5, 2], |&x| -x
        );

        // subtract floating point scalar; should become a floating point field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        test_commutative!(dv, "Foo", -, 2.0, FieldType::Float, float,
            vec![0.0, 1.0, 6.0, 0.0, 18.0, 1.0, -2.0], |&x| -x
        );

        /* signed data */
        let data_vec = vec![2i64, -3, -8, 2, -20, 3, 0];

        // subtract unsigned scalar; should remain a signed field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        test_commutative!(dv, "Foo", -, 2u64, FieldType::Signed, signed,
            vec![0i64, -5, -10, 0, -22, 1, -2], |&x| -x
        );

        // subtract signed scalar; should remain a signed field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        test_commutative!(dv, "Foo", -, -2i64, FieldType::Signed, signed,
            vec![4i64, -1, -6, 4, -18, 5, 2], |&x| -x
        );

        // subtract floating point scalar; should become a floating point field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        test_commutative!(dv, "Foo", -, 2.0, FieldType::Float, float,
            vec![0.0, -5.0, -10.0, 0.0, -22.0, 1.0, -2.0], |&x| -x
        );

        /* boolean data */
        let data_vec = vec![true, false, false, true, false, true, true];

        // subtract unsigned scalar; should become a signed field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        test_commutative!(dv, "Foo", -, 2u64, FieldType::Signed, signed,
            vec![-1i64, -2, -2, -1, -2, -1, -1], |&x| -x
        );

        // subtract signed scalar; should become signed field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        test_commutative!(dv, "Foo", -, -2i64, FieldType::Signed, signed,
            vec![3i64, 2, 2, 3, 2, 3, 3], |&x| -x
        );

        // subtract floating point scalar; should become a floating point field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        test_commutative!(dv, "Foo", -, 2.0, FieldType::Float, float,
            vec![-1.0, -2.0, -2.0, -1.0, -2.0, -1.0, -1.0], |&x| -x
        );

        /* floating point data */
        let data_vec = vec![2.0, -3.0, -8.0, 2.0, -20.0, 3.0, 0.0];

        // subtract unsigned scalar; should remain a floating point field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        test_commutative!(dv, "Foo", -, 2u64, FieldType::Float, float,
            vec![0.0, -5.0, -10.0, 0.0, -22.0, 1.0, -2.0], |&x| -x
        );

        // subtract signed scalar; should remain a floating point field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        test_commutative!(dv, "Foo", -, -2i64, FieldType::Float, float,
            vec![4.0, -1.0, -6.0, 4.0, -18.0, 5.0, 2.0], |&x| -x
        );

        // subtract floating point scalar; should remain a floating point field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        test_commutative!(dv, "Foo", -, 2.0, FieldType::Float, float,
            vec![0.0, -5.0, -10.0, 0.0, -22.0, 1.0, -2.0], |&x| -x
        );
    }


    #[test]
    fn multiply_scalar() {
        /* unsigned data */
        let data_vec = vec![2u64, 3, 8, 2, 20, 3, 0];

        // multiplied by unsigned scalar; should remain an unsigned field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        test_commutative!(dv, "Foo", *, 2u64, FieldType::Unsigned, unsigned,
            vec![4u64, 6, 16, 4, 40, 6, 0]
        );

        // multiplied by signed scalar; should become a signed field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        test_commutative!(dv, "Foo", *, -2i64, FieldType::Signed, signed,
            vec![-4i64, -6, -16, -4, -40, -6, -0]
        );

        // multiplied by floating point scalar; should become a floating point field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        test_commutative!(dv, "Foo", *, 2.0, FieldType::Float, float,
            vec![4.0, 6.0, 16.0, 4.0, 40.0, 6.0, 0.0]
        );

        /* signed data */
        let data_vec = vec![2i64, -3, -8, 2, -20, 3, 0];

        // multiplied by unsigned scalar; should remain a signed field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        test_commutative!(dv, "Foo", *, 2u64, FieldType::Signed, signed,
            vec![4i64, -6, -16, 4, -40, 6, 0]
        );

        // multiplied by signed scalar; should remain a signed field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        test_commutative!(dv, "Foo", *, -2i64, FieldType::Signed, signed,
            vec![-4i64, 6, 16, -4, 40, -6, -0]
        );

        // multiplied by floating point scalar; should become a floating point field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        test_commutative!(dv, "Foo", *, 2.0, FieldType::Float, float,
            vec![4.0, -6.0, -16.0, 4.0, -40.0, 6.0, 0.0]
        );

        /* boolean data */
        let data_vec = vec![true, false, false, true, false, true, true];

        // multiplied by unsigned scalar; should become an unsigned field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        test_commutative!(dv, "Foo", *, 2u64, FieldType::Unsigned, unsigned,
            vec![2u64, 0, 0, 2, 0, 2, 2]
        );

        // multiplied by signed scalar; should become signed field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        test_commutative!(dv, "Foo", *, -2i64, FieldType::Signed, signed,
            vec![-2i64, 0, 0, -2, 0, -2, -2]
        );

        // multiplied by floating point scalar; should become a floating point field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        test_commutative!(dv, "Foo", *, 2.0, FieldType::Float, float,
            vec![2.0, 0.0, 0.0, 2.0, 0.0, 2.0, 2.0]
        );

        /* floating point data */
        let data_vec = vec![2.0, -3.0, -8.0, 2.0, -20.0, 3.0, 0.0];

        // multiplied by unsigned scalar; should remain a floating point field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        test_commutative!(dv, "Foo", *, 2u64, FieldType::Float, float,
            vec![4.0, -6.0, -16.0, 4.0, -40.0, 6.0, 0.0]
        );

        // multiplied by signed scalar; should remain a floating point field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        test_commutative!(dv, "Foo", *, -2i64, FieldType::Float, float,
            vec![-4.0, 6.0, 16.0, -4.0, 40.0, -6.0, 0.0]
        );

        // multiplied by floating point scalar; should remain a floating point field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        test_commutative!(dv, "Foo", *, 2.0, FieldType::Float, float,
            vec![4.0, -6.0, -16.0, 4.0, -40.0, 6.0, 0.0]
        );
    }


    #[test]
    fn div_scalar() {

        /* unsigned data */
        let data_vec = vec![2u64, 3, 8, 2, 20, 3, 0];

        // divide by unsigned scalar; should become a floating point field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        test_commutative!(dv, "Foo", /, 2u64, FieldType::Float, float,
            vec![1.0, 1.5, 4.0, 1.0, 10.0, 1.5, 0.0], |&x| 1.0 / x
        );

        // divide by signed scalar; should become a floating point field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        test_commutative!(dv, "Foo", /, -2i64, FieldType::Float, float,
            vec![-1.0, -1.5, -4.0, -1.0, -10.0, -1.5, -0.0], |&x| 1.0 / x
        );

        // divide by floating point scalar; should become a floating point field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        test_commutative!(dv, "Foo", /, 2.0, FieldType::Float, float,
            vec![1.0, 1.5, 4.0, 1.0, 10.0, 1.5, 0.0], |&x| 1.0 / x
        );

        /* signed data */
        let data_vec = vec![2i64, -3, -8, 2, -20, 3, 0];

        // divide by unsigned scalar; should become a floating point field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        test_commutative!(dv, "Foo", /, 2u64, FieldType::Float, float,
            vec![1.0, -1.5, -4.0, 1.0, -10.0, 1.5, 0.0], |&x| 1.0 / x
        );

        // divide by signed scalar; should become a floating point field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        test_commutative!(dv, "Foo", /, -2i64, FieldType::Float, float,
            vec![-1.0, 1.5, 4.0, -1.0, 10.0, -1.5, -0.0], |&x| 1.0 / x
        );

        // divide by floating point scalar; should become a floating point field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        test_commutative!(dv, "Foo", /, 2.0, FieldType::Float, float,
            vec![1.0, -1.5, -4.0, 1.0, -10.0, 1.5, 0.0], |&x| 1.0 / x
        );

        /* boolean data */
        let data_vec = vec![true, false, false, true, false, true, true];

        // divide by unsigned scalar; should become a floating point field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        test_commutative!(dv, "Foo", /, 2u64, FieldType::Float, float,
            vec![0.5, 0.0, 0.0, 0.5, 0.0, 0.5, 0.5], |&x| 1.0 / x
        );

        // divide by signed scalar; should become a floating point field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        test_commutative!(dv, "Foo", /, -2i64, FieldType::Float, float,
            vec![-0.5, -0.0, -0.0, -0.5, -0.0, -0.5, -0.5], |&x| 1.0 / x
        );

        // divide by floating point scalar; should become a floating point field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        test_commutative!(dv, "Foo", /, 2.0, FieldType::Float, float,
            vec![0.5, 0.0, 0.0, 0.5, 0.0, 0.5, 0.5], |&x| 1.0 / x
        );

        /* floating point data */
        let data_vec = vec![2.0, -3.0, -8.0, 2.0, -20.0, 3.0, 0.0];

        // divide by unsigned scalar; should remain a floating point field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        test_commutative!(dv, "Foo", /, 2u64, FieldType::Float, float,
            vec![1.0, -1.5, -4.0, 1.0, -10.0, 1.5, 0.0], |&x| 1.0 / x
        );

        // divide by signed scalar; should remain a floating point field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        test_commutative!(dv, "Foo", /, -2i64, FieldType::Float, float,
            vec![-1.0, 1.5, 4.0, -1.0, 10.0, -1.5, -0.0], |&x| 1.0 / x
        );

        // divide by floating point scalar; should remain a floating point field
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        test_commutative!(dv, "Foo", /, 2.0, FieldType::Float, float,
            vec![1.0, -1.5, -4.0, 1.0, -10.0, 1.5, 0.0], |&x| 1.0 / x
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

        // divide-by-zero when zero is in the data view
        let data_vec = vec![2i64, -3, 0, 0, -20, 3, 1];
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        let computed_dv: DataView = (60u64 / dv.v("Foo")).unwrap();
        assert_eq!(computed_dv.get_field_type(&"60 / Foo".into()).unwrap(), FieldType::Float);
        float::assert_dv_eq_vec(&computed_dv, &"60 / Foo".into(),
            vec![30.0, -20.0, INF, INF, -3.0, 20.0, 60.0]
        );
        // check negative infinity too
        let data_vec = vec![2i64, -3, 0, 0, -20, 3, 1];
        let dv = data_vec.clone().merged_with_sample_emp_table("Foo");
        let computed_dv: DataView = (-60i64 / dv.v("Foo")).unwrap();
        assert_eq!(computed_dv.get_field_type(&"-60 / Foo".into()).unwrap(), FieldType::Float);
        float::assert_dv_eq_vec(&computed_dv, &"-60 / Foo".into(),
            vec![-30.0, 20.0, NEGINF, NEGINF, 3.0, -20.0, -60.0]
        );

    }
}

