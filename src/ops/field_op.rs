use std::ops::{Add, Sub, Mul, Div, Neg};

use error::*;
use field::{TypedFieldIdent, DataType, FieldType, FieldIdent};
use apply::{DataIndex, ReduceDataIndex, ApplyFieldReduce, FieldReduceFn, Select, OwnedOrRef,
    AddToDs, Convert, SingleTypeFn};
use ops::{BinOpTypes, utb, itb, btu, bti, btf, ftb};
use store::{DataStore, AddData};
use masked::MaybeNa;
use view::DataView;

impl<'a> ReduceDataIndex<'a> {
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

impl<'a> Neg for &'a DataView {
    type Output = Result<DataView>;
    fn neg(self) -> Result<DataView> {
        let mut store = DataStore::empty();
        for (ident, vf) in self.fields.iter() {
            let new_ident = FieldIdent::Name(format!("-{}", ident));
            let output_ty = infer_neg_output_type(
                self.frames[vf.frame_idx].get_field_type(&vf.rident.ident).unwrap()
            )?;
            store.add_field(TypedFieldIdent {
                ident: new_ident.clone(),
                ty: output_ty
            });
            match output_ty {
                FieldType::Signed => {
                    self.select(ident)
                        .map(Convert::<i64>::new())
                        .map(SingleTypeFn::new(|&x: &i64| -> i64 { -x }))
                        .map(AddToDs {
                            ds: &mut store,
                            ident: new_ident,
                        }).collect::<Vec<_>>()?;
                },
                FieldType::Float  => {
                    self.select(ident)
                        .map(Convert::<f64>::new())
                        .map(SingleTypeFn::new(|&x: &f64| -> f64 { -x }))
                        .map(AddToDs {
                            ds: &mut store,
                            ident: new_ident,
                        }).collect::<Vec<_>>()?;
                },
                _ => unreachable![]
            }
        }
        Ok(store.into())
    }
}
impl Neg for DataView {
    type Output = Result<DataView>;
    fn neg(self) -> Result<DataView> {
        (&self).neg()
    }
}
fn infer_neg_output_type(ft: FieldType) -> Result<FieldType> {
    match ft {
        FieldType::Unsigned => Ok(FieldType::Signed),
        FieldType::Signed   => Ok(FieldType::Signed),
        FieldType::Text     => Err(AgnesError::InvalidOp(
            "Unable to apply negation operator '-' to field of type 'Text'".into())),
        FieldType::Boolean  => Ok(FieldType::Signed),
        FieldType::Float    => Ok(FieldType::Float),
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use test_utils::*;

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

    #[test]
    fn neg_field() {
        let dv: DataView = DataStore::with_data(
            vec![("Foo", vec![0u64, 5, 2, 6, 3].into())], None, None, None, None
        ).into();
        let computed_dv: DataView = (-dv).unwrap();
        let target_ident = FieldIdent::Name("-Foo".into());
        assert_eq!(computed_dv.get_field_type(&target_ident).unwrap(), FieldType::Signed);
        signed::assert_dv_eq_vec(&computed_dv, &target_ident, vec![0i64, -5, -2, -6, -3]);

        let dv: DataView = DataStore::with_data(
            None, None, None, None, vec![("Foo", vec![0.0, -5.0, 2.0, 6.0, -3.0].into())]
        ).into();
        let computed_dv: DataView = (-dv).unwrap();
        println!("{}", computed_dv);
        let target_ident = FieldIdent::Name("-Foo".into());
        assert_eq!(computed_dv.get_field_type(&target_ident).unwrap(), FieldType::Float);
        float::assert_dv_eq_vec(&computed_dv, &target_ident, vec![0.0, 5.0, -2.0, -6.0, 3.0]);
    }
}
