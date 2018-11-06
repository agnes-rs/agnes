/*!
`DataView` join structs and implementations.
*/

use std::marker::PhantomData;
use std::cmp::Ordering;

use frame::{DataFrame};
use field::{RFieldIdent, FieldIdent};
use field::Value;
use view::{DataView, ViewField};
use store::{DataStore};
// use data_types::{MaxLen, CreateStorage, DataType, TypeSelector, AssocTypes, DTypeList};
use apply::sort::{DtOrd, sort_order};
use select::{FSelect};
use access::{DataIndex};
use cons::Nil;
use fieldlist::{FieldCons, FSelector};
// use store::{CopyIntoFn};
use error::*;

/// Join information used to describe the type of join being used.
#[derive(Debug, Clone)]
pub struct Join<LIdent, RIdent> {
    /// Join kind: Inner, Outer, or Cross
    pub kind: JoinKind,
    /// Join predicate: equijoin, inequality join
    pub predicate: Predicate,
    _lident: PhantomData<LIdent>,
    _rident: PhantomData<RIdent>,
}
impl<LIdent, RIdent> Join<LIdent, RIdent> {
    /// Create a new `Join` over the specified fields.
    pub fn new(kind: JoinKind, predicate: Predicate) -> Join<LIdent, RIdent>
    {
        Join {
            kind,
            predicate,
            _lident: PhantomData,
            _rident: PhantomData,
        }
    }

    /// Helper function to create a new `Join` with an 'Equal' predicate.
    pub fn equal(kind: JoinKind) -> Join<LIdent, RIdent>
    {
        Join {
            kind,
            predicate: Predicate::Equal,
            _lident: PhantomData,
            _rident: PhantomData,
        }
    }
    /// Helper function to create a new `Join` with an 'Less Than' predicate.
    pub fn less_than(kind: JoinKind) -> Join<LIdent, RIdent>
    {
        Join {
            kind,
            predicate: Predicate::LessThan,
            _lident: PhantomData,
            _rident: PhantomData,
        }
    }
    /// Helper function to create a new `Join` with an 'Less Than or Equal' predicate.
    pub fn less_than_equal(kind: JoinKind) -> Join<LIdent, RIdent>
    {
        Join {
            kind,
            predicate: Predicate::LessThanEqual,
            _lident: PhantomData,
            _rident: PhantomData,
        }
    }
    /// Helper function to create a new `Join` with an 'Greater Than' predicate.
    pub fn greater_than(kind: JoinKind) -> Join<LIdent, RIdent>
    {
        Join {
            kind,
            predicate: Predicate::GreaterThan,
            _lident: PhantomData,
            _rident: PhantomData,
        }
    }
    /// Helper function to create a new `Join` with an 'Greater Than or Equal' predicate.
    pub fn greater_than_equal(kind: JoinKind) -> Join<LIdent, RIdent>
    {
        Join {
            kind,
            predicate: Predicate::GreaterThanEqual,
            _lident: PhantomData,
            _rident: PhantomData,
        }
    }


}

/// The kind of join
#[derive(Debug, Clone, Copy)]
pub enum JoinKind {
    /// Inner Join
    Inner,
    /// Left Outer Join (simply reverse order of call to join() for right outer join)
    Outer,
    /// Full Outer Join, not yet implemented
    // FullOuter,
    /// Cross Join (cartesian product)
    Cross,
}
/// Join predicate (comparison operator between two sides of the join)
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Predicate {
    /// Comparison 'left == right'
    Equal,
    /// Comparison 'left < right'
    LessThan,
    /// Comparison 'left <= right'
    LessThanEqual,
    /// Comparison 'left > right'
    GreaterThan,
    /// Comparison 'left >= right'
    GreaterThanEqual,
}
impl Predicate {
    fn is_equality_pred(self) -> bool {
        self == Predicate::Equal || self == Predicate::GreaterThanEqual
            || self == Predicate::LessThanEqual
    }
    fn is_greater_than_pred(self) -> bool {
        self == Predicate::GreaterThan || self == Predicate::GreaterThanEqual
    }
    fn is_less_than_pred(self) -> bool {
        self == Predicate::LessThan || self == Predicate::LessThanEqual
    }
    fn apply<T>(self, left: Value<&T>, right: Value<&T>) -> PredResults
        where T: PartialEq + DtOrd
    {
        match self {
            Predicate::Equal => {
                match left.dt_cmp(&right) {
                    Ordering::Less => PredResults::Advance { left: true, right: false },
                    Ordering::Equal => PredResults::Add,
                    Ordering::Greater => PredResults::Advance { left: false, right: true },
                }
            },
            Predicate::LessThan => {
                match left.dt_cmp(&right) {
                    Ordering::Less => PredResults::Add,
                    _ => PredResults::Advance { left: false, right: true },
                }
            },
            Predicate::LessThanEqual => {
                match left.dt_cmp(&right) {
                    Ordering::Greater => PredResults::Advance { left: false, right: true },
                    _ => PredResults::Add
                }
            },
            Predicate::GreaterThan => {
                match left.dt_cmp(&right) {
                    Ordering::Greater => PredResults::Add,
                    _ => PredResults::Advance { left: true, right: false }
                }
            },
            Predicate::GreaterThanEqual => {
                match left.dt_cmp(&right) {
                    Ordering::Less => PredResults::Advance { left: true, right: false },
                    _ => PredResults::Add
                }
            }
        }
    }
}
#[derive(Debug, Copy, Clone, PartialEq)]
enum PredResults {
    Add,
    Advance {
        left: bool,
        right: bool
    }
}

pub trait MergeFields {
    type OutFields;
}
impl MergeFields for (Nil, Nil)
{
    type OutFields = Nil;
}
impl<LIdent, LFIdx, LDType, LTail> MergeFields
    for (FieldCons<LIdent, LFIdx, LDType, LTail>, Nil)
    where (LTail, Nil): MergeFields
{
    type OutFields = FieldCons<
        LIdent,
        LFIdx,
        LDType,
        <(LTail, Nil) as MergeFields>::OutFields
    >;
}
impl <RIdent, RFIdx, RDType, RTail> MergeFields
    for (Nil, FieldCons<RIdent, RFIdx, RDType, RTail>)
    where (Nil, RTail): MergeFields
{
    type OutFields = FieldCons<
        RIdent,
        RFIdx,
        RDType,
        <(Nil, RTail) as MergeFields>::OutFields
    >;
}
impl<LIdent, LFIdx, LDType, LTail, RIdent, RFIdx, RDType, RTail> MergeFields
    for (FieldCons<LIdent, LFIdx, LDType, LTail>, FieldCons<RIdent, RFIdx, RDType, RTail>)
    where (LTail, FieldCons<RIdent, RFIdx, RDType, RTail>): MergeFields
{
    type OutFields = FieldCons<
        LIdent,
        LFIdx,
        LDType,
        <(LTail, FieldCons<RIdent, RFIdx, RDType, RTail>) as MergeFields>::OutFields
    >;
}

/// Join two dataviews with specified `Join` using hash join algorithm. Only valid for
/// joins with the 'Equal' predicate.
//TODO: implement hash_join!
#[allow(dead_code)]
pub(crate) fn hash_join<LFields, RFields>(
    _left: &DataView<LFields>, _right: &DataView<RFields>, join: &Join
)
    -> Result<DataStore<<(LFields, RFields) as MergeFields>::OutFields>>
    where (LFields, RFields): MergeFields
    // where DTypes: DTypeList
{
    assert_eq!(join.predicate, Predicate::Equal, "hash_join only valid for equijoins");

    unimplemented!();
}

/// Join two dataviews with specified `Join` using the sort-merge algorithm.
pub(crate) fn sort_merge_join<'b, LFields, RFields, LIdent, LFIdx, RIdent, RFIdx>(
    left: &'b DataView<LFields>, right: &'b DataView<RFields>, join: &Join<LIdent, RIdent>
)
    -> Result<DataStore<<(LFields, RFields) as MergeFields>::OutFields>>
    where (LFields, RFields): MergeFields,
          LFields: FSelector<LIdent, LFIdx>,
          RFields: FSelector<RIdent, RFIdx, DType=<LFields as FSelector<LIdent, LFIdx>>::DType>,
          <LFields as FSelector<LIdent, LFIdx>>::DType: DtOrd + PartialEq + Default,
          // DTypes::Storage: MaxLen<DTypes>
          //         + TypeSelector<DTypes, T>
          //         + CreateStorage
          //         + for<'c> FramedMapExt<DTypes, CopyIntoFn<'c, DTypes>, ()>
{
    // return early if fields don't exist, don't match types, or if DataViews are empty
    if !left.has_field(&join.left_ident) {
        return Err(AgnesError::FieldNotFound(join.left_ident.clone()));
    }
    if !right.has_field(&join.right_ident) {
        return Err(AgnesError::FieldNotFound(join.right_ident.clone()));
    }
    if left.get_field_type(&join.left_ident) != right.get_field_type(&join.right_ident) {
        return Err(AgnesError::TypeMismatch("unable to join on fields of different types".into()));
    }
    if left.is_empty() || right.is_empty() {
        return Ok(DataStore::empty());
    }
    // sort (or rather, get the sorted order for field being merged)
    // we already checked if fields exist in DataViews, so unwraps are safe
    let left_perm = sort_order(&left.field::<LIdent, LFIdx>(join.left_ident.clone()).unwrap());
    let right_perm = sort_order(&right.field::<RIdent, RFIdx>(join.right_ident.clone()).unwrap());

    let merge_indices = merge_field_data(
        &left_perm,
        &right_perm,
        &left.field::<LIdent, LFIdx>(),
        &right.field::<RIdent, RFIdx>(),
        join.predicate
    );

    // compute merged frame list and field list for the new dataframe
    // compute the field list for the new dataframe
    let (_, other_frame_indices) = compute_merged_frames(left, right);
    let MergedFields { right_view_idents, new_fields } =
        compute_merged_field_list(left, right, &other_frame_indices, join)?;
    // create new datastore with fields of both left and right
    let mut ds = DataStore::empty();
    let new_field_idents = new_fields.iter()
        .map(|(_, view_field)| view_field.rident.to_renamed_field_ident())
        .collect::<Vec<_>>();

    let mut field_idx = 0;
    for left_ident in left.fields.keys() {
        for (left_idx, _) in &merge_indices {
            left.map_ext(
                left_ident,
                CopyIntoFn {
                    src_idx: *left_idx,
                    target_ident: new_field_idents[field_idx].clone(),
                    target_ds: &mut ds
                },
            )?;
        }
        field_idx += 1;
    }
    for right_ident in &right_view_idents {
        for (_, right_idx) in &merge_indices {
            right.map_ext(
                right_ident,
                CopyIntoFn {
                    src_idx: *right_idx,
                    target_ident: new_field_idents[field_idx].clone(),
                    target_ds: &mut ds
                },
            )?;
        }
        field_idx += 1;
    }

    Ok(ds)
}

fn merge_field_data<'a, T, U>(
    left_perm: &[usize],
    right_perm: &[usize],
    left_key_data: &'a U,
    right_key_data: &'a U,
    predicate: Predicate,
)   -> Vec<(usize, usize)>
    where T: PartialEq + DtOrd,
          U: DataIndex<DType=T> + ?Sized
{
    debug_assert!(!left_perm.is_empty() && !right_perm.is_empty());
    // NOTE: actual_idx = perm[sorted_idx]
    // NOTE: value = key_data.get(actual_idx).unwrap();

    let lval = |sorted_idx| left_key_data.get_datum(left_perm[sorted_idx]).unwrap();
    let rval = |sorted_idx| right_key_data.get_datum(right_perm[sorted_idx]).unwrap();

    // we know left_perm and right_perm both are non-empty, so there is at least one value
    let (mut left_idx, mut right_idx) = (0, 0);
    let mut merge_indices = vec![];
    while left_idx < left_perm.len() && right_idx < right_perm.len() {
        let left_val = lval(left_idx);
        let right_val = rval(right_idx);
        let pred_results = predicate.apply(left_val, right_val);
        match pred_results {
            PredResults::Add => {
                // figure out subsets
                let mut left_subset = vec![left_idx];
                let mut right_subset = vec![right_idx];
                let (mut left_idx_end, mut right_idx_end);
                if predicate.is_equality_pred() {
                    // for equality predicates, add all records with same value
                    left_idx_end = left_idx + 1;
                    while left_idx_end < left_perm.len() && left_val == lval(left_idx_end) {
                        left_subset.push(left_idx_end);
                        left_idx_end += 1;
                    }
                    right_idx_end = right_idx + 1;
                    while right_idx_end < right_perm.len() && right_val == rval(right_idx_end)
                    {
                        right_subset.push(right_idx_end);
                        right_idx_end += 1;
                    }
                } else {
                    left_idx_end = left_idx + 1;
                    right_idx_end = right_idx + 1;
                }
                let (left_eq_end, right_eq_end) = (left_idx_end, right_idx_end);
                if predicate.is_greater_than_pred() {
                    // for greater-than predicates, we can add the rest of the left values
                    while left_idx_end < left_perm.len() {
                        left_subset.push(left_idx_end);
                        left_idx_end += 1;
                    }
                }
                if predicate.is_less_than_pred() {
                    // for less-than predicates, we can add the rest of the right values
                    while right_idx_end < right_perm.len() {
                        right_subset.push(right_idx_end);
                        right_idx_end += 1;
                    }
                }
                // add cross product of subsets to merge indices
                for lidx in &left_subset {
                    // NAs shouldn't match a predicate, only add if value exists
                    if lval(*lidx).exists() {
                        for ridx in &right_subset {
                            if rval(*ridx).exists() {
                                merge_indices.push((left_perm[*lidx], right_perm[*ridx]));
                            }
                        }

                    }
                }
                // advance as needed
                match predicate {
                    Predicate::Equal => {
                        left_idx = left_eq_end;
                        right_idx = right_eq_end;
                    },
                    Predicate::GreaterThanEqual => {
                        right_idx = right_eq_end;
                    },
                    Predicate::GreaterThan => {
                        right_idx += 1;
                    },
                    Predicate::LessThanEqual => {
                        left_idx = left_eq_end;
                    },
                    Predicate::LessThan => {
                        left_idx += 1;
                    }
                }
            },
            PredResults::Advance { left, right } => {
                if left {
                    left_idx += 1;
                }
                if right {
                    right_idx += 1;
                }
            }
        }
    }
    merge_indices
}

pub(crate) fn compute_merged_frames<LFields, RFields>(
    left: &DataView<LFields>, right: &DataView<RFields>
)
    -> (Vec<DataFrame<<(LFields, RFields) as MergeFields>::OutFields>>, Vec<usize>)
        where (LFields, RFields): MergeFields
{
    // new frame vector is combination, without repetition, of existing frame vectors. also
    // keep track of the frame indices (for frame_idx) of the 'right' fields
    let mut new_frames = left.frames.clone();
    let mut right_frame_indices = vec![];
    for right_frame in &right.frames {
        match new_frames.iter().enumerate().find(|&(_, frame)| frame.has_same_store(right_frame)) {
            Some((idx, _)) => {
                right_frame_indices.push(idx);
            },
            None => {
                right_frame_indices.push(new_frames.len());
                new_frames.push(right_frame.clone());
            }
        }
    }
    (new_frames, right_frame_indices)
}

pub(crate) struct MergedFields {
    pub(crate) right_view_idents: Vec<FieldIdent>,
    pub(crate) new_fields: Vec<(FieldIdent, ViewField)>,
}

pub(crate) fn compute_merged_field_list<'a, DTypes, T: Into<Option<&'a Join>>>(
    left: &DataView<DTypes>, right: &DataView<DTypes>, right_frame_mapping: &[usize], join: T
)
    -> Result<MergedFields>
    where DTypes: DTypeList
{
    // build new fields vector, updating the frame indices in the ViewFields copied
    // from the 'right' fields list
    let mut new_fields = left.fields.clone();
    let mut right_idents = vec![];
    let mut field_collisions = vec![];
    let join = join.into();
    for (right_fieldname, right_field) in &right.fields {
        if new_fields.contains_key(right_fieldname) {
            // possible collision, see if collision is on join field
            if let Some(join) = join {
                if join.left_ident == join.right_ident && &join.left_ident == right_fieldname {
                    // collision on the join field
                    // * for equijoins, we only need one of the two (since they're the same), so we
                    // don't have to do anything
                    // * for non-equijoins, we rename both
                    if join.predicate != Predicate::Equal {
                        // unwrap safe, we can only get here if left_ident in new_fields
                        let mut left_key_field = new_fields.get(&join.left_ident).unwrap().clone();
                        let new_left_ident_name = format!("{}.0", join.left_ident);
                        left_key_field.rident.rename = Some(new_left_ident_name.clone());
                        new_fields.insert(new_left_ident_name.into(), left_key_field);
                        new_fields.swap_remove(&join.left_ident);

                        let new_right_ident_name = format!("{}.1", join.right_ident);
                        right_idents.push(right_fieldname.clone());
                        new_fields.insert(new_right_ident_name.clone().into(), ViewField {
                            rident: RFieldIdent {
                                ident: right_field.rident.ident.clone(),
                                rename: Some(new_right_ident_name),
                            },
                            frame_idx: right_frame_mapping[right_field.frame_idx]
                        });
                    }
                } else {
                    field_collisions.push(right_fieldname.clone());
                }
            } else {
                field_collisions.push(right_fieldname.clone());
            }
            continue;
        }
        right_idents.push(right_fieldname.clone());
        new_fields.insert(right_fieldname.clone(), ViewField {
            rident: right_field.rident.clone(),
            frame_idx: right_frame_mapping[right_field.frame_idx],
        });
    }
    if field_collisions.is_empty() {
        Ok(MergedFields {
            right_view_idents: right_idents,
            new_fields: new_fields.drain(..).collect::<Vec<_>>()
        })
    } else {
        Err(AgnesError::FieldCollision(field_collisions))
    }
}

#[cfg(test)]
mod tests {
    use super::{Join, JoinKind};
    use field::{Value, FieldData};
    use apply::sort::sort_order;
    use filter::Filter;
    use test_utils::*;

    // use data_types::standard::*;

    // #[test]
    // fn sort_order_no_na() {
    //     let field_data: FieldData<Types, u64> = FieldData::from_vec(vec![2u64, 5, 3, 1, 8]);
    //     let sorted_order = sort_order(&field_data);
    //     assert_eq!(sorted_order, vec![3, 0, 2, 1, 4]);

    //     let field_data: FieldData<Types, f64> =
    //         FieldData::from_vec(vec![2.0, 5.4, 3.1, 1.1, 8.2]);
    //     let sorted_order = sort_order(&field_data);
    //     assert_eq!(sorted_order, vec![3, 0, 2, 1, 4]);

    //     let field_data: FieldData<Types, f64> =
    //         FieldData::from_vec(vec![2.0, ::std::f64::NAN, 3.1, 1.1, 8.2]);
    //     let sorted_order = sort_order(&field_data);
    //     assert_eq!(sorted_order, vec![1, 3, 0, 2, 4]);

    //     let field_data: FieldData<Types, f64> =
    //         FieldData::from_vec(vec![2.0, ::std::f64::NAN, 3.1, ::std::f64::INFINITY, 8.2]);
    //     let sorted_order = sort_order(&field_data);
    //     assert_eq!(sorted_order, vec![1, 0, 2, 4, 3]);
    // }

    // #[test]
    // fn sort_order_na() {
    //     let field_data = FieldData::<Types, _>::from_field_vec(vec![
    //         Value::Exists(2u64),
    //         Value::Exists(5),
    //         Value::Na,
    //         Value::Exists(1),
    //         Value::Exists(8)
    //     ]);
    //     let sorted_order = sort_order(&field_data);
    //     assert_eq!(sorted_order, vec![2, 3, 0, 1, 4]);

    //     let field_data = FieldData::<Types, _>::from_field_vec(vec![
    //         Value::Exists(2.1),
    //         Value::Exists(5.5),
    //         Value::Na,
    //         Value::Exists(1.1),
    //         Value::Exists(8.2930)
    //     ]);
    //     let sorted_order = sort_order(&field_data);
    //     assert_eq!(sorted_order, vec![2, 3, 0, 1, 4]);

    //     let field_data = FieldData::<Types, _>::from_field_vec(vec![
    //         Value::Exists(2.1),
    //         Value::Exists(::std::f64::NAN),
    //         Value::Na,
    //         Value::Exists(1.1),
    //         Value::Exists(8.2930)
    //     ]);
    //     let sorted_order = sort_order(&field_data);
    //     assert_eq!(sorted_order, vec![2, 1, 3, 0, 4]);

    //     let field_data = FieldData::<Types, _>::from_field_vec(vec![
    //         Value::Exists(2.1),
    //         Value::Exists(::std::f64::NAN),
    //         Value::Na,
    //         Value::Exists(::std::f64::INFINITY),
    //         Value::Exists(8.2930)
    //     ]);
    //     let sorted_order = sort_order(&field_data);
    //     assert_eq!(sorted_order, vec![2, 1, 0, 4, 3]);
    // }

    // #[test]
    // fn inner_equi_join() {
    //     let ds1: DataStore = sample_emp_table();
    //     let ds2: DataStore = sample_dept_table();

    //     let (dv1, dv2): (DataView, DataView) = (ds1.into(), ds2.into());
    //     println!("{}", dv1);
    //     println!("{}", dv2);
    //     let joined_dv: DataView = dv1.join::<u64>(&dv2, &Join::equal(
    //         JoinKind::Inner,
    //         "DeptId",
    //         "DeptId"
    //     )).expect("join failure").into();
    //     // println!("{}", joined_dv);
    //     assert_eq!(joined_dv.nrows(), 7);
    //     assert_eq!(joined_dv.nfields(), 4);
    //     unsigned::assert_dv_sorted_eq(&joined_dv, &"EmpId".into(),
    //         vec![0u64, 2, 5, 6, 8, 9, 10]);
    //     unsigned::assert_dv_sorted_eq(&joined_dv, &"DeptId".into(),
    //         vec![1u64, 2, 1, 1, 3, 4, 4]);
    //     text::assert_dv_sorted_eq(&joined_dv, &"EmpName".into(),
    //         vec!["Sally", "Jamie", "Bob", "Louis", "Louise", "Cara", "Ann"]
    //     );
    //     text::assert_dv_sorted_eq(&joined_dv, &"DeptName".into(),
    //         vec!["Marketing", "Sales", "Marketing", "Marketing", "Manufacturing", "R&D", "R&D"]
    //     );
    // }

    // #[test]
    // fn inner_equi_join_missing_dept_id() {
    //     // dept id missing from dept table, should remove the entire marketing department from join
    //     let ds1 = sample_emp_table();
    //     let ds2 = dept_table_from_field(
    //         FieldData::<Types, _>::from_field_vec(vec![
    //             Value::Na,
    //             Value::Exists(2),
    //             Value::Exists(3),
    //             Value::Exists(4)
    //         ]),
    //         FieldData::<Types, _>::from_field_vec(vec![
    //             Value::Exists("Marketing".into()),
    //             Value::Exists("Sales".into()),
    //             Value::Exists("Manufacturing".into()),
    //             Value::Exists("R&D".into()),
    //         ])
    //     );

    //     let (dv1, dv2): (DataView, DataView) = (ds1.into(), ds2.into());
    //     // println!("{}", dv1);
    //     // println!("{}", dv2);
    //     let joined_dv: DataView = dv1.join::<u64>(&dv2, &Join::equal(
    //         JoinKind::Inner,
    //         "DeptId",
    //         "DeptId"
    //     )).expect("join failure").into();
    //     // println!("{}", joined_dv);
    //     assert_eq!(joined_dv.nrows(), 4);
    //     assert_eq!(joined_dv.nfields(), 4);
    //     unsigned::assert_dv_sorted_eq(&joined_dv, &"EmpId".into(),
    //         vec![2u64, 8, 9, 10]);
    //     unsigned::assert_dv_sorted_eq(&joined_dv, &"DeptId".into(),
    //         vec![2u64, 3, 4, 4]);
    //     text::assert_dv_sorted_eq(&joined_dv, &"EmpName".into(),
    //         vec!["Jamie", "Louis", "Louise", "Ann"]);
    //     text::assert_dv_sorted_eq(&joined_dv, &"DeptName".into(),
    //         vec!["Sales", "Manufacturing", "R&D", "R&D"]);

    //     // dept id missing from emp table, should remove single employee from join
    //     let ds1 = emp_table_from_field(
    //         FieldData::<Types, _>::from_field_vec(vec![
    //             Value::Exists(0),
    //             Value::Exists(2),
    //             Value::Exists(5),
    //             Value::Exists(6),
    //             Value::Exists(8),
    //             Value::Exists(9),
    //             Value::Exists(10),
    //         ]),
    //         FieldData::<Types, _>::from_field_vec(vec![
    //             Value::Exists(1),
    //             Value::Exists(2),
    //             Value::Na, // Bob's department isn't specified
    //             Value::Exists(1),
    //             Value::Exists(3),
    //             Value::Exists(4),
    //             Value::Exists(4),
    //         ]),
    //         FieldData::<Types, _>::from_field_vec(vec![
    //             Value::Exists("Sally".into()),
    //             Value::Exists("Jamie".into()),
    //             Value::Exists("Bob".into()),
    //             Value::Exists("Cara".into()),
    //             Value::Exists("Louis".into()),
    //             Value::Exists("Louise".into()),
    //             Value::Exists("Ann".into()),
    //         ]),
    //     );
    //     let ds2 = sample_dept_table();
    //     let (dv1, dv2): (DataView, DataView) = (ds1.into(), ds2.into());
    //     // println!("{}", dv1);
    //     // println!("{}", dv2);
    //     let joined_dv: DataView = dv1.join::<u64>(&dv2, &Join::equal(
    //         JoinKind::Inner,
    //         "DeptId",
    //         "DeptId"
    //     )).expect("join failure").into();
    //     // println!("{}", joined_dv);
    //     assert_eq!(joined_dv.nrows(), 6);
    //     assert_eq!(joined_dv.nfields(), 4);
    //     unsigned::assert_dv_sorted_eq(&joined_dv, &"EmpId".into(),
    //         vec![0u64, 2, 6, 8, 9, 10]);
    //     unsigned::assert_dv_sorted_eq(&joined_dv, &"DeptId".into(),
    //         vec![1u64, 2, 1, 3, 4, 4]);
    //     text::assert_dv_sorted_eq(&joined_dv, &"EmpName".into(),
    //         vec!["Sally", "Jamie", "Louis", "Louise", "Cara", "Ann"]
    //     );
    //     text::assert_dv_sorted_eq(&joined_dv, &"DeptName".into(),
    //         vec!["Marketing", "Sales", "Marketing", "Manufacturing", "R&D", "R&D"]
    //     );
    // }

    // #[test]
    // fn filter_inner_equi_join() {
    //     // should have same results as first test in inner_equi_join_missing_dept_id
    //     let ds1 = sample_emp_table();
    //     let ds2 = sample_dept_table();
    //     println!("{:?}", ds1);
    //     let (dv1, mut dv2): (DataView, DataView) = (ds1.into(), ds2.into());
    //     println!("{}", dv1);
    //     println!("{}", dv2);

    //     dv2.filter("DeptId", |val: &u64| *val != 1u64).unwrap();
    //     println!("{}", dv2);
    //     let joined_dv: DataView = dv1.join::<u64>(&dv2, &Join::equal(
    //         JoinKind::Inner,
    //         "DeptId",
    //         "DeptId"
    //     )).expect("join failure").into();
    //     println!("{}", joined_dv);
    //     assert_eq!(joined_dv.nrows(), 4);
    //     assert_eq!(joined_dv.nfields(), 4);
    //     unsigned::assert_dv_sorted_eq(&joined_dv, &"EmpId".into(),
    //         vec![2u64, 8, 9, 10]);
    //     unsigned::assert_dv_sorted_eq(&joined_dv, &"DeptId".into(),
    //         vec![2u64, 3, 4, 4]);
    //     text::assert_dv_sorted_eq(&joined_dv, &"EmpName".into(),
    //         vec!["Jamie", "Louis", "Louise", "Ann"]);
    //     text::assert_dv_sorted_eq(&joined_dv, &"DeptName".into(),
    //         vec!["Sales", "Manufacturing", "R&D", "R&D"]);
    // }

    // #[test]
    // fn inner_nonequi_join() {
    //     // greater than
    //     let ds1 = sample_emp_table();
    //     let ds2 = dept_table(vec![1, 2], vec!["Marketing", "Sales"]);

    //     let (dv1, mut dv2): (DataView, DataView) = (ds1.into(), ds2.into());
    //     // println!("~~\n>\n~~\n{}\n{}", dv1, dv2);
    //     // also test renaming
    //     dv2.rename("DeptId", "RightDeptId").expect("rename failed");
    //     let joined_dv: DataView = dv1.join::<u64>(&dv2, &Join::greater_than(
    //         JoinKind::Inner,
    //         "DeptId",
    //         "RightDeptId"
    //     )).expect("join failure").into();
    //     // println!("{}", joined_dv);
    //     assert_eq!(joined_dv.nrows(), 7);
    //     assert_eq!(joined_dv.nfields(), 5);
    //     unsigned::assert_dv_pred(&joined_dv, &"DeptId".into(),
    //         |&deptid| deptid >= 2);

    //     // greater than equal
    //     let ds1 = sample_emp_table();
    //     let ds2 = dept_table(vec![2], vec!["Sales"]);
    //     let (dv1, dv2): (DataView, DataView) = (ds1.into(), ds2.into());
    //     // println!("~~\n>=\n~~\n+{}\n{}", dv1, dv2);
    //     let joined_dv: DataView = dv1.join::<u64>(&dv2, &Join::greater_than_equal(
    //         JoinKind::Inner,
    //         "DeptId",
    //         "DeptId"
    //     )).expect("join failure").into();
    //     // println!("{}", joined_dv);
    //     assert_eq!(joined_dv.nrows(), 4);
    //     assert_eq!(joined_dv.nfields(), 5);
    //     unsigned::assert_dv_pred(&joined_dv, &"DeptId.0".into(),
    //         |&deptid| deptid >= 2);

    //     // less than
    //     let ds1 = sample_emp_table();
    //     let ds2 = dept_table(vec![2], vec!["Sales"]);
    //     let (dv1, dv2): (DataView, DataView) = (ds1.into(), ds2.into());
    //     // println!("~~\n<\n~~\n{}\n{}", dv1, dv2);
    //     let joined_dv: DataView = dv1.join::<u64>(&dv2, &Join::less_than(
    //         JoinKind::Inner,
    //         "DeptId",
    //         "DeptId"
    //     )).expect("join failure").into();
    //     // println!("{}", joined_dv);
    //     assert_eq!(joined_dv.nrows(), 3);
    //     assert_eq!(joined_dv.nfields(), 5);
    //     unsigned::assert_dv_pred(&joined_dv, &"DeptId.0".into(),
    //         |&deptid| deptid == 1);

    //     // less than equal
    //     let ds1 = sample_emp_table();
    //     let ds2 = dept_table(vec![2], vec!["Sales"]);
    //     let (dv1, dv2): (DataView, DataView) = (ds1.into(), ds2.into());
    //     // println!("~~\n<=\n~~\n{}\n{}", dv1, dv2);
    //     let joined_dv: DataView = dv1.join::<u64>(&dv2, &Join::less_than_equal(
    //         JoinKind::Inner,
    //         "DeptId",
    //         "DeptId"
    //     )).expect("join failure").into();
    //     // println!("{}", joined_dv);
    //     assert_eq!(joined_dv.nrows(), 4);
    //     assert_eq!(joined_dv.nfields(), 5);
    //     unsigned::assert_dv_pred(&joined_dv, &"DeptId.0".into(),
    //         |&deptid| deptid <= 2);
    // }
}
