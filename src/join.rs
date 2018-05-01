/*!
`DataView` join structs and implementations.
*/


use indexmap::IndexMap;

use frame::{DataFrame};
use field::{RFieldIdent, DataType, FieldIdent};
use masked::MaybeNa;
use view::{DataView, ViewField};
use store::{DataStore};
use apply::*;
use error::*;

/// Join information used to describe the type of join being used.
#[derive(Debug, Clone)]
pub struct Join {
    /// Join kind: Inner, Outer, or Cross
    pub kind: JoinKind,
    /// Join predicate: equijoin, inequality join
    pub predicate: Predicate,
    pub(crate) left_field: FieldIdent,
    pub(crate) right_field: FieldIdent,
}
impl Join {
    /// Create a new `Join` over the specified fields.
    pub fn new<L: Into<FieldIdent>, R: Into<FieldIdent>>(kind: JoinKind, predicate: Predicate,
        left_field: L, right_field: R) -> Join
    {
        Join {
            kind,
            predicate,
            left_field: left_field.into(),
            right_field: right_field.into()
        }
    }

    /// Helper function to create a new `Join` with an 'Equal' predicate.
    pub fn equal<L: Into<FieldIdent>, R: Into<FieldIdent>>(kind: JoinKind, left_field: L,
        right_field: R) -> Join
    {
        Join {
            kind,
            predicate: Predicate::Equal,
            left_field: left_field.into(),
            right_field: right_field.into(),
        }
    }
    /// Helper function to create a new `Join` with an 'Less Than' predicate.
    pub fn less_than<L: Into<FieldIdent>, R: Into<FieldIdent>>(kind: JoinKind, left_field: L,
        right_field: R) -> Join
    {
        Join {
            kind,
            predicate: Predicate::LessThan,
            left_field: left_field.into(),
            right_field: right_field.into(),
        }
    }
    /// Helper function to create a new `Join` with an 'Less Than or Equal' predicate.
    pub fn less_than_equal<L: Into<FieldIdent>, R: Into<FieldIdent>>(kind: JoinKind, left_field: L,
        right_field: R) -> Join
    {
        Join {
            kind,
            predicate: Predicate::LessThanEqual,
            left_field: left_field.into(),
            right_field: right_field.into(),
        }
    }
    /// Helper function to create a new `Join` with an 'Greater Than' predicate.
    pub fn greater_than<L: Into<FieldIdent>, R: Into<FieldIdent>>(kind: JoinKind, left_field: L,
        right_field: R) -> Join
    {
        Join {
            kind,
            predicate: Predicate::GreaterThan,
            left_field: left_field.into(),
            right_field: right_field.into(),
        }
    }
    /// Helper function to create a new `Join` with an 'Greater Than or Equal' predicate.
    pub fn greater_than_equal<L: Into<FieldIdent>, R: Into<FieldIdent>>(kind: JoinKind,
        left_field: L, right_field: R) -> Join
    {
        Join {
            kind,
            predicate: Predicate::GreaterThanEqual,
            left_field: left_field.into(),
            right_field: right_field.into(),
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
    fn is_equality_pred(&self) -> bool {
        *self == Predicate::Equal || *self == Predicate::GreaterThanEqual
            || *self == Predicate::LessThanEqual
    }
    fn is_greater_than_pred(&self) -> bool {
        *self == Predicate::GreaterThan || *self == Predicate::GreaterThanEqual
    }
    fn is_less_than_pred(&self) -> bool {
        *self == Predicate::LessThan || *self == Predicate::LessThanEqual
    }
    fn apply<T: DataType>(&self, left: &MaybeNa<T>, right: &MaybeNa<T>) -> PredResults {
        match *self {
            Predicate::Equal => {
                if left == right {
                    PredResults::Add
                } else if left < right {
                    PredResults::Advance { left: true, right: false }
                } else {
                    // right < left
                    PredResults::Advance { left: false, right: true }
                }
            },
            Predicate::LessThan => {
                if left < right {
                    PredResults::Add
                } else {
                    PredResults::Advance { left: false, right: true }
                }
            },
            Predicate::LessThanEqual => {
                if left <= right {
                    PredResults::Add
                } else {
                    PredResults::Advance { left: false, right: true }
                }
            },
            Predicate::GreaterThan => {
                if left > right {
                    PredResults::Add
                } else {
                    PredResults::Advance { left: true, right: false }
                }
            },
            Predicate::GreaterThanEqual => {
                if left >= right {
                    PredResults::Add
                } else {
                    PredResults::Advance { left: true, right: false }
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

/// Join two dataviews with specified `Join` using hash join algorithm. Only valid for
/// joins with the 'Equal' predicate.
pub fn hash_join(_left: &DataView, _right: &DataView, join: Join) -> Result<DataStore> {
    assert_eq!(join.predicate, Predicate::Equal, "hash_join only valid for equijoins");

    unimplemented!();
}

/// Join two dataviews with specified `Join` using the sort-merge algorithm.
pub fn sort_merge_join(left: &DataView, right: &DataView, join: Join) -> Result<DataStore> {
    // return early if fields don't exist, don't match types, or if DataViews are empty
    if !left.has_field(&join.left_field) {
        return Err(AgnesError::FieldNotFound(join.left_field.clone().into()));
    }
    if !right.has_field(&join.right_field) {
        return Err(AgnesError::FieldNotFound(join.right_field.clone().into()));
    }
    if left.get_field_type(&join.left_field) != right.get_field_type(&join.right_field) {
        return Err(AgnesError::TypeMismatch("unable to join on fields of different types".into()));
    }
    if left.is_empty() || right.is_empty() {
        return Ok(DataStore::empty());
    }
    return Ok(DataStore::empty());
    /*
    // sort (or rather, get the sorted order for field being merged)
    // we already checks if fields exist in DataViews, so unwraps are safe
    let left_perm = left.sort_order_by(FieldSelector(&join.left_field)).unwrap();
    let right_perm = right.sort_order_by(FieldSelector(&join.right_field)).unwrap();

    struct FindMergeIndices {
        left_perm: Vec<usize>,
        right_perm: Vec<usize>,
        predicate: Predicate,
    }
    macro_rules! impl_find_merge_indices {
        ($name:tt; $ty:ty) => {
            fn $name<'a, T: DataIndex<$ty>>(&mut self, field: &(&T, &T)) -> Vec<(usize, usize)> {
                merge_masked_data(&self.left_perm, &self.right_perm, field.0, field.1,
                    self.predicate)
            }
        }
    }
    impl Field2Fn for FindMergeIndices {
        type Output = Vec<(usize, usize)>;

        impl_find_merge_indices!(apply_unsigned; u64);
        impl_find_merge_indices!(apply_signed;   i64);
        impl_find_merge_indices!(apply_text;     String);
        impl_find_merge_indices!(apply_boolean;  bool);
        impl_find_merge_indices!(apply_float;    f64);
    }
    // find the join indices
    let merge_indices = (left, right).apply_to_field2(FindMergeIndices {
        left_perm,
        right_perm,
        predicate: join.predicate
    }, (FieldSelector(&join.left_field), FieldSelector(&join.right_field)))?;

    // compute merged frame list and field list for the new dataframe
    // compute the field list for the new dataframe
    let (new_frames, other_frame_indices) = compute_merged_frames(left, right);
    let new_fields = compute_merged_field_list(left, right, &other_frame_indices, &join)?;

    // create new datastore with fields of both left and right
    let mut new_field_idents = vec![];
    let mut ds = DataStore::with_fields(
        new_fields.values()
        .map(|&ref view_field| {
            let new_ident = view_field.rident.to_renamed_field_ident();
            new_field_idents.push(new_ident.clone());
            let field_type = new_frames[view_field.frame_idx]
                .get_field_type(&view_field.rident.ident)
                .expect("compute_merged_frames/field_list failed");
            TypedFieldIdent {
                ident: new_ident,
                ty: field_type,
            }
        })
        .collect::<Vec<_>>());

    struct AddToDs<'a> {
        ds: &'a mut DataStore,
        ident: FieldIdent
    }
    macro_rules! impl_add_to_ds {
        ($name:tt; $ty:ty) => {
            fn $name(&mut self, value: MaybeNa<&$ty>) {
                self.ds.add(self.ident.clone(), value.cloned())
            }
        }
    }
    impl<'a> ElemFn for AddToDs<'a> {
        type Output = ();
        impl_add_to_ds!(apply_unsigned; u64);
        impl_add_to_ds!(apply_signed;   i64);
        impl_add_to_ds!(apply_text;     String);
        impl_add_to_ds!(apply_boolean;  bool);
        impl_add_to_ds!(apply_float;    f64);
    }
    for (left_idx, right_idx) in merge_indices {
        let mut field_idx = 0;
        for left_ident in left.fields.keys() {
            left.apply_to_elem(AddToDs { ds: &mut ds, ident: new_field_idents[field_idx].clone() },
                &FieldIndexSelector(&left_ident, left_idx))?;
            field_idx += 1;
        }
        for right_ident in right.fields.keys() {
            right.apply_to_elem(AddToDs { ds: &mut ds, ident: new_field_idents[field_idx].clone() },
                &FieldIndexSelector(&right_ident, right_idx))?;
            field_idx += 1;
        }
    }

    Ok(ds)*/
}

fn merge_masked_data<'a, T: DataType, U: DataIndex<T>>(
    left_perm: &Vec<usize>,
    right_perm: &Vec<usize>,
    left_key_data: &'a U,
    right_key_data: &'a U,
    predicate: Predicate,
) -> Vec<(usize, usize)>
{
    debug_assert!(!left_perm.is_empty() && !right_perm.is_empty());
    // NOTE: actual_idx = perm[sorted_idx]
    // NOTE: value = key_data.get(actual_idx).unwrap();

    let lval = |sorted_idx| left_key_data.get_data(left_perm[sorted_idx]).unwrap();
    let rval = |sorted_idx| right_key_data.get_data(right_perm[sorted_idx]).unwrap();

    // we know left_perm and right_perm both are non-empty, so there is at least one value
    let (mut left_idx, mut right_idx) = (0, 0);
    let mut merge_indices = vec![];
    while left_idx < left_perm.len() && right_idx < right_perm.len() {
        let left_val = lval(left_idx);
        let right_val = rval(right_idx);
        let pred_results = predicate.apply(&left_val, &right_val);
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
                        right_idx = right_idx + 1;
                    },
                    Predicate::LessThanEqual => {
                        left_idx = left_eq_end;
                    },
                    Predicate::LessThan => {
                        left_idx = left_idx + 1;
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

pub(crate) fn compute_merged_frames(left: &DataView, right: &DataView)
    -> (Vec<DataFrame>, Vec<usize>)
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

pub(crate) fn compute_merged_field_list<'a, T: Into<Option<&'a Join>>>(left: &DataView,
    right: &DataView, right_frame_mapping: &Vec<usize>, join: T)
    -> Result<IndexMap<FieldIdent, ViewField>>
{
    // build new fields vector, updating the frame indices in the ViewFields copied
    // from the 'right' fields list
    let mut new_fields = left.fields.clone();
    let mut field_coll = vec![];
    let join = join.into();
    for (right_fieldname, right_field) in &right.fields {
        if new_fields.contains_key(right_fieldname) {
            // possible collision, see if collision is on join field
            if let Some(join) = join {
                if join.left_field == join.right_field && &join.left_field == right_fieldname {
                    // collision on the join field, rename both
                    // unwrap safe, we can only get here if left_field in new_fields
                    let mut left_key_field = new_fields.get(&join.left_field).unwrap().clone();
                    let new_left_field_name = format!("{}.0", join.left_field);
                    left_key_field.rident.rename = Some(new_left_field_name.clone());
                    new_fields.insert(new_left_field_name.into(), left_key_field);
                    new_fields.swap_remove(&join.left_field);

                    let new_right_field_name = format!("{}.1", join.right_field);
                    new_fields.insert(new_right_field_name.clone().into(), ViewField {
                        rident: RFieldIdent {
                            ident: right_field.rident.ident.clone(),
                            rename: Some(new_right_field_name),
                        },
                        frame_idx: right_frame_mapping[right_field.frame_idx]
                    });
                } else {
                    field_coll.push(right_fieldname.clone());
                }
            } else {
                field_coll.push(right_fieldname.clone());
            }
            continue;
        }
        new_fields.insert(right_fieldname.clone(), ViewField {
            rident: right_field.rident.clone(),
            frame_idx: right_frame_mapping[right_field.frame_idx],
        });
    }
    if field_coll.is_empty() {
        Ok(new_fields)
    } else {
        Err(AgnesError::FieldCollision(field_coll))
    }
}

#[cfg(test)]
mod tests {
    // use super::*;
    // use masked::{MaybeNa, MaskedData};
    // use test_utils::*;

    /*
    #[test]
    fn sort_order_no_na() {
        let masked_data: MaskedData<u64> = MaskedData::from_vec(vec![2u64, 5, 3, 1, 8]);
        let sorted_order = masked_data.sort_order_by(NilSelector).unwrap();
        assert_eq!(sorted_order, vec![3, 0, 2, 1, 4]);

        let masked_data: MaskedData<f64> = MaskedData::from_vec(vec![2.0, 5.4, 3.1, 1.1, 8.2]);
        let sorted_order = masked_data.sort_order_by(NilSelector).unwrap();
        assert_eq!(sorted_order, vec![3, 0, 2, 1, 4]);

        let masked_data: MaskedData<f64> =
            MaskedData::from_vec(vec![2.0, ::std::f64::NAN, 3.1, 1.1, 8.2]);
        let sorted_order = masked_data.sort_order_by(NilSelector).unwrap();
        assert_eq!(sorted_order, vec![1, 3, 0, 2, 4]);

        let masked_data: MaskedData<f64> = MaskedData::from_vec(vec![2.0, ::std::f64::NAN, 3.1,
            ::std::f64::INFINITY, 8.2]);
        let sorted_order = masked_data.sort_order_by(NilSelector).unwrap();
        assert_eq!(sorted_order, vec![1, 0, 2, 4, 3]);
    }

    #[test]
    fn sort_order_na() {
        let masked_data = MaskedData::from_masked_vec(vec![
            MaybeNa::Exists(2u64),
            MaybeNa::Exists(5),
            MaybeNa::Na,
            MaybeNa::Exists(1),
            MaybeNa::Exists(8)
        ]);
        let sorted_order = masked_data.sort_order_by(NilSelector).unwrap();
        assert_eq!(sorted_order, vec![2, 3, 0, 1, 4]);

        let masked_data = MaskedData::from_masked_vec(vec![
            MaybeNa::Exists(2.1),
            MaybeNa::Exists(5.5),
            MaybeNa::Na,
            MaybeNa::Exists(1.1),
            MaybeNa::Exists(8.2930)
        ]);
        let sorted_order = masked_data.sort_order_by(NilSelector).unwrap();
        assert_eq!(sorted_order, vec![2, 3, 0, 1, 4]);

        let masked_data = MaskedData::from_masked_vec(vec![
            MaybeNa::Exists(2.1),
            MaybeNa::Exists(::std::f64::NAN),
            MaybeNa::Na,
            MaybeNa::Exists(1.1),
            MaybeNa::Exists(8.2930)
        ]);
        let sorted_order = masked_data.sort_order_by(NilSelector).unwrap();
        assert_eq!(sorted_order, vec![2, 1, 3, 0, 4]);

        let masked_data = MaskedData::from_masked_vec(vec![
            MaybeNa::Exists(2.1),
            MaybeNa::Exists(::std::f64::NAN),
            MaybeNa::Na,
            MaybeNa::Exists(::std::f64::INFINITY),
            MaybeNa::Exists(8.2930)
        ]);
        let sorted_order = masked_data.sort_order_by(NilSelector).unwrap();
        assert_eq!(sorted_order, vec![2, 1, 0, 4, 3]);
    }

    #[test]
    fn inner_equi_join() {
        let ds1 = sample_emp_table();
        let ds2 = sample_dept_table();

        let (dv1, dv2): (DataView, DataView) = (ds1.into(), ds2.into());
        println!("{}", dv1);
        println!("{}", dv2);
        let joined_dv: DataView = dv1.join(&dv2, Join::equal(
            JoinKind::Inner,
            "DeptId",
            "DeptId"
        )).expect("join failure").into();
        println!("{}", joined_dv);
        assert_eq!(joined_dv.nrows(), 7);
        assert_eq!(joined_dv.nfields(), 5);
        unsigned::assert_sorted_eq(&joined_dv, &"EmpId".into(),
            vec![0u64, 2, 5, 6, 8, 9, 10]);
        unsigned::assert_sorted_eq(&joined_dv, &"DeptId.0".into(),
            vec![1u64, 2, 1, 1, 3, 4, 4]);
        unsigned::assert_sorted_eq(&joined_dv, &"DeptId.1".into(),
            vec![1u64, 2, 1, 1, 3, 4, 4]);
        text::assert_sorted_eq(&joined_dv, &"EmpName".into(),
            vec!["Sally", "Jamie", "Bob", "Louis", "Louise", "Cara", "Ann"]
        );
        text::assert_sorted_eq(&joined_dv, &"DeptName".into(),
            vec!["Marketing", "Sales", "Marketing", "Marketing", "Manufacturing", "R&D", "R&D"]
        );
    }

    #[test]
    fn inner_equi_join_missing_dept_id() {
        // dept id missing from dept table, should remove the entire marketing department from join
        let ds1 = sample_emp_table();
        let ds2 = dept_table_from_masked(
            MaskedData::from_masked_vec(vec![
                MaybeNa::Na,
                MaybeNa::Exists(2),
                MaybeNa::Exists(3),
                MaybeNa::Exists(4)
            ]),
            MaskedData::from_masked_vec(vec![
                MaybeNa::Exists("Marketing".into()),
                MaybeNa::Exists("Sales".into()),
                MaybeNa::Exists("Manufacturing".into()),
                MaybeNa::Exists("R&D".into()),
            ])
        );

        let (dv1, dv2): (DataView, DataView) = (ds1.into(), ds2.into());
        println!("{}", dv1);
        println!("{}", dv2);
        let joined_dv: DataView = dv1.join(&dv2, Join::equal(
            JoinKind::Inner,
            "DeptId",
            "DeptId"
        )).expect("join failure").into();
        println!("{}", joined_dv);
        assert_eq!(joined_dv.nrows(), 4);
        assert_eq!(joined_dv.nfields(), 5);
        unsigned::assert_sorted_eq(&joined_dv, &"EmpId".into(),
            vec![2u64, 8, 9, 10]);
        unsigned::assert_sorted_eq(&joined_dv, &"DeptId.0".into(),
            vec![2u64, 3, 4, 4]);
        unsigned::assert_sorted_eq(&joined_dv, &"DeptId.1".into(),
            vec![2u64, 3, 4, 4]);
        text::assert_sorted_eq(&joined_dv, &"EmpName".into(),
            vec!["Jamie", "Louis", "Louise", "Ann"]);
        text::assert_sorted_eq(&joined_dv, &"DeptName".into(),
            vec!["Sales", "Manufacturing", "R&D", "R&D"]);

        // dept id missing from emp table, should remove single employee from join
        let ds1 = emp_table_from_masked(
            MaskedData::from_masked_vec(vec![
                MaybeNa::Exists(0),
                MaybeNa::Exists(2),
                MaybeNa::Exists(5),
                MaybeNa::Exists(6),
                MaybeNa::Exists(8),
                MaybeNa::Exists(9),
                MaybeNa::Exists(10),
            ]),
            MaskedData::from_masked_vec(vec![
                MaybeNa::Exists(1),
                MaybeNa::Exists(2),
                MaybeNa::Na, // Bob's department isn't specified
                MaybeNa::Exists(1),
                MaybeNa::Exists(3),
                MaybeNa::Exists(4),
                MaybeNa::Exists(4),
            ]),
            MaskedData::from_masked_vec(vec![
                MaybeNa::Exists("Sally".into()),
                MaybeNa::Exists("Jamie".into()),
                MaybeNa::Exists("Bob".into()),
                MaybeNa::Exists("Cara".into()),
                MaybeNa::Exists("Louis".into()),
                MaybeNa::Exists("Louise".into()),
                MaybeNa::Exists("Ann".into()),
            ]),
        );
        let ds2 = sample_dept_table();
        let (dv1, dv2): (DataView, DataView) = (ds1.into(), ds2.into());
        println!("{}", dv1);
        println!("{}", dv2);
        let joined_dv: DataView = dv1.join(&dv2, Join::equal(
            JoinKind::Inner,
            "DeptId",
            "DeptId"
        )).expect("join failure").into();
        println!("{}", joined_dv);
        assert_eq!(joined_dv.nrows(), 6);
        assert_eq!(joined_dv.nfields(), 5);
        unsigned::assert_sorted_eq(&joined_dv, &"EmpId".into(),
            vec![0u64, 2, 6, 8, 9, 10]);
        unsigned::assert_sorted_eq(&joined_dv, &"DeptId.0".into(),
            vec![1u64, 2, 1, 3, 4, 4]);
        unsigned::assert_sorted_eq(&joined_dv, &"DeptId.1".into(),
            vec![1u64, 2, 1, 3, 4, 4]);
        text::assert_sorted_eq(&joined_dv, &"EmpName".into(),
            vec!["Sally", "Jamie", "Louis", "Louise", "Cara", "Ann"]
        );
        text::assert_sorted_eq(&joined_dv, &"DeptName".into(),
            vec!["Marketing", "Sales", "Marketing", "Manufacturing", "R&D", "R&D"]
        );
    }

    #[test]
    fn inner_nonequi_join() {
        // greater than
        let ds1 = sample_emp_table();
        let ds2 = dept_table(vec![1, 2], vec!["Marketing", "Sales"]);

        let (dv1, mut dv2): (DataView, DataView) = (ds1.into(), ds2.into());
        println!("~~\n>\n~~\n{}\n{}", dv1, dv2);
        // also test renaming
        dv2.rename("DeptId", "RightDeptId").expect("rename failed");
        let joined_dv: DataView = dv1.join(&dv2, Join::greater_than(
            JoinKind::Inner,
            "DeptId",
            "RightDeptId"
        )).expect("join failure").into();
        println!("{}", joined_dv);
        assert_eq!(joined_dv.nrows(), 7);
        assert_eq!(joined_dv.nfields(), 5);
        unsigned::assert_pred(&joined_dv, &"DeptId".into(),
            |&deptid| deptid >= 2);

        // greater than equal
        let ds1 = sample_emp_table();
        let ds2 = dept_table(vec![2], vec!["Sales"]);
        let (dv1, dv2): (DataView, DataView) = (ds1.into(), ds2.into());
        println!("~~\n>=\n~~\n+{}\n{}", dv1, dv2);
        let joined_dv: DataView = dv1.join(&dv2, Join::greater_than_equal(
            JoinKind::Inner,
            "DeptId",
            "DeptId"
        )).expect("join failure").into();
        println!("{}", joined_dv);
        assert_eq!(joined_dv.nrows(), 4);
        assert_eq!(joined_dv.nfields(), 5);
        unsigned::assert_pred(&joined_dv, &"DeptId.0".into(),
            |&deptid| deptid >= 2);

        // less than
        let ds1 = sample_emp_table();
        let ds2 = dept_table(vec![2], vec!["Sales"]);
        let (dv1, dv2): (DataView, DataView) = (ds1.into(), ds2.into());
        println!("~~\n<\n~~\n{}\n{}", dv1, dv2);
        let joined_dv: DataView = dv1.join(&dv2, Join::less_than(
            JoinKind::Inner,
            "DeptId",
            "DeptId"
        )).expect("join failure").into();
        println!("{}", joined_dv);
        assert_eq!(joined_dv.nrows(), 3);
        assert_eq!(joined_dv.nfields(), 5);
        unsigned::assert_pred(&joined_dv, &"DeptId.0".into(),
            |&deptid| deptid == 1);

        // less than equal
        let ds1 = sample_emp_table();
        let ds2 = dept_table(vec![2], vec!["Sales"]);
        let (dv1, dv2): (DataView, DataView) = (ds1.into(), ds2.into());
        println!("~~\n<=\n~~\n{}\n{}", dv1, dv2);
        let joined_dv: DataView = dv1.join(&dv2, Join::less_than_equal(
            JoinKind::Inner,
            "DeptId",
            "DeptId"
        )).expect("join failure").into();
        println!("{}", joined_dv);
        assert_eq!(joined_dv.nrows(), 4);
        assert_eq!(joined_dv.nfields(), 5);
        unsigned::assert_pred(&joined_dv, &"DeptId.0".into(),
            |&deptid| deptid <= 2);
    }*/
}
