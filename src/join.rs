/*!
`DataView` join structs and implementations.
*/

use std::cmp::Ordering;
use std::rc::Rc;

use indexmap::IndexMap;

use field::{RFieldIdent, TypedFieldIdent};
use masked::{MaskedData, FieldData, MaybeNa};
use view::{DataView, ViewField};
use store::DataStore;
use error::*;

/// Join information used to describe the type of join being used.
#[derive(Debug, Clone)]
pub struct Join {
    /// Join kind: Inner, Outer, or Cross
    pub kind: JoinKind,
    /// Join predicate: equijoin, inequality join
    pub predicate: Predicate,
    pub(crate) left_field: String,
    pub(crate) right_field: String,
}
impl Join {
    /// Create a new `Join` over the specified fields.
    pub fn new<L: Into<String>, R: Into<String>>(kind: JoinKind, predicate: Predicate,
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
    pub fn equal<L: Into<String>, R: Into<String>>(kind: JoinKind, left_field: L, right_field: R)
        -> Join
    {
        Join {
            kind,
            predicate: Predicate::Equal,
            left_field: left_field.into(),
            right_field: right_field.into(),
        }
    }
    /// Helper function to create a new `Join` with an 'Less Than' predicate.
    pub fn less_than<L: Into<String>, R: Into<String>>(kind: JoinKind, left_field: L,
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
    pub fn less_than_equal<L: Into<String>, R: Into<String>>(kind: JoinKind, left_field: L,
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
    pub fn greater_than<L: Into<String>, R: Into<String>>(kind: JoinKind, left_field: L,
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
    pub fn greater_than_equal<L: Into<String>, R: Into<String>>(kind: JoinKind, left_field: L,
        right_field: R) -> Join
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
    fn apply<T: PartialOrd>(&self, left: &MaybeNa<T>, right: &MaybeNa<T>) -> PredResults {
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
    // get the data for this field
    let left_key_data = left.get_field_data(&join.left_field)
        .ok_or(AgnesError::FieldNotFound(join.left_field.clone().into()))?;
    let right_key_data = right.get_field_data(&join.right_field)
        .ok_or(AgnesError::FieldNotFound(join.right_field.clone().into()))?;
    if left_key_data.get_field_type() != right_key_data.get_field_type() {
        return Err(AgnesError::TypeMismatch("unable to join on fields of different types".into()));
    }
    if left_key_data.is_empty() || right_key_data.is_empty() {
        return Ok(DataStore::empty());
    }

    // sort (or rather, get the sorted order for field being merged)
    let left_perm = left_key_data.sort_order();
    let right_perm = right_key_data.sort_order();

    // find the join indices
    let merge_indices = merge(left_perm, right_perm, left_key_data, right_key_data,
        join.predicate);

    // compute merged store list and field list for the new datastore
    // compute the field list for the new datastore
    let (new_stores, other_store_indices) = compute_merged_stores(left, right);
    let new_fields = compute_merged_field_list(left, right, &other_store_indices, &join)?;

    // create new datastore with fields of both left and right
    let mut new_field_idents = vec![];
    let mut ds = DataStore::with_fields(
        new_fields.values()
        .map(|&ref view_field| {
            let new_ident = view_field.rident.to_renamed_field_ident();
            new_field_idents.push(new_ident.clone());
            let field_type = new_stores[view_field.store_idx]
                .get_field_type(&view_field.rident.ident)
                .expect("compute_merged_stores/field_list failed");
            TypedFieldIdent {
                ident: new_ident,
                ty: field_type,
            }
        })
        .collect::<Vec<_>>());

    for (left_idx, right_idx) in merge_indices {
        let add_value = |ds: &mut DataStore, data: &DataView, field: &ViewField, idx, new_field| {
            // col.get(idx).unwrap() should be safe: indices originally generated from view nrows
            match data.get_viewfield_data(field).unwrap() {
                FieldData::Unsigned(col) => ds.add_unsigned(new_field,
                    col.get(idx).unwrap().cloned()),
                FieldData::Signed(col) => ds.add_signed(new_field,
                    col.get(idx).unwrap().cloned()),
                FieldData::Text(col) => ds.add_text(new_field,
                    col.get(idx).unwrap().cloned()),
                FieldData::Boolean(col) => ds.add_boolean(new_field,
                    col.get(idx).unwrap().cloned()),
                FieldData::Float(col) => ds.add_float(new_field,
                    col.get(idx).unwrap().cloned()),
            }
        };
        let mut field_idx = 0;
        for left_field in left.fields.values() {
            add_value(&mut ds, left, left_field, left_idx, new_field_idents[field_idx].clone());
            field_idx += 1;
        }
        for right_field in right.fields.values() {
            add_value(&mut ds, right, right_field, right_idx, new_field_idents[field_idx].clone());
            field_idx += 1;
        }
    }

    Ok(ds)
}

fn merge<'a>(
    left_perm_iter: Vec<usize>,
    right_perm_iter: Vec<usize>,
    left_key_data: FieldData<'a>,
    right_key_data: FieldData<'a>,
    predicate: Predicate
) -> Vec<(usize, usize)>
{
    match (left_key_data, right_key_data) {
        (FieldData::Unsigned(left_data), FieldData::Unsigned(right_data)) =>
            merge_masked_data(left_perm_iter, right_perm_iter, left_data, right_data, predicate),
        (FieldData::Signed(left_data), FieldData::Signed(right_data)) =>
            merge_masked_data(left_perm_iter, right_perm_iter, left_data, right_data, predicate),
        (FieldData::Text(left_data), FieldData::Text(right_data)) =>
            merge_masked_data(left_perm_iter, right_perm_iter, left_data, right_data, predicate),
        (FieldData::Boolean(left_data), FieldData::Boolean(right_data)) =>
            merge_masked_data(left_perm_iter, right_perm_iter, left_data, right_data, predicate),
        (FieldData::Float(left_data), FieldData::Float(right_data)) =>
            merge_masked_data(left_perm_iter, right_perm_iter, left_data, right_data, predicate),
        _ => panic!("attempt to merge non-identical field types")
    }

}
fn merge_masked_data<'a, T: PartialOrd>(
    left_perm: Vec<usize>,
    right_perm: Vec<usize>,
    left_key_data: &'a MaskedData<T>,
    right_key_data: &'a MaskedData<T>,
    predicate: Predicate,
) -> Vec<(usize, usize)>
{
    debug_assert!(!left_perm.is_empty() && !right_perm.is_empty());
    // NOTE: actual_idx = perm[sorted_idx]
    // NOTE: value = key_data.get(actual_idx).unwrap();
    let lval = |sorted_idx| left_key_data.get(left_perm[sorted_idx]).unwrap();
    let rval = |sorted_idx| right_key_data.get(right_perm[sorted_idx]).unwrap();

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

pub(crate) fn compute_merged_stores(left: &DataView, right: &DataView)
    -> (Vec<Rc<DataStore>>, Vec<usize>)
{
    // new store vector is combination, without repetition, of existing store vectors. also
    // keep track of the store indices (for store_idx) of the 'right' fields
    let mut new_stores = left.stores.clone();
    let mut right_store_indices = vec![];
    for right_store in &right.stores {
        match new_stores.iter().enumerate().find(|&(_, store)| Rc::ptr_eq(store, right_store)) {
            Some((idx, _)) => {
                right_store_indices.push(idx);
            },
            None => {
                right_store_indices.push(new_stores.len());
                new_stores.push(right_store.clone());
            }
        }
    }
    (new_stores, right_store_indices)
}

pub(crate) fn compute_merged_field_list<'a, T: Into<Option<&'a Join>>>(left: &DataView,
    right: &DataView, right_store_mapping: &Vec<usize>, join: T)
    -> Result<IndexMap<String, ViewField>>
{
    // build new fields vector, updating the store indices in the ViewFields copied
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
                    new_fields.insert(new_left_field_name, left_key_field);
                    new_fields.swap_remove(&join.left_field);

                    let new_right_field_name = format!("{}.1", join.right_field);
                    new_fields.insert(new_right_field_name.clone(), ViewField {
                        rident: RFieldIdent {
                            ident: right_field.rident.ident.clone(),
                            rename: Some(new_right_field_name),
                        },
                        store_idx: right_store_mapping[right_field.store_idx]
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
            store_idx: right_store_mapping[right_field.store_idx],
        });
    }
    if field_coll.is_empty() {
        Ok(new_fields)
    } else {
        Err(AgnesError::FieldCollision(field_coll))
    }
}

type SortedOrder = Vec<usize>;
trait SortOrder {
    fn sort_order(&self) -> SortedOrder;
}
// f64 ordering is (arbitrarily) going to be:
// NA values, followed by NAN values, followed by everything else ascending
impl SortOrder for MaskedData<f64> {
    fn sort_order(&self) -> SortedOrder {
        let mut order = (0..self.len()).collect::<Vec<_>>();
        order.sort_unstable_by(|&a, &b| {
            // a, b are always in range, so unwraps are safe
            let (vala, valb) = (self.get(a).unwrap(), self.get(b).unwrap());
            vala.partial_cmp(&valb).unwrap_or_else(|| {
                // partial_cmp doesn't fail for MaybeNa::NA, unwraps safe
                let (vala, valb) = (vala.unwrap(), valb.unwrap());
                if vala.is_nan() && !valb.is_nan() {
                    Ordering::Less
                } else {
                    // since partial_cmp only fails for NAN, then !vala.is_nan() && valb.is_nan()
                    Ordering::Greater
                }
            })
        });
        order
    }
}

macro_rules! impl_masked_sort {
    ($($t:ty)*) => {$(
        // ordering is (arbitrarily) going to be:
        // NA values, followed by everything else ascending
        impl SortOrder for MaskedData<$t> {
            fn sort_order(&self) -> SortedOrder {
                let mut order = (0..self.len()).collect::<Vec<_>>();
                order.sort_unstable_by(|&a, &b| {
                    // a, b are always in range, so unwraps are safe
                    self.get(a).unwrap().cmp(&self.get(b).unwrap())
                });
                order
            }
        }
    )*}
}
impl_masked_sort![u64 i64 String bool];

impl<'a> SortOrder for FieldData<'a> {
    fn sort_order(&self) -> SortedOrder {
        match *self {
            FieldData::Unsigned(v)  => v.sort_order(),
            FieldData::Signed(v)    => v.sort_order(),
            FieldData::Text(v)      => v.sort_order(),
            FieldData::Boolean(v)   => v.sort_order(),
            FieldData::Float(v)     => v.sort_order(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use masked::{MaybeNa, MaskedData};
    use store::DataStore;

    #[test]
    fn sort_order_no_na() {
        let masked_data: MaskedData<u64> = MaskedData::from_vec(vec![2u64, 5, 3, 1, 8]);
        let sort_order = masked_data.sort_order();
        assert_eq!(sort_order, vec![3, 0, 2, 1, 4]);

        let masked_data: MaskedData<f64> = MaskedData::from_vec(vec![2.0, 5.4, 3.1, 1.1, 8.2]);
        let sort_order = masked_data.sort_order();
        assert_eq!(sort_order, vec![3, 0, 2, 1, 4]);

        let masked_data: MaskedData<f64> =
            MaskedData::from_vec(vec![2.0, ::std::f64::NAN, 3.1, 1.1, 8.2]);
        let sort_order = masked_data.sort_order();
        assert_eq!(sort_order, vec![1, 3, 0, 2, 4]);

        let masked_data: MaskedData<f64> = MaskedData::from_vec(vec![2.0, ::std::f64::NAN, 3.1,
            ::std::f64::INFINITY, 8.2]);
        let sort_order = masked_data.sort_order();
        assert_eq!(sort_order, vec![1, 0, 2, 4, 3]);
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
        let sort_order = masked_data.sort_order();
        assert_eq!(sort_order, vec![2, 3, 0, 1, 4]);

        let masked_data = MaskedData::from_masked_vec(vec![
            MaybeNa::Exists(2.1),
            MaybeNa::Exists(5.5),
            MaybeNa::Na,
            MaybeNa::Exists(1.1),
            MaybeNa::Exists(8.2930)
        ]);
        let sort_order = masked_data.sort_order();
        assert_eq!(sort_order, vec![2, 3, 0, 1, 4]);

        let masked_data = MaskedData::from_masked_vec(vec![
            MaybeNa::Exists(2.1),
            MaybeNa::Exists(::std::f64::NAN),
            MaybeNa::Na,
            MaybeNa::Exists(1.1),
            MaybeNa::Exists(8.2930)
        ]);
        let sort_order = masked_data.sort_order();
        assert_eq!(sort_order, vec![2, 1, 3, 0, 4]);

        let masked_data = MaskedData::from_masked_vec(vec![
            MaybeNa::Exists(2.1),
            MaybeNa::Exists(::std::f64::NAN),
            MaybeNa::Na,
            MaybeNa::Exists(::std::f64::INFINITY),
            MaybeNa::Exists(8.2930)
        ]);
        let sort_order = masked_data.sort_order();
        assert_eq!(sort_order, vec![2, 1, 0, 4, 3]);
    }

    fn sample_emp_table() -> DataStore {
        emp_table(vec![0u64, 2, 5, 6, 8, 9, 10], vec![1u64, 2, 1, 1, 3, 4, 4],
            vec!["Sally", "Jamie", "Bob", "Cara", "Louis", "Louise", "Ann"])
    }
    fn emp_table(empids: Vec<u64>, deptids: Vec<u64>, names: Vec<&str>) -> DataStore {
        emp_table_from_masked(empids.into(), deptids.into(), names.into())
    }
    fn emp_table_from_masked(empids: MaskedData<u64>, deptids: MaskedData<u64>,
        names: MaskedData<String>) -> DataStore
    {
        DataStore::with_data(
            // unsigned
            vec![
                ("EmpId", empids),
                ("DeptId", deptids)
            ],
            // signed
            None,
            // text
            vec![
                ("EmpName", names)
            ],
            // boolean
            None,
            // float
            None
        )
    }

    fn sample_dept_table() -> DataStore {
        dept_table(vec![1u64, 2, 3, 4], vec!["Marketing", "Sales", "Manufacturing", "R&D"])
    }
    fn dept_table(deptids: Vec<u64>, names: Vec<&str>) -> DataStore {
        dept_table_from_masked(deptids.into(), names.into())
    }
    fn dept_table_from_masked(deptids: MaskedData<u64>, names: MaskedData<String>) -> DataStore {
        DataStore::with_data(
            // unsigned
            vec![
                ("DeptId", deptids)
            ],
            // signed
            None,
            // text
            vec![
                ("DeptName", names)
            ],
            // boolean
            None,
            // float
            None
        )
    }

    macro_rules! impl_test_helpers {
        ($name:tt; $variant:path, $dtype:ty) => {
            mod $name {
                use super::{FieldData, MaybeNa};
                #[allow(dead_code)]
                pub fn assert_sorted_eq(left: FieldData, right: Vec<$dtype>) {
                    if let $variant(masked) = left {
                        let mut masked = masked.as_vec();
                        masked.sort();
                        let mut right = right.iter()
                            .map(|val| MaybeNa::Exists(val)).collect::<Vec<_>>();
                        right.sort();
                        for (lval, rval) in masked.iter().zip(right.iter()) {
                            assert_eq!(lval, rval);
                        }
                    } else {
                        panic!("$name::assert_sorted_eq called with incorrect type FieldData")
                    }
                }
                #[allow(dead_code)]
                pub fn assert_pred<F: Fn(&$dtype) -> bool>(left: FieldData, f: F) {
                    if let $variant(masked) = left {
                        for val in masked.as_vec().iter() {
                            match val {
                                &MaybeNa::Exists(&ref val) => {
                                    assert!(f(val), "predicate failed");
                                },
                                &MaybeNa::Na => {
                                    panic!("$name::assert_pred called with NA value");
                                }
                            }
                        };
                    } else {
                        panic!("$name::assert_pred called with incorrect type FieldData")
                    }
                }
            }
        }
    }
    impl_test_helpers!(unsigned; FieldData::Unsigned, u64);
    impl_test_helpers!(text;     FieldData::Text,     String);

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
        unsigned::assert_sorted_eq(joined_dv.get_field_data("EmpId").unwrap(),
            vec![0, 2, 5, 6, 8, 9, 10]);
        unsigned::assert_sorted_eq(joined_dv.get_field_data("DeptId.0").unwrap(),
            vec![1, 2, 1, 1, 3, 4, 4]);
        unsigned::assert_sorted_eq(joined_dv.get_field_data("DeptId.1").unwrap(),
            vec![1, 2, 1, 1, 3, 4, 4]);
        text::assert_sorted_eq(joined_dv.get_field_data("EmpName").unwrap(),
            vec!["Sally", "Jamie", "Bob", "Louis", "Louise", "Cara", "Ann"]
                .iter().map(|name| name.to_string()).collect());
        text::assert_sorted_eq(joined_dv.get_field_data("DeptName").unwrap(),
            vec!["Marketing", "Sales", "Marketing", "Marketing", "Manufacturing", "R&D", "R&D"]
                .iter().map(|name| name.to_string()).collect());
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
        unsigned::assert_sorted_eq(joined_dv.get_field_data("EmpId").unwrap(),
            vec![2, 8, 9, 10]);
        unsigned::assert_sorted_eq(joined_dv.get_field_data("DeptId.0").unwrap(),
            vec![2, 3, 4, 4]);
        unsigned::assert_sorted_eq(joined_dv.get_field_data("DeptId.1").unwrap(),
            vec![2, 3, 4, 4]);
        text::assert_sorted_eq(joined_dv.get_field_data("EmpName").unwrap(),
            vec!["Jamie", "Louis", "Louise", "Ann"]
                .iter().map(|name| name.to_string()).collect());
        text::assert_sorted_eq(joined_dv.get_field_data("DeptName").unwrap(),
            vec!["Sales", "Manufacturing", "R&D", "R&D"]
                .iter().map(|name| name.to_string()).collect());

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
        unsigned::assert_sorted_eq(joined_dv.get_field_data("EmpId").unwrap(),
            vec![0, 2, 6, 8, 9, 10]);
        unsigned::assert_sorted_eq(joined_dv.get_field_data("DeptId.0").unwrap(),
            vec![1, 2, 1, 3, 4, 4]);
        unsigned::assert_sorted_eq(joined_dv.get_field_data("DeptId.1").unwrap(),
            vec![1, 2, 1, 3, 4, 4]);
        text::assert_sorted_eq(joined_dv.get_field_data("EmpName").unwrap(),
            vec!["Sally", "Jamie", "Louis", "Louise", "Cara", "Ann"]
                .iter().map(|name| name.to_string()).collect());
        text::assert_sorted_eq(joined_dv.get_field_data("DeptName").unwrap(),
            vec!["Marketing", "Sales", "Marketing", "Manufacturing", "R&D", "R&D"]
                .iter().map(|name| name.to_string()).collect());
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
        unsigned::assert_pred(joined_dv.get_field_data("DeptId").unwrap(),
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
        unsigned::assert_pred(joined_dv.get_field_data("DeptId.0").unwrap(),
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
        unsigned::assert_pred(joined_dv.get_field_data("DeptId.0").unwrap(),
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
        unsigned::assert_pred(joined_dv.get_field_data("DeptId.0").unwrap(),
            |&deptid| deptid <= 2);
    }
}
