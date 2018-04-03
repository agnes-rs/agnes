use std::cmp::Ordering;
use std::iter::Peekable;
use std::slice::Iter;
use std::rc::Rc;

use indexmap::IndexMap;

use field::TypedFieldIdent;
use masked::{MaskedData, FieldData};
use view::{DataView, ViewField};
use store::DataStore;
use error::*;

#[derive(Debug, Clone)]
pub struct Join {
    pub kind: JoinKind,
    pub predicate: Predicate,
    pub(crate) left_field: ViewField,
    pub(crate) right_field: ViewField,
}
impl Join {
    pub fn new(kind: JoinKind, predicate: Predicate, left_field: ViewField,
        right_field: ViewField) -> Join
    {
        Join {
            kind,
            predicate,
            left_field,
            right_field
        }
    }

    pub fn equal(kind: JoinKind, left_field: ViewField, right_field: ViewField) -> Join {
        Join {
            kind,
            predicate: Predicate::Equal,
            left_field,
            right_field,
        }
    }
    pub fn less_than(kind: JoinKind, left_field: ViewField, right_field: ViewField) -> Join {
        Join {
            kind,
            predicate: Predicate::LessThan,
            left_field,
            right_field,
        }
    }
    pub fn less_than_equal(kind: JoinKind, left_field: ViewField, right_field: ViewField)
        -> Join
    {
        Join {
            kind,
            predicate: Predicate::LessThanEqual,
            left_field,
            right_field,
        }
    }
    pub fn greater_than(kind: JoinKind, left_field: ViewField, right_field: ViewField) -> Join {
        Join {
            kind,
            predicate: Predicate::GreaterThan,
            left_field,
            right_field,
        }
    }
    pub fn greater_than_equal(kind: JoinKind, left_field: ViewField, right_field: ViewField)
        -> Join
    {
        Join {
            kind,
            predicate: Predicate::GreaterThanEqual,
            left_field,
            right_field,
        }
    }


}

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
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Predicate {
    Equal,
    LessThan,
    LessThanEqual,
    GreaterThan,
    GreaterThanEqual,
}

pub fn hash_join(_left: &DataView, _right: &DataView, join: Join) -> Result<DataStore> {
    assert_eq!(join.predicate, Predicate::Equal, "hash_join only valid for equijoins");

    unimplemented!();
}

pub fn sort_merge_join(left: &DataView, right: &DataView, join: Join) -> Result<DataStore> {
    // get the data for this field
    let left_key_data = left.get_field_data(&join.left_field)
        .ok_or(AgnesError::FieldNotFound(join.left_field.rident.ident))?;
    let right_key_data = right.get_field_data(&join.right_field)
        .ok_or(AgnesError::FieldNotFound(join.right_field.rident.ident))?;
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
    let left_perm_iter = left_perm.iter().peekable();
    let right_perm_iter = right_perm.iter().peekable();
    let merge_indices = merge(left_perm_iter, right_perm_iter, left_key_data, right_key_data);

    // compute merged store list and field list for the new datastore
    // compute the field list for the new datastore
    let (new_stores, other_store_indices) = compute_merged_stores(left, right);
    let new_fields = compute_merged_field_list(left, right, &other_store_indices)?;

    // create new datastore with fields of both left and right
    let mut ds = DataStore::with_fields(
        new_fields.values()
        .map(|&ref view_field| {
            let ident = view_field.rident.to_renamed_field_ident();
            let field_type = new_stores[view_field.store_idx].get_field_type(&ident)
                .expect("compute_merged_stores/field_list failed");
            TypedFieldIdent {
                ident,
                ty: field_type,
            }
        })
        .collect::<Vec<_>>());

    for (left_idx, right_idx) in merge_indices {
        let add_value = |ds: &mut DataStore, data: &DataView, field: &ViewField, idx| {
            // col.get(i).unwrap() should be safe: indices originally generated from view nrows
            let renfield = field.rident.to_renamed_field_ident();
            match data.get_field_data(field).unwrap() {
                FieldData::Unsigned(col) => ds.add_unsigned(renfield,
                    col.get(idx).unwrap().cloned()),
                FieldData::Signed(col) => ds.add_signed(renfield,
                    col.get(idx).unwrap().cloned()),
                FieldData::Text(col) => ds.add_text(renfield,
                    col.get(idx).unwrap().cloned()),
                FieldData::Boolean(col) => ds.add_boolean(renfield,
                    col.get(idx).unwrap().cloned()),
                FieldData::Float(col) => ds.add_float(renfield,
                    col.get(idx).unwrap().cloned()),
            }
        };
        for left_field in left.fields.values() {
            add_value(&mut ds, left, left_field, left_idx);
        }
        for right_field in right.fields.values() {
            add_value(&mut ds, right, right_field, right_idx);
        }
    }

    Ok(ds)
}

fn merge<'a>(
    left_perm_iter: Peekable<Iter<'a, usize>>,
    right_perm_iter: Peekable<Iter<'a, usize>>,
    left_key_data: FieldData<'a>,
    right_key_data: FieldData<'a>
) -> Vec<(usize, usize)>
{
    match (left_key_data, right_key_data) {
        (FieldData::Unsigned(left_data), FieldData::Unsigned(right_data)) =>
            merge_masked_data(left_perm_iter, right_perm_iter, left_data, right_data),
        (FieldData::Signed(left_data), FieldData::Signed(right_data)) =>
            merge_masked_data(left_perm_iter, right_perm_iter, left_data, right_data),
        (FieldData::Text(left_data), FieldData::Text(right_data)) =>
            merge_masked_data(left_perm_iter, right_perm_iter, left_data, right_data),
        (FieldData::Boolean(left_data), FieldData::Boolean(right_data)) =>
            merge_masked_data(left_perm_iter, right_perm_iter, left_data, right_data),
        (FieldData::Float(left_data), FieldData::Float(right_data)) =>
            merge_masked_data(left_perm_iter, right_perm_iter, left_data, right_data),
        _ => panic!("attempt to merge non-identical field types")
    }

}

fn merge_masked_data<'a, T: PartialOrd>(
    mut left_perm_iter: Peekable<Iter<'a, usize>>,
    mut right_perm_iter: Peekable<Iter<'a, usize>>,
    left_key_data: &'a MaskedData<T>,
    right_key_data: &'a MaskedData<T>
) -> Vec<(usize, usize)>
{
    debug_assert!(left_perm_iter.peek().is_some() && right_perm_iter.peek().is_some());
    // struct to keep track of current position, and value at that position
    struct CurPosition<U> {
        value: U,
        idx: usize
    }
    // advance position to next index in permutation iterator
    let advance = |key_data: &'a MaskedData<T>, perm_iter: &mut Peekable<Iter<'a, usize>>| {
        debug_assert!(perm_iter.peek().is_some());
        let idx = *perm_iter.next().unwrap();
        // permutation index is always within range, so unwrap is safe
        CurPosition { idx, value: key_data.get(idx).unwrap() }
    };
    // we know left_perm and right_perm both are non-empty, so there is at least one value and
    // advance is safe
    let mut left_pos = advance(left_key_data, &mut left_perm_iter);
    let mut right_pos = advance(right_key_data, &mut right_perm_iter);

    let mut merge_indices = vec![];
    while left_perm_iter.peek().is_some() && right_perm_iter.peek().is_some() {
        if left_pos.value == right_pos.value {
            merge_indices.push((left_pos.idx, right_pos.idx));
            left_pos = advance(left_key_data, &mut left_perm_iter);
            right_pos = advance(right_key_data, &mut right_perm_iter);
        } else if left_pos.value < right_pos.value {
            left_pos = advance(left_key_data, &mut left_perm_iter);
        } else {
            // left_pos.value > right_pos.value
            right_pos = advance(right_key_data, &mut right_perm_iter);
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

pub(crate) fn compute_merged_field_list(left: &DataView, right: &DataView,
    right_store_mapping: &Vec<usize>) -> Result<IndexMap<String, ViewField>>
{
    // build new fields vector, updating the store indices in the ViewFields copied
    // from the 'right' fields list
    let mut new_fields = left.fields.clone();
    for (right_fieldname, right_field) in &right.fields {
        if new_fields.contains_key(right_fieldname) {
            return Err(AgnesError::FieldCollision(right_fieldname.clone()));
        }
        new_fields.insert(right_fieldname.clone(), ViewField {
            rident: right_field.rident.clone(),
            store_idx: right_store_mapping[right_field.store_idx],
        });
    }
    Ok(new_fields)
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
    use super::SortOrder;
    use masked::{MaybeNa, MaskedData};

    #[test]
    fn sort_order_no_na() {
        let masked_data = MaskedData::from_vec(vec![2u64, 5, 3, 1, 8]);
        let sort_order = masked_data.sort_order();
        assert_eq!(sort_order, vec![3, 0, 2, 1, 4]);

        let masked_data = MaskedData::from_vec(vec![2.0, 5.4, 3.1, 1.1, 8.2]);
        let sort_order = masked_data.sort_order();
        assert_eq!(sort_order, vec![3, 0, 2, 1, 4]);

        let masked_data = MaskedData::from_vec(vec![2.0, ::std::f64::NAN, 3.1, 1.1, 8.2]);
        let sort_order = masked_data.sort_order();
        assert_eq!(sort_order, vec![1, 3, 0, 2, 4]);

        let masked_data = MaskedData::from_vec(vec![2.0, ::std::f64::NAN, 3.1,
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
}
