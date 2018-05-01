use field::FieldIdent;
use view::DataView;
use apply::{Map, Apply, MapFn};
use error::*;
use field::DataType;
use masked::MaybeNa;


/// Trait implemented by data structures that represent a single column / vector / field of data.
pub trait DataIndex<T: DataType> {
    /// Returns the data (possibly NA) at the specified index, if it exists.
    fn get_data(&self, idx: usize) -> Result<MaybeNa<&T>>;
    /// Returns the length of this data field.
    fn len(&self) -> usize;
}

impl<'a, T: DataType> DataIndex<T> for Vec<MaybeNa<&'a T>> {
    fn get_data(&self, idx: usize) -> Result<MaybeNa<&T>> {
        Ok(self[idx].clone())
    }
    fn len(&self) -> usize {
        self.len()
    }
}
impl<T: DataType> DataIndex<T> for Vec<T> {
    fn get_data(&self, idx: usize) -> Result<MaybeNa<&T>> {
        Ok(MaybeNa::Exists(&self[idx]))
    }
    fn len(&self) -> usize {
        self.len()
    }
}

// pub trait FieldDataIndex<T: DataType> {
//     fn get_field_data(&self, ident: &FieldIdent, idx: usize) -> Result<MaybeNa<&T>>;
//     fn field_len(&self, ident: &FieldIdent) -> usize;
// }

#[derive(Debug, Clone)]
pub struct Selection<'a> {
    data: &'a DataView,
    ident: FieldIdent,
}
// impl<'a, T: DataType, D: FieldDataIndex<T>> DataIndex<T> for Selection<'a, D>
// {
//     fn get_data(&self, idx: usize) -> Result<MaybeNa<&T>> {
//         self.data.get_field_data(&self.ident, idx)
//     }
//     fn len(&self) -> usize {
//         self.data.field_len(&self.ident)
//     }
// }

impl<'a> Apply for Selection<'a> {
    fn apply<F: MapFn>(&self, f: &mut F) -> Result<Vec<F::Output>> {
        self.data.apply(f, &self.ident)
    }
}

// impl<'a, D> Apply<NilSelector> for Selection<'a, D>
//     where for<'b> D: Apply<FieldSelector<'b>>
// {
//     fn apply<F: MapFn>(&self, f: &mut F, select: &NilSelector) -> Result<F::Output> {
//         self.data.apply(f, &FieldSelector(&self.ident))
//     }
// }
// impl<'a, S: Selector, T: DataType, D: 'a + DataIndex<T>> ApplyToElem<S> for Selection<'a, T, D, S> {
//     fn apply_to_elem<F: MapFn>(&self, f: F, select: &S) -> Result<F::Output> {
//         self.data.apply_to_elem(self.f, &self.selector)
//     }
// }
impl<'a> Selection<'a> {
    pub fn new<I: Into<FieldIdent>>(data: &'a DataView, ident: I) -> Selection<'a> {
        Selection {
            data,
            ident: ident.into()
        }
    }
    pub fn map<F: MapFn>(&self, f: F) -> Map<Self, F> {
        Map::new(self, f, None)
    }
}

// pub trait Select {
//     fn select<'a>(&'a self, selector: FieldSelector) -> Selection<'a, Self>;
// }
// impl<D> Select for D {
//     fn select<'a>(&'a self, selector: FieldSelector<'a>) -> Selection<'a, Self> {
//         Selection {
//             data: self,
//             selector: selector
//         }
//     }
// }

/// Data selector for the `ApplyToElem` and `ApplyToField` methods.
pub trait Selector: Clone {
    /// The type of the selector (the information used to specify what the `FieldFn` or `MapFn`
    /// operates upon).
    type IndexType;
    /// Returns the field / element selector details.
    fn index(&self) -> Self::IndexType;
}
/// A data selector unsing only a data index. Used to select a specific element among a
/// single column / field / vector for use with an `MapFn`.
#[derive(Debug, Clone)]
pub struct IndexSelector(pub usize);
impl Selector for IndexSelector {
    type IndexType = usize;
    fn index(&self) -> usize { self.0 }
}
/// A data selector using both a data field identifier and the data index. Used to select a
/// specific element in a two-dimensional data structs (with both fields and elements) along with
/// a `FieldFn`.
#[derive(Debug, Clone)]
pub struct FieldIndexSelector<'a>(pub &'a FieldIdent, pub usize);
impl<'a> Selector for FieldIndexSelector<'a> {
    type IndexType = (&'a FieldIdent, usize);
    fn index(&self) -> (&'a FieldIdent, usize) { (self.0, self.1) }
}
/// A data selector using only a field identifier. Used to select a specific field to be passed to
/// `FieldFn`.
#[derive(Debug, Clone)]
pub struct FieldSelector<'a>(pub &'a FieldIdent);
impl<'a> Selector for FieldSelector<'a> {
    type IndexType = (&'a FieldIdent);
    fn index(&self) -> (&'a FieldIdent) { (self.0) }
}
/// A data selector with no data. Used to select an entire field with `FieldFn` when a data
/// structure only has a single field's data.
#[derive(Debug, Clone)]
pub struct NilSelector;
impl Selector for NilSelector {
    type IndexType = ();
    fn index(&self) -> () {}
}
