use field::FieldIdent;
use apply::{Map, Apply, ApplyTo, MapFn};
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
impl<T: DataType> DataIndex<T> for Vec<MaybeNa<T>> {
    fn get_data(&self, idx: usize) -> Result<MaybeNa<&T>> {
        Ok(self[idx].as_ref())
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

// pub trait TypedDataIndex {
//     fn get_data_index_enum(&self) -> DataIndexEnum;
//     // fn get_unsigned_data(&self, idx: usize) -> Result<MaybeNa<&u64>>;
//     // fn get_signed_data(&self, idx: usize) -> Result<MaybeNa<&i64>>;
//     // fn get_text_data(&self, idx: usize) -> Result<MaybeNa<&String>>;
//     // fn get_boolean_data(&self, idx: usize) -> Result<MaybeNa<&bool>>;
//     // fn get_float_data(&self, idx: usize) -> Result<MaybeNa<&f64>>;
//     // fn len(&self) -> usize;
// }
// impl<U> TypedDataIndex for U where U: DataIndex<u64> + DataIndex<i64> + DataIndex<String>
//     + DataIndex<bool> + DataIndex<f64>
// {
//     fn get_unsigned_data(&self, idx: usize) -> Result<MaybeNa<&u64>> {
//         DataIndex::<u64>::get_data(self, idx)
//     }
//     fn get_signed_data(&self, idx: usize) -> Result<MaybeNa<&i64>> {
//         DataIndex::<i64>::get_data(self, idx)
//     }
//     fn get_text_data(&self, idx: usize) -> Result<MaybeNa<&String>> {
//         DataIndex::<String>::get_data(self, idx)
//     }
//     fn get_boolean_data(&self, idx: usize) -> Result<MaybeNa<&bool>> {
//         DataIndex::<bool>::get_data(self, idx)
//     }
//     fn get_float_data(&self, idx: usize) -> Result<MaybeNa<&f64>> {
//         DataIndex::<f64>::get_data(self, idx)
//     }
//     fn len(&self) -> usize {
//         DataIndex::<u64>::len(self)
//     }
// }

pub enum OwnedOrRef<'a, T: 'a + DataType> {
    Owned(Box<DataIndex<T> + 'a>),
    Ref(&'a DataIndex<T>)
}
impl<'a, T: 'a + DataType> OwnedOrRef<'a, T> {
    pub fn as_ref(&'a self) -> &'a DataIndex<T> {
        match *self {
            OwnedOrRef::Owned(ref data) => data.as_ref(),
            OwnedOrRef::Ref(data) => data,
        }
    }
}
impl<'a, T: 'a + DataType> DataIndex<T> for OwnedOrRef<'a, T> {
    fn get_data(&self, idx: usize) -> Result<MaybeNa<&T>> {
        match *self {
            OwnedOrRef::Owned(ref data) => data.get_data(idx),
            OwnedOrRef::Ref(ref data) => data.get_data(idx),
        }
    }
    fn len(&self) -> usize {
        match *self {
            OwnedOrRef::Owned(ref data) => data.len(),
            OwnedOrRef::Ref(ref data) => data.len(),
        }
    }
}
pub enum ReduceDataIndex<'a> {
    Unsigned(OwnedOrRef<'a, u64>),
    Signed(OwnedOrRef<'a, i64>),
    Text(OwnedOrRef<'a, String>),
    Boolean(OwnedOrRef<'a, bool>),
    Float(OwnedOrRef<'a, f64>),
}

// pub trait FieldDataIndex<T: DataType> {
//     fn get_field_data(&self, ident: &FieldIdent, idx: usize) -> Result<MaybeNa<&T>>;
//     fn field_len(&self, ident: &FieldIdent) -> usize;
// }

#[derive(Debug, Clone)]
pub struct Selection<'a, 'b, D: 'a + ?Sized> {
    pub data: &'a D,
    pub ident: &'b FieldIdent,
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

impl<'a, 'b, D: 'a + ApplyTo> Apply for Selection<'a, 'b, D> {
    fn apply<F: MapFn>(&self, f: &mut F) -> Result<Vec<F::Output>> {
        self.data.apply_to(f, &self.ident)
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
impl<'a, 'b, D> Selection<'a, 'b, D> {
    pub fn new(data: &'a D, ident: &'b FieldIdent) -> Selection<'a, 'b, D> {
        Selection {
            data,
            ident: ident
        }
    }
}
impl<'a, 'b, D: ApplyTo> Selection<'a, 'b, D> {
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

pub trait Select {
    fn select<'a, 'b>(&'a self, ident: &'b FieldIdent)
       -> Selection<'a, 'b, Self>;
}

impl<T> Select for T {
    fn select<'a, 'b>(&'a self, ident: &'b FieldIdent)
        -> Selection<'a, 'b, Self>
    {
        Selection::new(self, ident)
    }
}

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
