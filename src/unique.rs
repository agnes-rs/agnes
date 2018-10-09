/*!
`DataView` methods and macros for finding unique values in a field.
*/

use std::collections::HashSet;
use std::hash::Hash;

use data_types::*;
use error::*;
use select::Field;
use access::DataIndex;
use view::DataView;
use field::FieldIdent;

impl<'a, DTypes> DataView<DTypes>
    where DTypes: DTypeList
{
    /// Returns a `Vec` of indices that point to the set of unique values of the specified
    /// identifier.
    ///
    /// Fails if identifier is not found in the `DataView` or the incorrect type `T` is specified.
    pub fn unique_indices<T, I>(&self, ident: I) -> Result<Vec<usize>>
        where DTypes::Storage: MaxLen<DTypes> + TypeSelector<DTypes, T>,
              T: 'static + DataType<DTypes> + Hash + Eq,
              I: Into<FieldIdent>
    {
        let mut indices = vec![];
        let field = self.field::<T, _>(ident)?;
        let mut set = HashSet::new();
        for i in 0..field.len() {
            let datum = field.get_datum(i).unwrap();
            if !set.contains(&datum) {
                set.insert(datum);
                indices.push(i);
            }
        }
        Ok(indices)
    }

    /// Returns a newly constructed `DataView` of the unique values of the specified identifier for
    /// this `DataView`.
    ///
    /// Fails if the identifier is not found in the `DataView` or the incorrect type `T` is
    /// specified.
    pub fn unique<T, I>(&self, ident: I) -> Result<DataView<DTypes>>
        where DTypes::Storage: MaxLen<DTypes> + TypeSelector<DTypes, T>,
              T: 'static + DataType<DTypes> + Hash + Eq,
              I: Into<FieldIdent>
    {
        let ident = ident.into();
        let permutation = self.unique_indices::<T, _>(ident.clone())?;
        let mut subview = self.v(ident);
        for frame in &mut subview.frames {
            frame.update_permutation(&permutation);
        }

        Ok(subview)
    }
}


#[macro_export]
macro_rules! composite_unique_indices {
    ($dv:expr, $([$id:expr]($ty:ty)),*$(,)*) => {{
        use std::collections::HashSet;
        use access::DataIndex;
        use select::{Selection, Field};
        use data_types::{Nil, Append};

        let dv = &$dv;
        let get_fields = || -> $crate::error::Result<_> {
            Ok(Nil $(.append(dv.field::<$ty, _>($id)?))*)
        };
        get_fields().map(|fields| {
            let mut indices = vec![];
            let mut set = HashSet::new();
            for i in 0..dv.nrows() {
                // FIXME: call to `cloned` method doesn't seem like it should be needed here,
                // but I ran into some lifetime issues I haven't figured out
                let record = map![
                    fields,
                    $([
                        |field: &Selection<_, _, $ty>| field.get_datum(i).unwrap().cloned()
                    ])*
                ];
                if !set.contains(&record) {
                    set.insert(record);
                    indices.push(i);
                }
            }
            indices
        })
    }};
}

#[macro_export]
macro_rules! composite_unique {
    ($dv:expr, $([$id:expr]($ty:ty)),*$(,)*) => {{
        composite_unique_indices![$dv, $([$id]($ty)),*].map(|indices| {
            let mut subview = $dv.v(vec![$($id),*]);
            for frame in &mut subview.frames {
                frame.update_permutation(&indices);
            }
            subview
        })
    }};
}

#[cfg(test)]
mod tests {
    use field::Value;
    use data_types::standard::*;
    use test_utils::*;

    #[test]
    fn unique() {
        let dv: DataView = DataStore::empty().with_data_vec::<u64, _, _>("Foo", vec![
                Value::Exists(0),
                Value::Exists(5),
                Value::Exists(5),
                Value::Exists(0),
                Value::Exists(3)
        ]).unwrap().into();
        let dv_unique_indices = dv.unique_indices::<u64, _>("Foo").unwrap();
        assert_eq!(dv_unique_indices, vec![0, 1, 4]);
        let dv_unique = dv.unique::<u64, _>("Foo").unwrap();
        unsigned::assert_dv_eq_vec(&dv_unique, &"Foo".into(),
            vec![0u64, 5, 3]
        );
    }

    #[test]
    fn composite_unique() {
        let dv = sample_merged_emp_table();
        let dv_unique_indices = composite_unique_indices![
            dv,
            ["DeptId"](u64),
            ["DidTraining"](bool),
        ].unwrap();
        // the only repeat is index 3
        assert_eq!(dv_unique_indices, vec![0, 1, 2, 4, 5, 6]);

        let dv_unique = composite_unique![
            dv,
            ["DeptId"](u64),
            ["DidTraining"](bool),
        ].unwrap();
        unsigned::assert_dv_eq_vec(&dv_unique, &"DeptId".into(),
            vec![1u64, 2, 1, 3, 4, 4]
        );
        boolean::assert_dv_eq_vec(&dv_unique, &"DidTraining".into(),
            vec![false, false, true, true, false, true]
        );
    }
}
