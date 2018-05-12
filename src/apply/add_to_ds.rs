use store::{AddData, DataStore};
use field::FieldIdent;
use masked::MaybeNa;
use apply::MapFn;

/// `MapFn` for adding data to an existing `DataStore`.
map_fn![
    pub AddToDsFn<('a)> {
        type Output = ();
        pub ds: &'a mut DataStore,
        pub ident: FieldIdent,
    }
    fn all(self, value) {
        self.ds.add(self.ident.clone(), value.cloned())
    }
];
