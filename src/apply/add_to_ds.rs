use store::{AddData, DataStore};
use field::FieldIdent;
use field::Value;
use apply::mapfn::MapFn;

map_fn![
    /// `MapFn` for adding data to an existing `DataStore`.
    pub AddToDsFn<('a)> {
        type Output = ();
        /// Target `DataStore` to add to.
        pub ds: &'a mut DataStore,
        /// Target field identifier within store to add to.
        pub ident: FieldIdent,
    }
    fn all(self, value) {
        self.ds.add(self.ident.clone(), value.cloned())
    }
];
