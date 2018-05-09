use store::{AddData, DataStore};
use field::FieldIdent;
use masked::MaybeNa;
use apply::MapFn;

/// `MapFn` for adding data to an existing `DataStore`.
pub struct AddToDs<'a> {
    /// Target `DataStore` to add to.
    pub ds: &'a mut DataStore,
    /// Target field identifier within store to add to.
    pub ident: FieldIdent
}
macro_rules! impl_add_to_ds {
    ($name:tt; $ty:ty) => {
        fn $name(&mut self, value: MaybeNa<&$ty>) {
            self.ds.add(self.ident.clone(), value.cloned())
        }
    }
}
impl<'a> MapFn for AddToDs<'a> {
    type Output = ();
    impl_add_to_ds!(apply_unsigned; u64);
    impl_add_to_ds!(apply_signed;   i64);
    impl_add_to_ds!(apply_text;     String);
    impl_add_to_ds!(apply_boolean;  bool);
    impl_add_to_ds!(apply_float;    f64);
}
