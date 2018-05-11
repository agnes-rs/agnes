use apply::{DataIndex, FieldMapFn, FieldApplyTo};
use field::FieldIdent;
use error::*;

pub trait NumNa {
    fn num_na(&self, ident: &FieldIdent) -> Result<usize>;
}
impl<T> NumNa for T where T: FieldApplyTo {
    fn num_na(&self, ident: &FieldIdent) -> Result<usize> {
        self.field_apply_to(&mut NumNaFn {}, ident)
    }
}

macro_rules! impl_num_na_fn {
    ($name:tt, $ty:ty) => {
        fn $name<'a, T: DataIndex<$ty>>(&mut self, field: &T) -> usize {
            (0..field.len()).fold(0, |acc, idx| {
                acc + if field.get_data(idx).unwrap().is_na() { 1 } else { 0 }
            })
        }
    }
}
pub struct NumNaFn {}
impl FieldMapFn for NumNaFn {
    type Output = usize;
    impl_num_na_fn!(apply_unsigned, u64);
    impl_num_na_fn!(apply_signed,   i64);
    impl_num_na_fn!(apply_text,     String);
    impl_num_na_fn!(apply_boolean,  bool);
    impl_num_na_fn!(apply_float,    f64);
}


#[cfg(test)]
mod tests {
    use super::*;
    use view::DataView;
    use store::DataStore;
    use masked::{MaskedData, MaybeNa};

    #[test]
    fn num_na() {
        let dv: DataView = DataStore::with_data(
            None, None, None, None, vec![("Foo", MaskedData::from_masked_vec(vec![
                MaybeNa::Exists(0.0),
                MaybeNa::Exists(-5.0),
                MaybeNa::Na,
                MaybeNa::Na,
                MaybeNa::Exists(-3.0)
            ]))]
        ).into();
        println!("{}", dv);
        assert_eq!(dv.num_na(&"Foo".into()).unwrap(), 2);
    }
}
