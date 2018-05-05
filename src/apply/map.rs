use masked::{MaybeNa, IntoMaybeNa};
use error::*;
use field::{DataType, FieldIdent};
use view::DataView;
use store::{DataStore, AddDataVec};
use apply::{DataIndex, ReduceDataIndex};

/// Apply a `MapFn` (single-element mapping function) to this data structure
pub trait Apply {
    fn apply<F: MapFn>(&self, f: &mut F) -> Result<Vec<F::Output>>;
}

pub trait ApplyTo {
    fn apply_to<F: MapFn>(&self, f: &mut F, ident: &FieldIdent) -> Result<Vec<F::Output>>;
}
// impl<'a, T> ApplyTo for T where T: Apply {
//     fn apply_to<F: MapFn>(&self, f: &mut F, ident: &FieldIdent) -> Result<Vec<F::Output>> {
//         Selection { data: self, ident }.apply(f)
//     }
// }

pub trait ApplyToElem {
    fn apply_to_elem<F: MapFn>(&self, f: &mut F, ident: &FieldIdent, idx: usize)
        -> Result<F::Output>;
}

pub trait FieldApply {
    fn field_apply<F: FieldMapFn>(&self, f: &mut F) -> Result<F::Output>;
}
pub trait FieldApplyTo {
    fn field_apply_to<F: FieldMapFn>(&self, f: &mut F, ident: &FieldIdent) -> Result<F::Output>;
}
// impl<'a, T> FieldApplyTo for T where T: Apply {
//     fn field_apply_to<F: FieldMapFn>(&self, f: &mut F, ident: &FieldIdent) -> Result<F::Output> {
//         Selection { data: self, ident }.field_apply(f)
//     }
// }

#[derive(Debug, Clone)]
pub struct Map<'a, D: 'a + Apply, F: MapFn> {
    data: &'a D,
    f: F,
    name: String,
}
impl<'a, D: 'a + Apply, F: MapFn> Map<'a, D, F> {
    pub fn new<N: Into<Option<String>>>(data: &'a D, f: F, name: N) -> Map<'a, D, F> {
        Map {
            data,
            f,
            name: name.into().unwrap_or("Mapped".into())
        }
    }
    pub fn map<G: MapFn>(self, g: G) -> Map<'a, D, Composed<F, G>>
        where G: ApplyToDatum<<F::Output as IntoMaybeNa>::DType>
    {
        Map::new(self.data, Composed { f: self.f, g }, self.name)
    }
    pub fn name<S: AsRef<str>>(self, new_name: S) -> Map<'a, D, F> {
        Map::new(self.data, self.f, new_name.as_ref().to_string())
    }
    pub fn collect<B: FromMap<F::Output>>(self) -> Result<B> {
        B::from_map(self)
    }
}
pub trait FromMap<A: IntoMaybeNa>: Sized {
    fn from_map<'a, D: 'a + Apply, F>(map: Map<'a, D, F>) -> Result<Self>
        where F: MapFn<Output=A>;
}
impl<A: IntoMaybeNa> FromMap<A> for Vec<A> {
    fn from_map<'a, D: 'a + Apply, F>(mut map: Map<'a, D, F>) -> Result<Vec<A>>
        where F: MapFn<Output=A>
    {
        map.data.apply(&mut map.f)
    }
}
impl<A: IntoMaybeNa> FromMap<A> for DataView
    where DataStore: AddDataVec<A::DType>
{
    fn from_map<'a, D: 'a + Apply, F>(map: Map<'a, D, F>) -> Result<DataView>
        where F: MapFn<Output=A>
    {
        let field_name = map.name.clone();
        let mut mapped_data_vec = map.collect::<Vec<_>>()?;
        let data_vec = mapped_data_vec.drain(..)
            .map(|value| value.into_maybena()).collect();
        let mut ds = DataStore::empty();
        ds.add_data_vec(field_name.into(), data_vec);
        Ok(ds.into())
    }
}
// impl<'a, D: 'a + Apply, F: MapFn> Apply for Map<'a, D, F>
// {
//     fn apply<G: MapFn>(&self, g: &mut G) -> Result<Vec<G::Output>>
//         // where F: ApplyElem<<G::Output as IntoMaybeNa>::DType>
//     {
//         self.data.apply(&mut Composed { f: &mut self.f, g })
//     }
// }

// impl<'a, D: 'a + Apply, F: MapFn> TryFrom<Map<'a, D, F>> for DataView
//     where DataStore: AddDataVec<<F::Output as IntoMaybeNa>::DType>
// {
//     type Err = AgnesError;

//     fn try_from(map: Map<'a, D, F>) -> Result<DataView>
//     {
//         let mut mapped_data_vec = map.collect::<Vec<_>>()?;
//         let data_vec = mapped_data_vec.drain(..)
//             .map(|value| value.into_maybena()).collect();
//         let mut ds = DataStore::empty();
//         ds.add_data_vec("mapped".into(), data_vec);
//         Ok(ds.into())
//     }
// }
// impl<'a, D: 'a + Apply<IndexSelector> + ApplyReduce, F: MapFn> Map<'a, D, F> {
//     pub fn reduce<T: DataType, R: ReduceFn>(self, reducer: R) -> Result<R::Output>
//         where D: DataIndex<T>, F::Output: DataIndex<T>
//     {
//         Ok((0..self.data.len())
//             .map(|idx| self.data.apply(&self.f, &IndexSelector(idx)))
//             .fold(R::Initializer::initialize(), |acc, val| self.data.apply_reduce(&reducer)))
//     }
// }


/// Trait implemented by data structures which wish to be able to support `MapFn`s (type-dependent
/// functions that apply to a specific element).
// pub trait Apply<S: Selector> {
//     /// Apply an `MapFn` to an element selected with the provided `Selector`.
//     fn apply<F: MapFn>(&self, f: &mut F, select: &S) -> Result<Vec<F::Output>>;
// }


// pub trait ApplyReduce {
//     fn apply_reduce<F: ReduceFn>(&self, f: F) -> Result<F::Output>;
// }

/// Creates a MapFn that computes f(g(x)).
pub struct Composed<F: MapFn, G: MapFn> {
    f: F,
    g: G,
}
impl<F: MapFn, G: MapFn> MapFn for Composed<F, G>
    where G: ApplyToDatum<<F::Output as IntoMaybeNa>::DType>
{
    type Output = <G as ApplyToDatum<<F::Output as IntoMaybeNa>::DType>>::Output;

    fn apply_unsigned(&mut self, value: MaybeNa<&u64>) -> Self::Output {
        self.g.apply_to_datum(self.f.apply_unsigned(value).into_maybena().as_ref())
    }
    fn apply_signed(&mut self, value: MaybeNa<&i64>) -> Self::Output {
        self.g.apply_to_datum(self.f.apply_signed(value).into_maybena().as_ref())
    }
    fn apply_text(&mut self, value: MaybeNa<&String>) -> Self::Output {
        self.g.apply_to_datum(self.f.apply_text(value).into_maybena().as_ref())
    }
    fn apply_boolean(&mut self, value: MaybeNa<&bool>) -> Self::Output {
        self.g.apply_to_datum(self.f.apply_boolean(value).into_maybena().as_ref())
    }
    fn apply_float(&mut self, value: MaybeNa<&f64>) -> Self::Output {
        self.g.apply_to_datum(self.f.apply_float(value).into_maybena().as_ref())
    }
}


/// Trait for a type-dependent function that applies to a specific element.
pub trait MapFn {
    /// The desired output of this function.
    type Output: IntoMaybeNa;
    /// The method to use when working with unsigned (`u64`) data.
    fn apply_unsigned(&mut self, value: MaybeNa<&u64>) -> Self::Output;
    /// The method to use when working with signed (`i64`) data.
    fn apply_signed(&mut self, value: MaybeNa<&i64>) -> Self::Output;
    /// The method to use when working with text (`String`) data.
    fn apply_text(&mut self, value: MaybeNa<&String>) -> Self::Output;
    /// The method to use when working with boolean (`bool`) data.
    fn apply_boolean(&mut self, value: MaybeNa<&bool>) -> Self::Output;
    /// The method to use when working with floating-point (`f64`) data.
    fn apply_float(&mut self, value: MaybeNa<&f64>) -> Self::Output;
}
pub trait ApplyToDatum<T: DataType> {
    type Output: IntoMaybeNa;
    fn apply_to_datum(&mut self, value: MaybeNa<&T>) -> Self::Output;
}
macro_rules! impl_apply_datum {
    ($($dtype:ty, $f:tt);*) => {$(

impl<T> ApplyToDatum<$dtype> for T where T: MapFn {
    type Output = <Self as MapFn>::Output;
    fn apply_to_datum(&mut self, value: MaybeNa<&$dtype>) -> Self::Output {
        self.$f(value)
    }
}

    )*}
}
impl_apply_datum!(
    u64,    apply_unsigned;
    i64,    apply_signed;
    String, apply_text;
    bool,   apply_boolean;
    f64,    apply_float
);


/// Trait for a type-dependent function that applies to a single field.
pub trait FieldMapFn {
    /// The desired output of this function.
    type Output;
    /// The method to use when working with unsigned (`u64`) data.
    fn apply_unsigned<T: DataIndex<u64>>(&mut self, field: &T) -> Self::Output;
    /// The method to use when working with signed (`i64`) data.
    fn apply_signed<T: DataIndex<i64>>(&mut self, field: &T) -> Self::Output;
    /// The method to use when working with text (`String`) data.
    fn apply_text<T: DataIndex<String>>(&mut self, field: &T) -> Self::Output;
    /// The method to use when working with boolean (`bool`) data.
    fn apply_boolean<T: DataIndex<bool>>(&mut self, field: &T) -> Self::Output;
    /// The method to use when working with floating-point (`f64`) data.
    fn apply_float<T: DataIndex<f64>>(&mut self, field: &T) -> Self::Output;
}



// impl<'a, T> MapFn for &'a T where T: MapFn {
//     type Output = T::Output;
//     fn apply_unsigned(&mut self, value: MaybeNa<&u64>) -> T::Output { self.apply_unsigned(value) }
//     fn apply_signed(&mut self, value: MaybeNa<&i64>) -> T::Output { self.apply_signed(value) }
//     fn apply_text(&mut self, value: MaybeNa<&String>) -> T::Output { self.apply_text(value) }
//     fn apply_boolean(&mut self, value: MaybeNa<&bool>) -> T::Output { self.apply_boolean(value) }
//     fn apply_float(&mut self, value: MaybeNa<&f64>) -> T::Output { self.apply_float(value) }
// }


pub trait FieldReduceFn<'a> {
    /// The desired output of this function.
    type Output;
    // type Initializer: ReduceInitializer<Self::Output>;

    fn reduce(&mut self, fields: Vec<ReduceDataIndex<'a>>) -> Self::Output;

    // /// The method to use when working with unsigned (`u64`) data.
    // fn apply_unsigned<T: DataIndex<u64>>(&mut self, field: &T) -> Self::Output;
    // /// The method to use when working with signed (`i64`) data.
    // fn apply_signed<T: DataIndex<i64>>(&mut self, field: &T) -> Self::Output;
    // /// The method to use when working with text (`String`) data.
    // fn apply_text<T: DataIndex<String>>(&mut self, field: &T) -> Self::Output;
    // /// The method to use when working with boolean (`bool`) data.
    // fn apply_boolean<T: DataIndex<bool>>(&mut self, field: &T) -> Self::Output;
    // /// The method to use when working with floating-point (`f64`) data.
    // fn apply_float<T: DataIndex<f64>>(&mut self, field: &T) -> Self::Output;
}

pub trait ApplyFieldReduce<'a> {
    fn apply_field_reduce<F: FieldReduceFn<'a>>(&self, f: &mut F)
        -> Result<F::Output>;
}

// pub trait ReduceInitializer<O> {
//     fn initialize() -> O;
// }


#[cfg(test)]
mod tests {
    use super::*;
    use test_utils::*;
    use view::DataView;
    use apply::Select;

    #[test]
    fn convert() {
        let dv = sample_merged_emp_table();
        println!("{}", dv);

        struct ConvertUnsigned {}
        impl MapFn for ConvertUnsigned {
            type Output = MaybeNa<u64>;
            fn apply_unsigned(&mut self, value: MaybeNa<&u64>) -> Self::Output {
                value.map(|&val| val)
            }
            fn apply_signed(&mut self, value: MaybeNa<&i64>) -> Self::Output {
                value.map(|&val| if val < 0 { 0 } else { val as u64 })
            }
            fn apply_text(&mut self, value: MaybeNa<&String>) -> Self::Output {
                value.map(|&ref val| val.parse().unwrap_or(0))
            }
            fn apply_boolean(&mut self, value: MaybeNa<&bool>) -> Self::Output {
                value.map(|&val| if val { 1 } else { 0 })
            }
            fn apply_float(&mut self, value: MaybeNa<&f64>) -> Self::Output {
                value.map(|&val| if val < 0.0 { 0 } else { val as u64 })
            }
        }
        let mapped: DataView = dv.select(&"VacationHrs".into()).map(ConvertUnsigned {}).collect()
            .expect("failed to convert");
        println!("{}", mapped);

        struct ConvertFloat {}
        impl MapFn for ConvertFloat {
            type Output = MaybeNa<f64>;
            fn apply_unsigned(&mut self, value: MaybeNa<&u64>) -> Self::Output {
                value.map(|&val| val as f64 + 0.0001)
            }
            fn apply_signed(&mut self, value: MaybeNa<&i64>) -> Self::Output {
                value.map(|&val| val as f64)
            }
            fn apply_text(&mut self, value: MaybeNa<&String>) -> Self::Output {
                value.map(|&ref val| val.parse().unwrap_or(0.0))
            }
            fn apply_boolean(&mut self, value: MaybeNa<&bool>) -> Self::Output {
                value.map(|&val| if val { 1.0 } else { 0.0 })
            }
            fn apply_float(&mut self, value: MaybeNa<&f64>) -> Self::Output {
                value.map(|&val| val as f64)
            }
        }
        let mapped2: DataView = dv
            .select(&"VacationHrs".into())
            .map(ConvertUnsigned {})
            .map(ConvertFloat {})
            .name("VacationHrs2")
            .collect().expect("convert failed");
        println!("{}", mapped2);
    }
}
