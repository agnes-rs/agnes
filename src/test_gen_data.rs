use std::fmt::Debug;
use std::marker::PhantomData;

use rand::{self, StdRng,SeedableRng};
use rand::distributions as rdists;
use rand::distributions::Distribution;

use field::FieldIdent;
use store::{DataStore, AddFieldFromIter};
use field::Value;

pub trait GenerateInto<Fields, NewIdent, NewDType>: AddFieldFromIter<NewIdent, NewDType>
{
    fn generate_into(&self, &mut DataStore<Fields>, sz: usize, rng: &mut StdRng)
        -> DataStore<Self::OutputFields>;
}

pub struct Normal<Out> {
    mean: f64,
    stdev: f64,
    phantom: PhantomData<Out>
}
impl<Out> Normal<Out> {
    fn new(mean: f64, stdev: f64) -> Normal<Out> {
        Normal { mean, stdev, phantom: PhantomData }
    }
}

impl<Fields, NewIdent> GenerateInto<Fields, NewIdent, u64> for Normal<u64>
    // where DTypes: DTypeList,
    //       u64: DataType<DTypes>,
    //       DTypes::Storage: MaxLen<DTypes> + TypeSelector<DTypes, u64>,
    //       DataStore<DTypes>: AddDataVec<u64, DTypes>
{
    fn generate_into(&self, store: &mut DataStore<Fields>, sz: usize, rng: &mut StdRng)
        -> DataStore<Self::OutputFields>
    {
        let normal = rdists::Normal::new(self.mean, self.stdev);
        // let data: Vec<Value<u64>> =
        //     normal
        //         .sample_iter(rng)
        //         .map(|value| if value < 0.0 { 0 } else { value.round() as u64 })
        //         .map(|value| Value::Exists(value))
        //         .take(sz)
        //         .collect();
        // store.add_data_vec(ident, data.into()).expect("failure adding data while generating");
        store.add_field_from_iter::<NewIdent, _, _, _>(
            normal
                .sample_iter(rng)
                .map(|value| if value < 0.0 { 0 } else { value.round() as u64 })
                .map(|value| Value::Exists(value))
                .take(sz)
        )
    }
}
impl<Fields, NewIdent> GenerateInto<Fields, NewIdent, i64> for Normal<i64>
    // where DTypes: DTypeList,
    //       i64: DataType<DTypes>,
    //       DTypes::Storage: MaxLen<DTypes> + TypeSelector<DTypes, i64>,
    //       DataStore<DTypes>: AddDataVec<i64, DTypes>
{
    fn generate_into(&self, store: &mut DataStore<Fields>, sz: usize, rng: &mut StdRng)
        -> DataStore<Self::OutputFields>
    {
        let normal = rdists::Normal::new(self.mean, self.stdev);
        // let data: Vec<Value<i64>> =
        //     normal
        //         .sample_iter(rng)
        //         .map(|value| value.round() as i64)
        //         .map(|value| Value::Exists(value))
        //         .take(sz)
        //         .collect();
        // store.add_data_vec(ident, data.into()).expect("failure adding data while generating");
        store.add_field_from_iter::<NewIdent, _, _, _>(
            normal
                .sample_iter(rng)
                .map(|value| value.round() as i64)
                .map(|value| Value::Exists(value))
                .take(sz)
        )
    }
}
impl<Fields, NewIdent> GenerateInto<Fields, NewIdent, f64> for Normal<f64>
    // where DTypes: DTypeList,
    //       f64: DataType<DTypes>,
    //       DTypes::Storage: MaxLen<DTypes> + TypeSelector<DTypes, f64>,
    //       DataStore<DTypes>: AddDataVec<f64, DTypes>
{
    fn generate_into(&self, store: &mut DataStore<Fields>, sz: usize, rng: &mut StdRng)
        -> DataStore<Self::OutputFields>
    {
        let normal = rdists::Normal::new(self.mean, self.stdev);
        // let data: Vec<Value<f64>> =
        //     normal
        //         .sample_iter(rng)
        //         .map(|value| Value::Exists(value))
        //         .take(sz)
        //         .collect();
        // store.add_data_vec(ident, data.into()).expect("failure adding data while generating");
        store.add_field_from_iter::<NewIdent, _, _, _>(
            normal
                .sample_iter(rng)
                .map(|value| Value::Exists(value))
                .take(sz)
        )
    }
}

pub struct Uniform<T> {
    low: T,
    high: T,
}
impl<T> Uniform<T> {
    fn new(low: T, high: T) -> Uniform<T> {
        Uniform { low, high }
    }
}

macro_rules! impl_uniform_generate_into {
    ($($t:ty)*) => {$(

impl<Fields, NewIdent> GenerateInto<Fields, NewIdent, $t> for Uniform<$t>
    // where DTypes: DTypeList,
    //       $t: DataType<DTypes>,
    //       DTypes::Storage: MaxLen<DTypes> + TypeSelector<DTypes, $t>,
    //       DataStore<DTypes>: AddDataVec<$t, DTypes>
{
    fn generate_into(&self, store: &mut DataStore<Fields>, sz: usize, rng: &mut StdRng)
        -> DataStore<Self::OutputFields>
    {
        let uniform = rdists::Uniform::new(self.low, self.high);
        store.add_field_from_iter::<NewIdent, _, _, _>(
            uniform
                .sample_iter(rng)
                .map(|value| Value::Exists(value))
                .take(sz)
        )
        // let data: Vec<Value<$t>> =
        //     uniform
        //         .sample_iter(rng)
        //         .map(|value| Value::Exists(value))
        //         .take(sz)
        //         .collect();
        // store.add_data_vec(ident, data.into()).expect("failure adding data while generating");
    }
}

    )*}
}
impl_uniform_generate_into![u64 i64 f64];

pub struct UniformChoice<T> {
    choices: Vec<T>
}
impl<T> UniformChoice<T> {
    fn new(choices: Vec<T>) -> UniformChoice<T> {
        UniformChoice { choices }
    }
}
macro_rules! impl_uniform_choice_generate_into {
    ($($t:ty)*) => {$(

impl<Fields, NewIdent> GenerateInto<Fields, NewIdent, $t> for UniformChoice<$t>
    // where DTypes: DTypeList,
    //       $t: DataType<DTypes>,
    //       DTypes::Storage: MaxLen<DTypes> + TypeSelector<DTypes, $t>,
    //       DataStore<DTypes>: AddDataVec<$t, DTypes>

{
    fn generate_into(
        &self, store: &mut DataStore<Fields>, sz: usize, rng: &mut StdRng
    )
        -> DataStore<Self::OutputFields>
    {
        let uniform = rdists::Uniform::new(0, self.choices.len());
        // let data: Vec<Value<$t>> =
        //     uniform
        //     .sample_iter(rng)
        //     .map(|idx| self.choices[idx].clone())
        //     .map(|value| Value::Exists(value))
        //     .take(sz)
        //     .collect();
        // store.add_data_vec(ident, data.into()).expect("failure adding data while generating");
        store.add_field_from_iter::<NewIdent, _, _, _>(
            uniform
                .sample_iter(rng)
                .map(|idx| self.choices[idx].clone())
                .map(|value| Value::Exists(value))
                .take(sz)
        )
    }
}

    )*}
}
impl_uniform_choice_generate_into![u64 i64 String bool f64];

impl<'a> From<Vec<&'a str>> for UniformChoice<String> {
    fn from(other: Vec<&'a str>) -> UniformChoice<String> {
        UniformChoice::new(other.iter().map(|s| s.to_string()).collect())
    }
}

pub struct FieldGenerator<Fields, NewIdent, NewDType>(
    Box<dyn GenerateInto<Fields, NewIdent, NewDType>>
);
impl<Fields, NewIdent, NewDType> GenerateInto<Fields, NewIdent, NewDType>
    for FieldGenerator<Fields, NewIdent, NewDType>
    // where DTypes: DTypeList
{
    fn generate_into(&self, store: &mut DataStore<Fields>, sz: usize, rng: &mut StdRng)
    {
        self.0.generate_into(store, sz, rng);
    }
}
impl<Fields, NewIdent, NewDType> From<Normal<NewDType>>
    for FieldGenerator<Fields, NewIdent, NewDType>
    where Normal<NewDType>: GenerateInto<Fields, NewIdent, NewDType>,
{
    fn from(normal: Normal<NewDType>) -> FieldGenerator<Fields, NewIdent, NewDType> {
        FieldGenerator(Box::new(normal))
    }
}
impl<Fields, NewIdent, NewDType> From<Uniform<NewDType>>
    for FieldGenerator<Fields, NewIdent, NewDType>
    where Uniform<NewDType>: GenerateInto<Fields, NewIdent, NewDType>,
          // DTypes: DTypeList
{
    fn from(uniform: Uniform<NewDType>) -> FieldGenerator<Fields, NewIdent, NewDType> {
        FieldGenerator(Box::new(uniform))
    }
}
impl<Fields, NewIdent, NewDType> From<UniformChoice<NewDType>>
    for FieldGenerator<Fields, NewIdent, NewDType>
    where UniformChoice<NewDType>: GenerateInto<Fields, NewIdent, NewDType>,
          // DTypes: DTypeList
{
    fn from(uc: UniformChoice<NewDType>) -> FieldGenerator<Fields, NewIdent, NewDType> {
        FieldGenerator(Box::new(uc))
    }
}

#[allow(dead_code)]
pub struct FieldSpec<Fields, NewIdent, NewDType> {
    generator: FieldGenerator<Fields, NewIdent, NewDType>
}
impl<Fields, NewIdent, NewDType> FieldSpec<Fields, NewIdent, NewDType> {
    #[allow(dead_code)]
    pub fn new<I, G>(generator: G)
        -> FieldSpec<Fields, NewIdent, NewDType>
        where G: Into<FieldGenerator<Fields, NewIdent, NewDType>>
    {
        FieldSpec {
            generator: generator.into()
        }
    }
}
#[allow(dead_code)]
pub fn generate_random_datastore<Fields, NewIdent, NewDType, I: Into<Option<u64>>>(
    fields: Vec<FieldSpec<Fields, NewIdent, NewDType>>,
    nrecords: usize,
    seed: I
)
    -> DataStore<GenerateInto<Fields, NewIdent, NewDType>::Output>
    // where DTypes: DTypeList,
    //       DTypes::Storage: MaxLen<DTypes> + CreateStorage
{
    let mut store = DataStore::empty();
    let mut rng = match seed.into() {
        Some(seed) => {
            //TODO: switch to from_bytes when 1.29.0 hits stable
            let bytes: [u8; 8] = unsafe { ::std::mem::transmute(seed) };
            let mut seed_bytes = [0u8; 32];
            for i in 0..8 { seed_bytes[i] = bytes[i]; }
            StdRng::from_seed(seed_bytes)
        },
        None => {
            StdRng::from_rng(rand::thread_rng()).expect("failed to create StdRng from thread RNG")
        }
    };
    for field in &fields {
        field.generator.generate_into(&mut store, field.ident.clone(), nrecords, &mut rng);
    }
    store
}

#[allow(dead_code)]
pub fn generate_sample_random_datastore<Fields, NewIdent, NewDType, I: Into<Option<u64>>>(
    nrecords: usize, seed: I
)
    -> DataStore<Fields>
    // where DTypes: DTypeList,
    //       String: DataType<DTypes>, i64: DataType<DTypes>, f64: DataType<DTypes>,
    //       u64: DataType<DTypes>,
    //       DTypes::Storage: MaxLen<DTypes> + CreateStorage
    //               + DTypeSelector<DTypes, String> + TypeSelector<DTypes, String>
    //               + DTypeSelector<DTypes, i64> + TypeSelector<DTypes, i64>
    //               + DTypeSelector<DTypes, f64> + TypeSelector<DTypes, f64>
    //               + DTypeSelector<DTypes, u64> + TypeSelector<DTypes, u64>
{
    generate_random_datastore(
        vec![
            FieldSpec::new("col1", UniformChoice::from(vec!["choice1", "choice2"])),
            FieldSpec::new("col2", Uniform::new(1u64, 5u64)),
            FieldSpec::new("col3", Normal::<u64>::new(8.0, 2.0)),
            FieldSpec::new("col4", Normal::<f64>::new(102.4, 15.2)),
            FieldSpec::new("col5", Normal::<i64>::new(3.0, 2.0)),
        ],
        nrecords,
        seed
    )
}