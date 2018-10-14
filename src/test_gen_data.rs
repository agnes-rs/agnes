use std::marker::PhantomData;

use rand::{self, StdRng,SeedableRng};
use rand::distributions as rdists;
use rand::distributions::Distribution;

use data_types::{DTypeList, MaxLen, CreateStorage, DTypeSelector, TypeSelector,
    DataType};
use field::FieldIdent;
use store::{AddDataVec, DataStore};
use field::Value;

pub(crate) trait GenerateInto<DTypes: DTypeList> {
    fn generate_into(&self, &mut DataStore<DTypes>, ident: FieldIdent, sz: usize, rng: &mut StdRng);
}

pub(crate) struct Normal<Out> {
    mean: f64,
    stdev: f64,
    phantom: PhantomData<Out>
}
impl<Out> Normal<Out> {
    fn new(mean: f64, stdev: f64) -> Normal<Out> {
        Normal { mean, stdev, phantom: PhantomData }
    }
}

impl<DTypes> GenerateInto<DTypes> for Normal<u64>
    where DTypes: DTypeList,
          u64: DataType<DTypes>,
          DTypes::Storage: MaxLen<DTypes> + TypeSelector<DTypes, u64>,
          DataStore<DTypes>: AddDataVec<u64, DTypes>
{
    fn generate_into(
        &self, store: &mut DataStore<DTypes>, ident: FieldIdent, sz: usize, rng: &mut StdRng
    )
    {
        let normal = rdists::Normal::new(self.mean, self.stdev);
        let data: Vec<Value<u64>> =
            normal
                .sample_iter(rng)
                .map(|value| if value < 0.0 { 0 } else { value.round() as u64 })
                .map(|value| Value::Exists(value))
                .take(sz)
                .collect();
        store.add_data_vec(ident, data.into()).expect("failure adding data while generating");
    }
}
impl<DTypes> GenerateInto<DTypes> for Normal<i64>
    where DTypes: DTypeList,
          i64: DataType<DTypes>,
          DTypes::Storage: MaxLen<DTypes> + TypeSelector<DTypes, i64>,
          DataStore<DTypes>: AddDataVec<i64, DTypes>
{
    fn generate_into(
        &self, store: &mut DataStore<DTypes>, ident: FieldIdent, sz: usize, rng: &mut StdRng
    )
    {
        let normal = rdists::Normal::new(self.mean, self.stdev);
        let data: Vec<Value<i64>> =
            normal
                .sample_iter(rng)
                .map(|value| value.round() as i64)
                .map(|value| Value::Exists(value))
                .take(sz)
                .collect();
        store.add_data_vec(ident, data.into()).expect("failure adding data while generating");
    }
}
impl<DTypes> GenerateInto<DTypes> for Normal<f64>
    where DTypes: DTypeList,
          f64: DataType<DTypes>,
          DTypes::Storage: MaxLen<DTypes> + TypeSelector<DTypes, f64>,
          DataStore<DTypes>: AddDataVec<f64, DTypes>
{
    fn generate_into(
        &self, store: &mut DataStore<DTypes>, ident: FieldIdent, sz: usize, rng: &mut StdRng
    )
    {
        let normal = rdists::Normal::new(self.mean, self.stdev);
        let data: Vec<Value<f64>> =
            normal
                .sample_iter(rng)
                .map(|value| Value::Exists(value))
                .take(sz)
                .collect();
        store.add_data_vec(ident, data.into()).expect("failure adding data while generating");
    }
}

pub(crate) struct Uniform<T> {
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

impl<DTypes> GenerateInto<DTypes> for Uniform<$t>
    where DTypes: DTypeList,
          $t: DataType<DTypes>,
          DTypes::Storage: MaxLen<DTypes> + TypeSelector<DTypes, $t>,
          DataStore<DTypes>: AddDataVec<$t, DTypes>
{
    fn generate_into(
        &self, store: &mut DataStore<DTypes>, ident: FieldIdent, sz: usize, rng: &mut StdRng
    )
    {
        let uniform = rdists::Uniform::new(self.low, self.high);
        let data: Vec<Value<$t>> =
            uniform
                .sample_iter(rng)
                .map(|value| Value::Exists(value))
                .take(sz)
                .collect();
        store.add_data_vec(ident, data.into()).expect("failure adding data while generating");
    }
}

    )*}
}
impl_uniform_generate_into![u64 i64 f64];

pub(crate) struct UniformChoice<T> {
    choices: Vec<T>
}
impl<T> UniformChoice<T> {
    fn new(choices: Vec<T>) -> UniformChoice<T> {
        UniformChoice { choices }
    }
}
macro_rules! impl_uniform_choice_generate_into {
    ($($t:ty)*) => {$(

impl<DTypes> GenerateInto<DTypes> for UniformChoice<$t>
    where DTypes: DTypeList,
          $t: DataType<DTypes>,
          DTypes::Storage: MaxLen<DTypes> + TypeSelector<DTypes, $t>,
          DataStore<DTypes>: AddDataVec<$t, DTypes>

{
    fn generate_into(
        &self, store: &mut DataStore<DTypes>, ident: FieldIdent, sz: usize, rng: &mut StdRng
    )
    {

        let uniform = rdists::Uniform::new(0, self.choices.len());
        let data: Vec<Value<$t>> =

            uniform
            .sample_iter(rng)
            .map(|idx| self.choices[idx].clone())
            .map(|value| Value::Exists(value))
            .take(sz)
            .collect();
        store.add_data_vec(ident, data.into()).expect("failure adding data while generating");
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

pub(crate) struct FieldGenerator<DTypes>(Box<dyn GenerateInto<DTypes>>);
impl<DTypes> GenerateInto<DTypes> for FieldGenerator<DTypes>
    where DTypes: DTypeList
{
    fn generate_into(
        &self, store: &mut DataStore<DTypes>, ident: FieldIdent, sz: usize, rng: &mut StdRng
    )
    {
        self.0.generate_into(store, ident, sz, rng);
    }
}
impl<DTypes, T> From<Normal<T>> for FieldGenerator<DTypes>
    where Normal<T>: 'static + GenerateInto<DTypes>,
          DTypes: DTypeList
{
    fn from(normal: Normal<T>) -> FieldGenerator<DTypes> {
        FieldGenerator(Box::new(normal))
    }
}
impl<DTypes, T> From<Uniform<T>> for FieldGenerator<DTypes>
    where Uniform<T>: 'static + GenerateInto<DTypes>,
          DTypes: DTypeList
{
    fn from(uniform: Uniform<T>) -> FieldGenerator<DTypes> {
        FieldGenerator(Box::new(uniform))
    }
}
impl<DTypes, T> From<UniformChoice<T>> for FieldGenerator<DTypes>
    where UniformChoice<T>: 'static + GenerateInto<DTypes>,
          DTypes: DTypeList
{
    fn from(uc: UniformChoice<T>) -> FieldGenerator<DTypes> {
        FieldGenerator(Box::new(uc))
    }
}

#[allow(dead_code)]
pub(crate) struct FieldSpec<DTypes> {
    ident: FieldIdent,
    generator: FieldGenerator<DTypes>
}
impl<DTypes> FieldSpec<DTypes> {
    #[allow(dead_code)]
    pub(crate) fn new<I, G>(ident: I, generator: G)
        -> FieldSpec<DTypes>
        where I: Into<FieldIdent>, G: Into<FieldGenerator<DTypes>>
    {
        FieldSpec {
            ident: ident.into(),
            generator: generator.into()
        }
    }
}
#[allow(dead_code)]
pub(crate) fn generate_random_datastore<I: Into<Option<u64>>, DTypes>(
    fields: Vec<FieldSpec<DTypes>>,
    nrecords: usize,
    seed: I
)
    -> DataStore<DTypes>
    where DTypes: DTypeList,
          DTypes::Storage: MaxLen<DTypes> + CreateStorage
{
    let mut store = DataStore::<DTypes>::empty();
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
pub(crate) fn generate_sample_random_datastore<DTypes, I: Into<Option<u64>>>(
    nrecords: usize, seed: I
)
    -> DataStore<DTypes>
    where DTypes: DTypeList,
          String: DataType<DTypes>, i64: DataType<DTypes>, f64: DataType<DTypes>,
          u64: DataType<DTypes>,
          DTypes::Storage: MaxLen<DTypes> + CreateStorage
                  + DTypeSelector<DTypes, String> + TypeSelector<DTypes, String>
                  + DTypeSelector<DTypes, i64> + TypeSelector<DTypes, i64>
                  + DTypeSelector<DTypes, f64> + TypeSelector<DTypes, f64>
                  + DTypeSelector<DTypes, u64> + TypeSelector<DTypes, u64>
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