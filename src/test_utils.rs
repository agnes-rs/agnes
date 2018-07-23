use std::marker::PhantomData;

use rand::{self, StdRng,SeedableRng};
use rand::distributions as rdists;
use rand::distributions::Distribution;

use masked::MaybeNa;
use view::IntoFieldList;
use store::{DataStore, AddDataVec};
use view::DataView;
use masked::MaskedData;
use field::FieldIdent;

pub(crate) fn sample_emp_table() -> DataStore {
    emp_table(vec![0u64, 2, 5, 6, 8, 9, 10], vec![1u64, 2, 1, 1, 3, 4, 4],
        vec!["Sally", "Jamie", "Bob", "Cara", "Louis", "Louise", "Ann"])
}
pub(crate) fn emp_table(empids: Vec<u64>, deptids: Vec<u64>, names: Vec<&str>) -> DataStore {
    emp_table_from_masked(empids.into(), deptids.into(), names.into())
}
pub(crate) fn emp_table_from_masked(empids: MaskedData<u64>, deptids: MaskedData<u64>,
    names: MaskedData<String>) -> DataStore
{
    DataStore::with_data(
        // unsigned
        vec![
            ("EmpId", empids),
            ("DeptId", deptids)
        ],
        // signed
        None,
        // text
        vec![
            ("EmpName", names)
        ],
        // boolean
        None,
        // float
        None
    )
}
pub(crate) fn sample_emp_table_extra() -> DataStore {
    DataStore::with_data(
        None,
        vec![
            ("SalaryOffset", vec![-5, 4, 12, -33, 10, 0, -1].into())
        ],
        None,
        vec![
            ("DidTraining", vec![false, false, true, true, true, false, true].into())
        ],
        vec![
            ("VacationHrs", vec![47.3, 54.1, 98.3, 12.2, -1.2, 5.4, 22.5].into()),
        ]
    )
}
pub(crate) fn sample_merged_emp_table() -> DataView {
    let ds = sample_emp_table();
    let orig_dv: DataView = ds.into();
    orig_dv.merge(&sample_emp_table_extra().into()).unwrap()
}
pub(crate) trait MergedWithSample {
    fn merged_with_sample_emp_table(self, name: &str) -> DataView;
}
impl MergedWithSample for Vec<u64> {
    fn merged_with_sample_emp_table(self, name: &str) -> DataView {
        let orig_dv: DataView = sample_emp_table().into();
        orig_dv
            .merge(&DataStore::with_data(vec![(name, self.into())], None, None, None, None).into())
            .unwrap()
    }
}
impl MergedWithSample for Vec<i64> {
    fn merged_with_sample_emp_table(self, name: &str) -> DataView {
        let orig_dv: DataView = sample_emp_table().into();
        orig_dv
            .merge(&DataStore::with_data(None, vec![(name, self.into())], None, None, None).into())
            .unwrap()
    }
}
impl MergedWithSample for Vec<String> {
    fn merged_with_sample_emp_table(self, name: &str) -> DataView {
        let orig_dv: DataView = sample_emp_table().into();
        orig_dv
            .merge(&DataStore::with_data(None, None, vec![(name, self.into())], None, None).into())
            .unwrap()
    }
}
impl MergedWithSample for Vec<bool> {
    fn merged_with_sample_emp_table(self, name: &str) -> DataView {
        let orig_dv: DataView = sample_emp_table().into();
        orig_dv
            .merge(&DataStore::with_data(None, None, None, vec![(name, self.into())], None).into())
            .unwrap()
    }
}
impl MergedWithSample for Vec<f64> {
    fn merged_with_sample_emp_table(self, name: &str) -> DataView {
        let orig_dv: DataView = sample_emp_table().into();
        orig_dv
            .merge(&DataStore::with_data(None, None, None, None, vec![(name, self.into())]).into())
            .unwrap()
    }
}

pub(crate) fn sample_dept_table() -> DataStore {
    dept_table(vec![1u64, 2, 3, 4], vec!["Marketing", "Sales", "Manufacturing", "R&D"])
}
pub(crate) fn dept_table(deptids: Vec<u64>, names: Vec<&str>) -> DataStore {
    dept_table_from_masked(deptids.into(), names.into())
}
pub(crate) fn dept_table_from_masked(deptids: MaskedData<u64>, names: MaskedData<String>)
    -> DataStore
{
    DataStore::with_data(
        // unsigned
        vec![
            ("DeptId", deptids)
        ],
        // signed
        None,
        // text
        vec![
            ("DeptName", names)
        ],
        // boolean
        None,
        // float
        None
    )
}

macro_rules! impl_assert_vec_eq_and_pred {
    ($dtype:ty) => {

use view::DataView;
use apply::{Field, Matches};
use masked::MaybeNa;

#[allow(dead_code)]
pub(crate) fn assert_dv_eq_vec<'a, R>(left: &DataView, ident: &'a FieldIdent, mut right: Vec<R>)
    // where T: ApplyToField<FieldSelector<'a>> + Matches<FieldIndexSelector<'a>, $dtype>,
          where R: Into<$dtype>
{
    let right: Vec<$dtype> = right.drain(..).map(|r| r.into()).collect();
    for (i, rval) in (0..right.len()).zip(right) {
        assert!(left.field(ident).unwrap().matches(i, &rval).unwrap());
    }
}

#[allow(dead_code)]
pub(crate) fn assert_dv_pred<'a, F>(left: &DataView, ident: &'a FieldIdent, mut f: F)
    where F: FnMut(&$dtype) -> bool
{
    assert!(left.field(ident).unwrap().data_iter::<$dtype>().all(|val| {
        match val {
            MaybeNa::Exists(val) => f(val),
            MaybeNa::Na => false
        }
    }));
}

    }
}

macro_rules! impl_assert_sorted_eq {
    ($dtype:ty) => {

use apply::SortOrderBy;

#[allow(dead_code)]
pub(crate) fn assert_dv_sorted_eq<'a, R>(left: &DataView, ident: &'a FieldIdent, mut right: Vec<R>)
    where //T: ApplyToField<FieldSelector<'a>> + Matches<FieldIndexSelector<'a>, $dtype>,
          R: Into<$dtype>
{
    let left_order = left.sort_order_by(ident).unwrap();
    let mut right: Vec<$dtype> = right.drain(..).map(|r| r.into()).collect();
    right.sort();

    for (lidx, rval) in left_order.iter().zip(right.iter()) {
        assert!(left.field(ident).unwrap().matches(*lidx, rval).unwrap());
    }
}

    }
}

macro_rules! impl_test_helpers {
    ($name:tt; $dtype:ty) => {

pub(crate) mod $name {
    use field::FieldIdent;

    impl_assert_vec_eq_and_pred!($dtype);
    impl_assert_sorted_eq!($dtype);

}

    }
}

impl_test_helpers!(unsigned; u64);
impl_test_helpers!(signed;   i64);
impl_test_helpers!(text;     String);
impl_test_helpers!(boolean;  bool);

pub(crate) mod float {
    use field::FieldIdent;
    use apply::SortOrderBy;

    impl_assert_vec_eq_and_pred!(f64);

    #[allow(dead_code)]
    pub(crate) fn assert_dv_sorted_eq<'a, R>(left: &DataView, ident: &'a FieldIdent,
        mut right: Vec<R>)
        where R: Into<f64>
    {
        let left_order = left.sort_order_by(ident).unwrap();
        let mut right: Vec<f64> = right.drain(..).map(|r| r.into()).collect();
        right.sort_by(|a, b| a.partial_cmp(b).unwrap());

        for (lidx, rval) in left_order.iter().zip(right.iter()) {
            assert!(left.field(ident).unwrap().matches(*lidx, rval).unwrap());
        }
    }

}

pub(crate) fn assert_field_lists_match<L: IntoFieldList, R: IntoFieldList>(left: L, right: R) {
    assert_eq!(left.into_field_list(), right.into_field_list());
}

pub(crate) trait GenerateInto {
    fn generate_into(&self, &mut DataStore, ident: FieldIdent, sz: usize, rng: &mut StdRng);
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

impl GenerateInto for Normal<u64> {
    fn generate_into(&self, store: &mut DataStore, ident: FieldIdent, sz: usize, rng: &mut StdRng)
    {
        let normal = rdists::Normal::new(self.mean, self.stdev);
        let data: Vec<MaybeNa<u64>> =
            normal
                .sample_iter(rng)
                .map(|value| if value < 0.0 { 0 } else { value.round() as u64 })
                .map(|value| MaybeNa::Exists(value))
                .take(sz)
                .collect();
        store.add_data_vec(ident, data.into());
    }
}
impl GenerateInto for Normal<i64> {
    fn generate_into(&self, store: &mut DataStore, ident: FieldIdent, sz: usize, rng: &mut StdRng)
    {
        let normal = rdists::Normal::new(self.mean, self.stdev);
        let data: Vec<MaybeNa<i64>> =
            normal
                .sample_iter(rng)
                .map(|value| value.round() as i64)
                .map(|value| MaybeNa::Exists(value))
                .take(sz)
                .collect();
        store.add_data_vec(ident, data.into());
    }
}
impl GenerateInto for Normal<f64> {
    fn generate_into(&self, store: &mut DataStore, ident: FieldIdent, sz: usize, rng: &mut StdRng)
    {
        let normal = rdists::Normal::new(self.mean, self.stdev);
        let data: Vec<MaybeNa<f64>> =
            normal
                .sample_iter(rng)
                .map(|value| MaybeNa::Exists(value))
                .take(sz)
                .collect();
        store.add_data_vec(ident, data.into());
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

impl GenerateInto for Uniform<$t> {
    fn generate_into(&self, store: &mut DataStore, ident: FieldIdent, sz: usize, rng: &mut StdRng)
    {
        let uniform = rdists::Uniform::new(self.low, self.high);
        let data: Vec<MaybeNa<$t>> =
            uniform
                .sample_iter(rng)
                .map(|value| MaybeNa::Exists(value))
                .take(sz)
                .collect();
        store.add_data_vec(ident, data.into());
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

impl GenerateInto for UniformChoice<$t> {
    fn generate_into(&self, store: &mut DataStore, ident: FieldIdent, sz: usize, rng: &mut StdRng)
    {

        let uniform = rdists::Uniform::new(0, self.choices.len());
        let data: Vec<MaybeNa<$t>> =

            uniform
            .sample_iter(rng)
            .map(|idx| self.choices[idx].clone())
            .map(|value| MaybeNa::Exists(value))
            .take(sz)
            .collect();
        store.add_data_vec(ident, data.into());
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

pub(crate) struct FieldGenerator(Box<dyn GenerateInto>);
impl GenerateInto for FieldGenerator {
    fn generate_into(&self, store: &mut DataStore, ident: FieldIdent, sz: usize, rng: &mut StdRng)
    {
        self.0.generate_into(store, ident, sz, rng);
    }
}
impl<T> From<Normal<T>> for FieldGenerator where Normal<T>: 'static + GenerateInto {
    fn from(normal: Normal<T>) -> FieldGenerator {
        FieldGenerator(Box::new(normal))
    }
}
impl<T> From<Uniform<T>> for FieldGenerator where Uniform<T>: 'static + GenerateInto {
    fn from(uniform: Uniform<T>) -> FieldGenerator {
        FieldGenerator(Box::new(uniform))
    }
}
impl<T> From<UniformChoice<T>> for FieldGenerator where UniformChoice<T>: 'static + GenerateInto {
    fn from(uc: UniformChoice<T>) -> FieldGenerator {
        FieldGenerator(Box::new(uc))
    }
}

#[allow(dead_code)]
pub(crate) struct FieldSpec {
    ident: FieldIdent,
    generator: FieldGenerator
}
impl FieldSpec {
    #[allow(dead_code)]
    pub(crate) fn new<I: Into<FieldIdent>, G: Into<FieldGenerator>>(ident: I, generator: G)
        -> FieldSpec
    {
        FieldSpec {
            ident: ident.into(),
            generator: generator.into()
        }
    }
}
#[allow(dead_code)]
pub(crate) fn generate_random_datastore<I: Into<Option<u64>>>(
    fields: Vec<FieldSpec>,
    nrecords: usize,
    seed: I)
    -> DataStore
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
pub(crate) fn generate_sample_random_datastore<I: Into<Option<u64>>>(nrecords: usize, seed: I)
    -> DataStore
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
