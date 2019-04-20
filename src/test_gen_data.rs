use field::FieldData;
use rand::distributions as rdists;
use rand::distributions::Distribution;
use rand::StdRng;

use value::Value;

pub trait Generate<T> {
    fn generate(&self, sz: usize, rng: &mut StdRng) -> FieldData<T>;
}

pub struct Normal {
    mean: f64,
    stdev: f64,
}
impl Normal {
    pub fn new(mean: f64, stdev: f64) -> Normal {
        Normal { mean, stdev }
    }
}

impl Generate<u64> for Normal {
    fn generate(&self, sz: usize, rng: &mut StdRng) -> FieldData<u64> {
        let normal = rdists::Normal::new(self.mean, self.stdev);
        normal
            .sample_iter(rng)
            .map(|value| if value < 0.0 { 0 } else { value.round() as u64 })
            .map(|value| Value::Exists(value))
            .take(sz)
            .collect()
    }
}

impl Generate<i64> for Normal {
    fn generate(&self, sz: usize, rng: &mut StdRng) -> FieldData<i64> {
        let normal = rdists::Normal::new(self.mean, self.stdev);
        normal
            .sample_iter(rng)
            .map(|value| value.round() as i64)
            .map(|value| Value::Exists(value))
            .take(sz)
            .collect()
    }
}

impl Generate<f64> for Normal {
    fn generate(&self, sz: usize, rng: &mut StdRng) -> FieldData<f64> {
        let normal = rdists::Normal::new(self.mean, self.stdev);
        normal
            .sample_iter(rng)
            .map(|value| Value::Exists(value))
            .take(sz)
            .collect()
    }
}

pub struct Uniform<T> {
    low: T,
    high: T,
}

impl<T> Uniform<T> {
    pub fn new(low: T, high: T) -> Uniform<T> {
        Uniform { low, high }
    }
}

macro_rules! impl_uniform_generate {
    ($($t:ty)*) => {$(

        impl Generate<$t> for Uniform<$t> {
            fn generate(&self, sz: usize, rng: &mut StdRng) -> FieldData<$t> {

                let uniform = rdists::Uniform::new(self.low, self.high);
                uniform
                    .sample_iter(rng)
                    .map(|value| Value::Exists(value))
                    .take(sz)
                    .collect()
            }
        }

    )*}
}

impl_uniform_generate![u64 i64 f64];

pub struct UniformChoice<T> {
    choices: Vec<T>,
}

impl<T> UniformChoice<T> {
    fn new(choices: Vec<T>) -> UniformChoice<T> {
        UniformChoice { choices }
    }
}

macro_rules! impl_uniform_choice_generate {
    ($($t:ty)*) => {$(

        impl Generate<$t> for UniformChoice<$t> {
            fn generate(&self, sz: usize, rng: &mut StdRng) -> FieldData<$t> {
                let uniform = rdists::Uniform::new(0, self.choices.len());
                uniform
                    .sample_iter(rng)
                    .map(|idx| self.choices[idx].clone())
                    .map(|value| Value::Exists(value))
                    .take(sz)
                    .collect()
            }
        }

    )*}
}

impl_uniform_choice_generate![u64 i64 String bool f64];

impl From<&[&str]> for UniformChoice<String> {
    fn from(other: &[&str]) -> UniformChoice<String> {
        UniformChoice::new(other.iter().map(|s| s.to_string()).collect())
    }
}
