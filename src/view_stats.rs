/*!
Functions for displaying statistics about a `DataView`.
*/

use std::fmt;

use prettytable as pt;

use access::DataIndex;
use cons::Len;
use label::{StrLabels, StrTypes};
use partial::*;
use stats::*;
use store::NRows;
use view::{AssocDataIndexCons, AssocDataIndexConsOf, DataView};

/// Structure containing general statistics of a `DataView`.
#[derive(Debug, Clone)]
pub struct ViewStats {
    nrows: usize,
    nfields: usize,
    idents: Vec<String>,
    tys: Vec<String>,
    mins: Vec<String>,
    maxs: Vec<String>,
    sums: Vec<String>,
    means: Vec<String>,
    stdevs: Vec<String>,
}

#[derive(Debug)]
pub struct MinFn {
    values: Vec<String>,
}
impl Default for MinFn {
    fn default() -> MinFn {
        MinFn { values: vec![] }
    }
}
impl FuncDefault for MinFn {
    type Output = ();
    fn call(&mut self) -> () {
        self.values.push(String::new());
    }
}

#[derive(Debug)]
pub struct MaxFn {
    values: Vec<String>,
}
impl Default for MaxFn {
    fn default() -> MaxFn {
        MaxFn { values: vec![] }
    }
}
impl FuncDefault for MaxFn {
    type Output = ();
    fn call(&mut self) -> () {
        self.values.push(String::new());
    }
}

#[derive(Debug)]
pub struct SumFn {
    values: Vec<String>,
}
impl Default for SumFn {
    fn default() -> SumFn {
        SumFn { values: vec![] }
    }
}
impl FuncDefault for SumFn {
    type Output = ();
    fn call(&mut self) -> () {
        self.values.push(String::new());
    }
}

#[derive(Debug)]
pub struct MeanFn {
    values: Vec<String>,
}
impl Default for MeanFn {
    fn default() -> MeanFn {
        MeanFn { values: vec![] }
    }
}
impl FuncDefault for MeanFn {
    type Output = ();
    fn call(&mut self) -> () {
        self.values.push(String::new());
    }
}

#[derive(Debug)]
pub struct StDevFn {
    values: Vec<String>,
}
impl Default for StDevFn {
    fn default() -> StDevFn {
        StDevFn { values: vec![] }
    }
}
impl FuncDefault for StDevFn {
    type Output = ();
    fn call(&mut self) -> () {
        self.values.push(String::new());
    }
}

macro_rules! impl_stats_fns {
    ($($dtype:ty)*) => {$(

        impl Func<$dtype> for MinFn {
            type Output = ();
            fn call<DI>(&mut self, data: &DI) -> ()
            where
                DI: DataIndex<DType=$dtype>
            {
                self.values.push(data.min().map_or(String::new(), ToString::to_string));
            }
        }
        impl IsImplemented<MinFn> for $dtype {
            type IsImpl = Implemented;
        }

        impl Func<$dtype> for MaxFn {
            type Output = ();
            fn call<DI>(&mut self, data: &DI) -> ()
            where
                DI: DataIndex<DType=$dtype>
            {
                self.values.push(data.max().map_or(String::new(), ToString::to_string));
            }
        }
        impl IsImplemented<MaxFn> for $dtype {
            type IsImpl = Implemented;
        }

        impl Func<$dtype> for SumFn {
            type Output = ();
            fn call<DI>(&mut self, data: &DI) -> ()
            where
                DI: DataIndex<DType=$dtype>
            {
                self.values.push(data.sum().to_string());
            }
        }
        impl IsImplemented<SumFn> for $dtype {
            type IsImpl = Implemented;
        }

        impl Func<$dtype> for MeanFn {
            type Output = ();
            fn call<DI>(&mut self, data: &DI) -> ()
            where
                DI: DataIndex<DType=$dtype>
            {
                self.values.push(data.mean().to_string());
            }
        }
        impl IsImplemented<MeanFn> for $dtype {
            type IsImpl = Implemented;
        }

        impl Func<$dtype> for StDevFn {
            type Output = ();
            fn call<DI>(&mut self, data: &DI) -> ()
            where
                DI: DataIndex<DType=$dtype>
            {
                self.values.push(data.stdev().to_string());
            }
        }
        impl IsImplemented<StDevFn> for $dtype {
            type IsImpl = Implemented;
        }

    )*}
}

impl_stats_fns![f64 f32 u64 u32 usize i64 i32 isize];

macro_rules! impl_stats_fns_nonimpl {
    ($($dtype:ty)*) => {$(

        impl IsImplemented<MinFn> for $dtype {
            type IsImpl = Unimplemented;
        }
        impl IsImplemented<MaxFn> for $dtype {
            type IsImpl = Unimplemented;
        }
        impl IsImplemented<SumFn> for $dtype {
            type IsImpl = Unimplemented;
        }
        impl IsImplemented<MeanFn> for $dtype {
            type IsImpl = Unimplemented;
        }
        impl IsImplemented<StDevFn> for $dtype {
            type IsImpl = Unimplemented;
        }

    )*}
}

impl_stats_fns_nonimpl![bool String];

impl<Labels, Frames> DataView<Labels, Frames>
where
    Frames: Len + NRows + AssocDataIndexCons<Labels>,
    AssocDataIndexConsOf<Labels, Frames>: DeriveCapabilities<MinFn>,
    AssocDataIndexConsOf<Labels, Frames>: DeriveCapabilities<MaxFn>,
    AssocDataIndexConsOf<Labels, Frames>: DeriveCapabilities<SumFn>,
    AssocDataIndexConsOf<Labels, Frames>: DeriveCapabilities<MeanFn>,
    AssocDataIndexConsOf<Labels, Frames>: DeriveCapabilities<StDevFn>,
    Labels: Len + StrLabels + StrTypes,
{
    /// Compute and return general statistics for this `DataView`.
    pub fn view_stats(&self) -> ViewStats {
        let mut min_fn = MinFn::default();
        DeriveCapabilities::<MinFn>::derive(self.frames.assoc_data()).map(&mut min_fn);
        let mut max_fn = MaxFn::default();
        DeriveCapabilities::<MaxFn>::derive(self.frames.assoc_data()).map(&mut max_fn);
        let mut sum_fn = SumFn::default();
        DeriveCapabilities::<SumFn>::derive(self.frames.assoc_data()).map(&mut sum_fn);
        let mut mean_fn = MeanFn::default();
        DeriveCapabilities::<MeanFn>::derive(self.frames.assoc_data()).map(&mut mean_fn);
        let mut stdev_fn = StDevFn::default();
        DeriveCapabilities::<StDevFn>::derive(self.frames.assoc_data()).map(&mut stdev_fn);

        let view_stats = ViewStats {
            nrows: self.nrows(),
            nfields: self.nfields(),
            idents: <Labels as StrLabels>::labels()
                .iter()
                .map(|s| s.to_string())
                .collect(),
            tys: <Labels as StrTypes>::str_types()
                .iter()
                .map(|s| s.to_string())
                .collect(),
            mins: min_fn.values,
            maxs: max_fn.values,
            sums: sum_fn.values,
            means: mean_fn.values,
            stdevs: stdev_fn.values,
        };

        view_stats
    }
}

impl fmt::Display for ViewStats {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(
            f,
            "DataView with {} rows, {} fields",
            self.nrows, self.nfields
        )?;

        let mut table = pt::Table::new();
        table.set_titles(
            ["Field", "Type", "Min", "Max", "Sum", "Mean", "StDev"]
                .iter()
                .into(),
        );

        debug_assert_eq!(self.idents.len(), self.tys.len());
        debug_assert_eq!(self.idents.len(), self.mins.len());
        debug_assert_eq!(self.idents.len(), self.maxs.len());
        debug_assert_eq!(self.idents.len(), self.sums.len());
        debug_assert_eq!(self.idents.len(), self.means.len());
        debug_assert_eq!(self.idents.len(), self.stdevs.len());

        for i in 0..self.mins.len() {
            table.add_row(pt::row::Row::new(vec![
                cell![self.idents[i]],
                cell![self.tys[i]],
                cell![self.mins[i]],
                cell![self.maxs[i]],
                cell![self.sums[i]],
                cell![self.means[i]],
                cell![self.stdevs[i]],
            ]));
        }

        table.set_format(*pt::format::consts::FORMAT_NO_LINESEP_WITH_TITLE);
        table.fmt(f)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use test_utils::*;

    macro_rules! assert_float_eq {
        ($actual:expr, $expected:expr) => {{
            assert!(($actual.clone().parse::<f64>().unwrap() - $expected).abs() < 1e-4);
        }};
    }
    #[test]
    fn view_stats_display() {
        let dv_emp = sample_emp_table().into_view();
        println!("{}", dv_emp);
        let vs1 = dv_emp.view_stats();
        println!("{}", vs1);
        assert_eq!(vs1.nrows, 7);
        assert_eq!(vs1.nfields, 3);
        assert_eq!(vs1.tys[0], "u64".to_string());
        assert_eq!(vs1.tys[1], "u64".to_string());
        assert_eq!(vs1.tys[2], "String".to_string());

        assert_eq!(vs1.mins[0], "0".to_string()); // EmpId min
        assert_eq!(vs1.maxs[0], "10".to_string()); // EmpId max
        assert_eq!(vs1.sums[0], "40".to_string()); // EmpId sum
        assert_float_eq!(vs1.means[0], 5.714286); // EmpId mean
        assert_float_eq!(vs1.stdevs[0], 3.683942); // EmpId stdev

        assert_eq!(vs1.mins[2], "".to_string()); // EmpName shortest len
        assert_eq!(vs1.maxs[2], "".to_string()); // EmpName longest len
        assert_eq!(vs1.sums[2], "".to_string()); // EmpName sum is NA
        assert_eq!(vs1.means[2], "".to_string()); // EmpName mean is NA
        assert_eq!(vs1.stdevs[2], "".to_string()); // EmpName stdev is NA

        println!("{}", vs1);

        let dv_extra = sample_emp_table_extra().into_view();
        println!("{}", dv_extra);
        let vs2 = dv_extra.view_stats();
        println!("{}", vs2);

        assert_eq!(vs2.nrows, 7);
        assert_eq!(vs2.nfields, 3);
        assert_eq!(vs2.tys[0], "i64".to_string());
        assert_eq!(vs2.tys[1], "bool".to_string());
        assert_eq!(vs2.tys[2], "f32".to_string());

        assert_eq!(vs2.mins[0], "-33".to_string()); // SalaryOffset min
        assert_eq!(vs2.maxs[0], "12".to_string()); // SalaryOffset max
        assert_eq!(vs2.sums[0], "-13".to_string()); // SalaryOffset sum (# of true)
        assert_float_eq!(vs2.means[0], -1.857143); // SalaryOffset mean
        assert_float_eq!(vs2.stdevs[0], 15.004761); // SalaryOffset stdev

        assert_eq!(vs2.mins[1], "".to_string()); // DidTraining min
        assert_eq!(vs2.maxs[1], "".to_string()); // DidTraining max
        assert_eq!(vs2.sums[1], "".to_string()); // DidTraining sum
        assert_eq!(vs2.means[1], "".to_string()); // DidTraining mean
        assert_eq!(vs2.stdevs[1], "".to_string()); // DidTraining stdev

        assert_eq!(vs2.mins[2], "-1.2".to_string()); // VacationHrs min
        assert_eq!(vs2.maxs[2], "98.3".to_string()); // VacationHrs max
        assert_float_eq!(vs2.sums[2], 238.6); // VacationHrs sum
        assert_float_eq!(vs2.means[2], 34.0857143); // VacationHrs mean
        assert_float_eq!(vs2.stdevs[2], 35.070948); // VacationHrs stdev
    }
}
