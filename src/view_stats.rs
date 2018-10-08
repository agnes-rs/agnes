use std::fmt;

use prettytable as pt;

use data_types::{MaxLen};
use apply::stats::{MinFn, MaxFn, SumFn, MeanFn, StdevFn};
use view::DataView;
use field::FieldIdent;
use frame::{Reindexer};
use data_types::{DTypeList, MapPartial, FuncPartial, FieldLocator};
use error::*;

pub struct StringifyFn<F> {
    inner: F,
}
impl<DTypes, F> FuncPartial<DTypes> for StringifyFn<F>
    where DTypes: DTypeList,
          F: FuncPartial<DTypes>,
          F::Output: ToString,
{
    type Output = String;
    fn call_partial<L, R>(
        &mut self,
        locator: &L,
        reindexer: &R,
        storage: &DTypes::Storage,
    )
        -> Option<String>
        where L: FieldLocator<DTypes>,
              R: Reindexer<DTypes>
    {
        self.inner.call_partial(locator, reindexer, storage).map(|out| out.to_string())
    }
}

/// Structure containing general statistics of a `DataView`.
#[derive(Debug, Clone)]
pub struct ViewStats<DTypes: DTypeList> {
    nrows: usize,
    fields: Vec<FieldStats<DTypes>>
}

/// Structure containing various statistics of a single field in a `DataView`.
#[derive(Debug, Clone)]
pub struct FieldStats<DTypes: DTypeList> {
    ident: FieldIdent,
    ty: DTypes::DType,
    min: Option<String>,
    max: Option<String>,
    sum: Option<String>,
    mean: Option<String>,
    stdev: Option<String>,
}

impl<DTypes: DTypeList> DataView<DTypes>
    where DTypes: DTypeList,
          DTypes::Storage: MaxLen<DTypes>,
          MinFn: FuncPartial<DTypes>, <MinFn as FuncPartial<DTypes>>::Output: ToString,
          MaxFn: FuncPartial<DTypes>, <MaxFn as FuncPartial<DTypes>>::Output: ToString,
          SumFn: FuncPartial<DTypes>, <SumFn as FuncPartial<DTypes>>::Output: ToString,
          MeanFn: FuncPartial<DTypes>, <MeanFn as FuncPartial<DTypes>>::Output: ToString,
          StdevFn: FuncPartial<DTypes>, <StdevFn as FuncPartial<DTypes>>::Output: ToString,
          DTypes::Storage: MapPartial<DTypes, StringifyFn<MinFn>>,
          DTypes::Storage: MapPartial<DTypes, StringifyFn<MaxFn>>,
          DTypes::Storage: MapPartial<DTypes, StringifyFn<SumFn>>,
          DTypes::Storage: MapPartial<DTypes, StringifyFn<MeanFn>>,
          DTypes::Storage: MapPartial<DTypes, StringifyFn<StdevFn>>,
{
    /// Compute and return general statistics for this `DataView`.
    pub fn view_stats(&self) -> Result<ViewStats<DTypes>>
    {
        Ok(ViewStats {
            nrows: self.nrows(),
            fields: self.idents().map(|ident| -> Result<FieldStats<DTypes>> {
                Ok(FieldStats {
                    ident: ident.clone(),
                    ty: self.get_field_type(ident).unwrap(),
                    min: self.map_partial(ident, StringifyFn { inner: MinFn })?,
                    max: self.map_partial(ident, StringifyFn { inner: MaxFn })?,
                    sum: self.map_partial(ident, StringifyFn { inner: SumFn })?,
                    mean: self.map_partial(ident, StringifyFn { inner: MeanFn })?,
                    stdev: self.map_partial(ident, StringifyFn { inner: StdevFn })?,
                })
            }).collect::<Result<_>>()?
        })
    }
}

impl<DTypes: DTypeList> fmt::Display for ViewStats<DTypes> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "DataView with {} rows, {} fields", self.nrows, self.fields.len())?;

        let mut table = pt::Table::new();
        table.set_titles(["Field", "Type", "Min*", "Max*", "Sum", "Mean", "StDev"].iter().into());

        for fstats in &self.fields {
            table.add_row(pt::row::Row::new(vec![
                cell![fstats.ident],
                cell![fstats.ty],
                cell![fstats.min.clone().unwrap_or_default()],
                cell![fstats.max.clone().unwrap_or_default()],
                cell![fstats.sum.clone().unwrap_or_default()],
                cell![fstats.mean.clone().unwrap_or_default()],
                cell![fstats.stdev.clone().unwrap_or_default()],
            ]));
        }

        table.set_format(*pt::format::consts::FORMAT_NO_LINESEP_WITH_TITLE);
        table.fmt(f)?;

        // TODO: add this footer (and footnore markers on Min and Max) only if text field exists
        // in DataView
        writeln!(f, "* For text fields, Min and Max refer to length of contents.")?;
        Ok(())
    }
}


#[cfg(test)]
mod tests {
    use test_utils::*;
    use data_types::standard::*;

    #[test]
    fn view_stats_display() {
        let dv1: DataView = sample_emp_table().into();
        let vs1 = dv1.view_stats().unwrap();
        println!("{}", vs1);
        assert_eq!(vs1.nrows, 7);
        assert_eq!(vs1.fields.len(), 3);
        println!("{:?}", vs1.fields);
        assert_eq!(vs1.fields[0].ty, DType::u64);
        assert_eq!(vs1.fields[1].ty, DType::u64);
        assert_eq!(vs1.fields[2].ty, DType::String);

        assert_eq!(vs1.fields[0].min, Some("0".to_string())); // EmpId min
        assert_eq!(vs1.fields[0].max, Some("10".to_string())); // EmpId max
        assert_eq!(vs1.fields[0].sum, Some("40".to_string())); // EmpId sum
        assert!((vs1.fields[0].mean.clone().unwrap().parse::<f64>().unwrap() - 5.714286).abs()
            < 1e-6); // EmpId mean
        assert!((vs1.fields[0].stdev.clone().unwrap().parse::<f64>().unwrap() - 3.683942).abs()
            < 1e-6); // EmpId stdev

        assert_eq!(vs1.fields[2].min, Some("3".to_string())); // EmpName shortest len
        assert_eq!(vs1.fields[2].max, Some("6".to_string())); // EmpName longest len
        assert_eq!(vs1.fields[2].sum, None); // EmpName sum is NA
        assert_eq!(vs1.fields[2].mean, None); // EmpName mean is NA
        assert_eq!(vs1.fields[2].stdev, None); // EmpName stdev is NA

        println!("{}", vs1);


        let dv2: DataView = sample_emp_table_extra().into();
        let vs2 = dv2.view_stats().unwrap();
        println!("{}", vs2);

        assert_eq!(vs2.nrows, 7);
        assert_eq!(vs2.fields.len(), 3);
        assert_eq!(vs2.fields[0].ty, DType::i64);
        assert_eq!(vs2.fields[1].ty, DType::bool);
        assert_eq!(vs2.fields[2].ty, DType::f64);

        assert_eq!(vs2.fields[0].min, Some("-33".to_string())); // SalaryOffset min
        assert_eq!(vs2.fields[0].max, Some("12".to_string())); // SalaryOffset max
        assert_eq!(vs2.fields[0].sum, Some("-13".to_string())); // SalaryOffset sum (# of true)
        assert!((vs2.fields[0].mean.clone().unwrap().parse::<f64>().unwrap() - -1.857143).abs()
            < 1e-6); // SalaryOffset mean
        assert!((vs2.fields[0].stdev.clone().unwrap().parse::<f64>().unwrap() - 15.004761).abs()
            < 1e-6); // SalaryOffset stdev

        assert_eq!(vs2.fields[1].min, Some("false".to_string())); // DidTraining min
        assert_eq!(vs2.fields[1].max, Some("true".to_string())); // DidTraining max
        assert_eq!(vs2.fields[1].sum, Some("4".to_string())); // DidTraining sum (# of true)
        assert!((vs2.fields[1].mean.clone().unwrap().parse::<f64>().unwrap() - 0.571429).abs()
            < 1e-6); // DidTraining mean
        assert!((vs2.fields[1].stdev.clone().unwrap().parse::<f64>().unwrap() - 0.534522).abs()
            < 1e-6); // DidTraining stdev

        assert_eq!(vs2.fields[2].min, Some("-1.2".to_string())); // VacationHrs min
        assert_eq!(vs2.fields[2].max, Some("98.3".to_string())); // VacationHrs max
        assert_eq!(vs2.fields[2].sum, Some("238.6".to_string())); // VacationHrs sum
        assert!((vs2.fields[2].mean.clone().unwrap().parse::<f64>().unwrap() - 34.0857143).abs()
            < 1e-6); // VacationHrs mean
        assert!((vs2.fields[2].stdev.clone().unwrap().parse::<f64>().unwrap() -  35.070948).abs()
            < 1e-6); // VacationHrs stdev


    }
}
