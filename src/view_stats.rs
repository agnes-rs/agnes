use std::fmt;

use prettytable as pt;

use view::DataView;
use field::DtValue;
use field::FieldIdent;
use field::FieldType;
use apply::Select;
use error::*;

/// Structure containing general statistics of a `DataView`.
#[derive(Debug, Clone)]
pub struct ViewStats {
    nrows: usize,
    fields: Vec<FieldStats>
}

/// Structure containing various statistics of a single field in a `DataView`.
#[derive(Debug, Clone)]
pub struct FieldStats {
    ident: FieldIdent,
    ty: FieldType,
    min: Option<DtValue>,
    max: Option<DtValue>,
    sum: Option<DtValue>,
    mean: Option<f64>,
    stdev: Option<f64>,
}

impl DataView {
    /// Compute and return general statistics for this `DataView`.
    pub fn view_stats(&self) -> Result<ViewStats> {
        // error handler to treat InvalidType errors as `None`.
        fn err_handler<T>(err: AgnesError) -> Result<Option<T>> {
            match err {
                AgnesError::InvalidType(_, _) => Ok(None),
                e => Err(e)
            }
        }
        Ok(ViewStats {
            nrows: self.nrows(),
            fields: self.fields().map(|ident| -> Result<FieldStats> {
                Ok(FieldStats {
                    ident: ident.clone(),
                    ty: self.get_field_type(ident).unwrap(),
                    min: self.select_one(ident).min().map(|val| Some(val)).or_else(err_handler)?,
                    max: self.select_one(ident).max().map(|val| Some(val)).or_else(err_handler)?,
                    sum: self.select_one(ident).sum().map(|val| Some(val)).or_else(err_handler)?,
                    mean: self.select_one(ident).mean().map(|val| Some(val)).or_else(err_handler)?,
                    stdev: self.select_one(ident).stdev()
                        .map(|val| Some(val)).or_else(err_handler)?,
                })
            }).collect::<Result<_>>()?
        })
    }
}

impl fmt::Display for ViewStats {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "DataView with {} rows, {} fields", self.nrows, self.fields.len())?;

        let mut table = pt::Table::new();
        table.set_titles(["Field", "Type", "Min", "Max", "Sum", "Mean", "StDev"].iter().into());

        let mut str_exists = false;
        for fstats in &self.fields {
            if fstats.ty == FieldType::Text { str_exists = true; }
            table.add_row(pt::row::Row::new(vec![
                cell![fstats.ident],
                cell![fstats.ty],
                cell![format!("{}{}",
                    fstats.min.clone().unwrap_or(DtValue::Text("".into())).to_string(),
                    if fstats.ty == FieldType::Text { "*" } else { "" }
                )],
                cell![format!("{}{}",
                    fstats.max.clone().unwrap_or(DtValue::Text("".into())).to_string(),
                    if fstats.ty == FieldType::Text { "*" } else { "" }
                )],
                cell![fstats.sum.clone().unwrap_or(DtValue::Text("".into()))],
                cell![fstats.mean.map(|val| DtValue::Float(val.clone()))
                    .unwrap_or(DtValue::Text("".into()))],
                cell![fstats.stdev.map(|val| DtValue::Float(val.clone()))
                    .unwrap_or(DtValue::Text("".into()))],
            ]));
        }

        table.set_format(*pt::format::consts::FORMAT_NO_LINESEP_WITH_TITLE);
        table.fmt(f)?;

        // if there's a text field, add descriptive footer
        if str_exists {
            writeln!(f, "* For text fields, Min and Max refer to length of contents.")?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_utils::*;

    #[test]
    fn view_stats_display() {
        let dv1: DataView = sample_emp_table().into();
        let vs1 = dv1.view_stats().unwrap();

        assert_eq!(vs1.nrows, 7);
        assert_eq!(vs1.fields.len(), 3);
        assert_eq!(vs1.fields[0].ty, FieldType::Unsigned);
        assert_eq!(vs1.fields[1].ty, FieldType::Unsigned);
        assert_eq!(vs1.fields[2].ty, FieldType::Text);

        assert_eq!(vs1.fields[0].min, Some(DtValue::Unsigned(0))); // EmpId min
        assert_eq!(vs1.fields[0].max, Some(DtValue::Unsigned(10))); // EmpId max
        assert_eq!(vs1.fields[0].sum, Some(DtValue::Unsigned(40))); // EmpId sum
        assert!((vs1.fields[0].mean.unwrap() - 5.714286).abs() < 1e-6); // EmpId mean
        assert!((vs1.fields[0].stdev.unwrap() - 3.683942).abs() < 1e-6); // EmpId stdev

        assert_eq!(vs1.fields[2].min, Some(DtValue::Unsigned(3))); // EmpName shortest len
        assert_eq!(vs1.fields[2].max, Some(DtValue::Unsigned(6))); // EmpName longest len
        assert_eq!(vs1.fields[2].sum, None); // EmpName sum is NA
        assert_eq!(vs1.fields[2].mean, None); // EmpName mean is NA
        assert_eq!(vs1.fields[2].stdev, None); // EmpName stdev is NA

        println!("{}", vs1);


        let dv2: DataView = sample_emp_table_extra().into();
        let vs2 = dv2.view_stats().unwrap();
        println!("{}", vs2);

        assert_eq!(vs2.nrows, 7);
        assert_eq!(vs2.fields.len(), 3);
        assert_eq!(vs2.fields[0].ty, FieldType::Signed);
        assert_eq!(vs2.fields[1].ty, FieldType::Boolean);
        assert_eq!(vs2.fields[2].ty, FieldType::Float);

        assert_eq!(vs2.fields[0].min, Some(DtValue::Signed(-33))); // SalaryOffset min
        assert_eq!(vs2.fields[0].max, Some(DtValue::Signed(12))); // SalaryOffset max
        assert_eq!(vs2.fields[0].sum, Some(DtValue::Signed(-13))); // SalaryOffset sum (# of true)
        assert!((vs2.fields[0].mean.unwrap() - -1.857143).abs() < 1e-6); // SalaryOffset mean
        assert!((vs2.fields[0].stdev.unwrap() - 15.004761).abs() < 1e-6); // SalaryOffset stdev

        assert_eq!(vs2.fields[1].min, Some(DtValue::Boolean(false))); // DidTraining min
        assert_eq!(vs2.fields[1].max, Some(DtValue::Boolean(true))); // DidTraining max
        assert_eq!(vs2.fields[1].sum, Some(DtValue::Unsigned(4))); // DidTraining sum (# of true)
        assert!((vs2.fields[1].mean.unwrap() - 0.571429).abs() < 1e-6); // DidTraining mean
        assert!((vs2.fields[1].stdev.unwrap() - 0.534522).abs() < 1e-6); // DidTraining stdev

        assert_eq!(vs2.fields[2].min, Some(DtValue::Float(-1.2))); // VacationHrs min
        assert_eq!(vs2.fields[2].max, Some(DtValue::Float(98.3))); // VacationHrs max
        assert_eq!(vs2.fields[2].sum, Some(DtValue::Float(238.6))); // VacationHrs sum
        assert!((vs2.fields[2].mean.unwrap() - 34.0857143).abs() < 1e-6); // VacationHrs mean
        assert!((vs2.fields[2].stdev.unwrap() -  35.070948).abs() < 1e-6); // VacationHrs stdev

    }
}
