use std::fmt;

use prettytable as pt;

use view::DataView;
use field::DtValue;
use field::FieldIdent;
use field::FieldType;
use error::*;
use apply::{Sum, Mean, Min, Max};


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
                    min: self.min(ident).map(|val| Some(val)).or_else(err_handler)?,
                    max: self.max(ident).map(|val| Some(val)).or_else(err_handler)?,
                    sum: self.sum(ident).map(|val| Some(val)).or_else(err_handler)?,
                    mean: self.mean(ident).map(|val| Some(val)).or_else(err_handler)?,
                    stdev: None
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
        println!("{}", dv1.view_stats().unwrap());
        let dv2: DataView = sample_emp_table_extra().into();
        println!("{}", dv2.view_stats().unwrap());
    }
}
