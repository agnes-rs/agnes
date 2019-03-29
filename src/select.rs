/*!
Traits for selecting a field from a data structure.
*/
use access::DataIndex;

/// Trait for accessing the data of a single field as a struct which implements
/// [DataIndex](../access/trait.DataIndex.html).
pub trait FieldSelect {
    /// Returns a struct containing the data for the field specified by `Label`.
    ///
    /// This method is a convenience method for calling the
    /// [select_field](trait.SelectFieldByLabel.html#select_field)
    /// method on the [SelectFieldByLabel](trait.SelectFieldByLabel.html) trait.
    fn field<Label>(&self) -> <Self as SelectFieldByLabel<Label>>::Output
    where
        Self: SelectFieldByLabel<Label>,
    {
        SelectFieldByLabel::select_field(self)
    }
}

/// Trait implemented by data structures to provide access to data for a single field.
pub trait SelectFieldByLabel<Label> {
    /// Data type of accessed data.
    type DType;
    /// The return type for the `select_field` method.
    type Output: DataIndex<DType = Self::DType>;

    /// Returns an object that provides [DataIndex](../access/trait.DataIndex.html) access to the
    /// data in the field specified by `Label`.
    fn select_field(&self) -> Self::Output;
}

#[cfg(test)]
mod tests {
    use super::FieldSelect;

    use access::DataIndex;
    use field::Value;

    #[cfg(feature = "test-utils")]
    use test_utils::*;

    #[cfg(feature = "test-utils")]
    #[test]
    fn select() {
        use test_utils::emp_table::*;

        let dv = sample_merged_emp_table();
        println!("{}", dv);
        let result = dv
            .field::<EmpId>()
            .iter()
            .map(|datum: Value<&u64>| if datum.exists() { 1i64 } else { 0 })
            .collect::<Vec<_>>();
        assert_eq!(result, vec![1, 1, 1, 1, 1, 1, 1]);
    }
}
