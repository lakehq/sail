use std::sync::Arc;

use arrow_schema::{DataType, Field};
use datafusion_common::{Result, plan_err};
use sail_common::spec::SAIL_LIST_FIELD_NAME;
pub struct ListParams {
    pub inner_field: Arc<Field>,
    pub is_large: bool,
    pub fixed_size_opt: Option<i32>,
}

impl ListParams {
    pub fn new(inner_field: Arc<Field>, is_large: bool, fixed_size_opt: Option<i32>) -> Self {
        Self {
            inner_field,
            is_large,
            fixed_size_opt,
        }
    }
}

pub fn get_list_params(data_type: &DataType) -> Result<ListParams> {
    match data_type {
        DataType::List(field) | DataType::ListView(field) => {
            Ok(ListParams::new(field.clone(), false, None))
        }
        DataType::FixedSizeList(field, size) => {
            Ok(ListParams::new(field.clone(), false, Some(*size)))
        }
        DataType::LargeList(field) | DataType::LargeListView(field) => {
            Ok(ListParams::new(field.clone(), true, None))
        }
        // Spark coerces bare untyped NULL to `array<void>`.
        DataType::Null => Ok(ListParams::new(
            Arc::new(Field::new_list_field(DataType::Null, true)),
            false,
            None,
        )),
        _ => plan_err!("`arrays_zip` can only accept List, LargeList or FixedSizeList."),
    }
}
pub fn default_field_names(count: usize) -> Vec<String> {
    (0..count).map(|i| format!("{i}")).collect::<Vec<_>>()
}
pub fn struct_result_field(inner_fields: &[Arc<Field>], field_names: &[String]) -> Arc<Field> {
    let fields = field_names
        .iter()
        .zip(inner_fields)
        .map(|(name, f)| {
            Field::new(name, f.data_type().clone(), true).with_metadata(f.metadata().clone())
        })
        .collect::<Vec<_>>();
    Arc::new(Field::new_struct(SAIL_LIST_FIELD_NAME, fields, false))
}
pub fn return_type(field_names: &[String], arg_types: &[DataType]) -> Result<DataType> {
    let params = arg_types
        .iter()
        .map(get_list_params)
        .collect::<Result<Vec<_>>>()?;

    let field_names = if field_names.len() == params.len() {
        field_names.to_vec()
    } else {
        default_field_names(params.len())
    };
    let struct_field = struct_result_field(
        &params
            .iter()
            .map(|row| row.inner_field.clone())
            .collect::<Vec<_>>(),
        &field_names,
    );

    let is_large = params.iter().any(|row| row.is_large);

    let fixed_size_opt = params
        .iter()
        .map(|row| row.fixed_size_opt)
        .try_fold(None, |prev, item| match (prev, item) {
            (None, x) => Ok(x),
            (Some(p), Some(x)) if p == x => Ok(Some(p)),
            _ => Err(()),
        })
        .ok()
        .flatten();

    Ok(match (is_large, fixed_size_opt) {
        (true, _) => DataType::LargeList(struct_field),
        (false, Some(fixed_size)) => DataType::FixedSizeList(struct_field, fixed_size),
        _ => DataType::List(struct_field),
    })
}
