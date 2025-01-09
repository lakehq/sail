use std::sync::Arc;

use datafusion::catalog::TableFunction;

use crate::function::table::range::RangeTableFunction;

mod range;

pub(super) fn list_built_in_table_functions() -> Vec<(&'static str, Arc<TableFunction>)> {
    vec![("range", Arc::new(RangeTableFunction::new()))]
        .into_iter()
        .map(|(name, func)| (name, Arc::new(TableFunction::new(name.to_string(), func))))
        .collect()
}
