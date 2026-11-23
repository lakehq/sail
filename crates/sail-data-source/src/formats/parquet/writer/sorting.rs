// SPDX-License-Identifier: Apache-2.0

use datafusion::arrow::datatypes::Schema;
use datafusion::parquet::file::metadata::SortingColumn;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr_common::sort_expr::{LexOrdering, PhysicalSortExpr};
use datafusion_common::{DataFusionError, Result, internal_datafusion_err};

// SPDX-SnippetBegin
// SPDX-License-Identifier: Apache-2.0
// SPDX-SnippetComment: Credit: https://github.com/apache/datafusion/blob/500744d5c25499055e3b2ca46b022bc425989203/datafusion/datasource-parquet/src/metadata.rs#L954-L1016
/// Convert a [`PhysicalSortExpr`] to a Parquet [`SortingColumn`].
///
/// Returns `Ok(None)` if the referenced column is not in `writer_schema`, such as a
/// hive partition column that is removed before writing the Parquet file. Returns
/// `Err` if the expression is not a simple column reference or references a column
/// outside `input_schema`.
pub(crate) fn sort_expr_to_sorting_column(
    sort_expr: &PhysicalSortExpr,
    input_schema: &Schema,
    writer_schema: &Schema,
) -> Result<Option<SortingColumn>> {
    let column = sort_expr.expr.downcast_ref::<Column>().ok_or_else(|| {
        DataFusionError::Plan(format!(
            "Parquet sorting_columns only supports simple column references, \
                 but got expression: {}",
            sort_expr.expr
        ))
    })?;

    let input_field = input_schema.fields().get(column.index()).ok_or_else(|| {
        internal_datafusion_err!(
            "Parquet sorting column '{}' references index {} but the input schema has {} columns",
            column.name(),
            column.index(),
            input_schema.fields().len()
        )
    })?;
    let Some((writer_index, _)) = writer_schema.column_with_name(input_field.name()) else {
        return Ok(None);
    };

    let column_idx: i32 = writer_index.try_into().map_err(|_| {
        DataFusionError::Plan(format!(
            "Column index {writer_index} is too large to be represented as i32"
        ))
    })?;

    Ok(Some(SortingColumn {
        column_idx,
        descending: sort_expr.options.descending,
        nulls_first: sort_expr.options.nulls_first,
    }))
}

/// Convert a [`LexOrdering`] to `Vec<SortingColumn>` for Parquet.
///
/// Columns that are not present in `writer_schema` are omitted from the resulting
/// metadata. Returns `Err` if any expression is not a simple column reference or
/// references a column outside `input_schema`.
pub(crate) fn lex_ordering_to_sorting_columns(
    ordering: &LexOrdering,
    input_schema: &Schema,
    writer_schema: &Schema,
) -> Result<Vec<SortingColumn>> {
    ordering
        .iter()
        .filter_map(|sort_expr| {
            sort_expr_to_sorting_column(sort_expr, input_schema, writer_schema).transpose()
        })
        .collect()
}
// SPDX-SnippetEnd
