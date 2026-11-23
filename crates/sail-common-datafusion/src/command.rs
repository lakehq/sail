// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, Int64Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::prelude::SessionConfig;
use datafusion_common::{DFSchema, DFSchemaRef, Result, internal_err};

use crate::session::config::SessionConfigService;

pub const RETURN_WRITE_METRICS_KEY: &str = "sail.execution.return_write_metrics";

/// Expose write counts to SQL clients that request them while preserving Spark defaults.
pub fn return_write_metrics(config: &SessionConfig) -> Result<bool> {
    let Some(service) = config.get_extension::<SessionConfigService>() else {
        return Ok(false);
    };
    match service.get_option(RETURN_WRITE_METRICS_KEY)?.as_deref() {
        None => Ok(false),
        Some(v) if v.trim().eq_ignore_ascii_case("true") => Ok(true),
        Some(v) if v.trim().eq_ignore_ascii_case("false") => Ok(false),
        Some(v) => {
            datafusion_common::plan_err!("{RETURN_WRITE_METRICS_KEY} must be a boolean, got {v}")
        }
    }
}

/// The result contract selected by a command's planner, independent of runtime input.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum CommandOutput {
    Empty,
    /// Retained for internal writes and commands outside SQL row-level DML.
    Count,
    AffectedRows,
    MergeRows,
}

impl CommandOutput {
    pub fn schema(self) -> SchemaRef {
        let names: &[&str] = match self {
            Self::Empty => &[],
            Self::Count => &["count"],
            Self::AffectedRows => &["num_affected_rows"],
            Self::MergeRows => &[
                "num_affected_rows",
                "num_updated_rows",
                "num_deleted_rows",
                "num_inserted_rows",
            ],
        };
        Arc::new(Schema::new(
            names
                .iter()
                .map(|name| Field::new(*name, DataType::Int64, true))
                .collect::<Vec<_>>(),
        ))
    }

    #[expect(clippy::expect_used)]
    pub fn df_schema(self) -> DFSchemaRef {
        // The fixed result fields are unique and unqualified.
        Arc::new(DFSchema::try_from(self.schema()).expect("valid command result schema"))
    }

    /// Empty results contain neither columns nor rows. Call only after consuming the mutation.
    pub fn batch(self, values: &[i64]) -> Result<RecordBatch> {
        let schema = self.schema();
        if values.len() != schema.fields().len() {
            return internal_err!("command result does not match its planned schema");
        }
        if self == Self::Empty {
            return Ok(RecordBatch::new_empty(schema));
        }
        let columns = values
            .iter()
            .map(|value| Arc::new(Int64Array::from(vec![*value])) as ArrayRef)
            .collect();
        Ok(RecordBatch::try_new(schema, columns)?)
    }

    pub fn empty_batch(self) -> Result<RecordBatch> {
        self.batch(&vec![0; self.schema().fields().len()])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn command_results_have_static_nullable_long_schemas() -> Result<()> {
        for (output, columns, rows) in [
            (CommandOutput::Empty, 0, 0),
            (CommandOutput::Count, 1, 1),
            (CommandOutput::AffectedRows, 1, 1),
            (CommandOutput::MergeRows, 4, 1),
        ] {
            let batch = output.empty_batch()?;
            assert_eq!(batch.num_columns(), columns);
            assert_eq!(batch.num_rows(), rows);
            assert_eq!(output.df_schema().as_arrow(), batch.schema().as_ref());
            assert!(
                batch
                    .schema()
                    .fields()
                    .iter()
                    .all(|field| field.is_nullable() && field.data_type() == &DataType::Int64)
            );
        }
        Ok(())
    }
}
