use datafusion_expr::LogicalPlan;
use sail_common::spec;

use crate::error::{PlanError, PlanResult};
use crate::resolver::command::write::{WriteColumnMatch, WriteMode, WritePlanBuilder, WriteTarget};
use crate::resolver::state::PlanResolverState;
use crate::resolver::PlanResolver;

impl PlanResolver<'_> {
    pub(super) async fn resolve_command_insert_overwrite_directory(
        &self,
        input: spec::QueryPlan,
        // TODO: `local` is ignored for now since the object store can be inferred from
        //   the URL scheme in `location`. But we may want to validate it in the future
        //   and return an error if `local` does not match the type of `location`.
        _local: bool,
        location: Option<String>,
        file_format: Option<spec::TableFileFormat>,
        row_format: Option<spec::TableRowFormat>,
        options: Vec<(String, String)>,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let Some(location) = location else {
            return Err(PlanError::invalid(
                "missing location for INSERT OVERWRITE DIRECTORY",
            ));
        };
        if row_format.is_some() {
            return Err(PlanError::todo("row format for INSERT OVERWRITE DIRECTORY"));
        }
        let format = match file_format {
            Some(spec::TableFileFormat::General { format }) => format,
            Some(spec::TableFileFormat::Table { .. }) => {
                return Err(PlanError::todo(
                    "table file format for INSERT OVERWRITE DIRECTORY",
                ));
            }
            None => {
                return Err(PlanError::invalid(
                    "missing file format for INSERT OVERWRITE DIRECTORY",
                ));
            }
        };
        let builder = WritePlanBuilder::new()
            .with_mode(WriteMode::Replace {
                error_if_absent: false,
            })
            .with_target(WriteTarget::DataSource)
            .with_format(format)
            .with_options(options)
            .with_options(vec![("path".to_string(), location)]);
        let input = self.resolve_write_input(input, state).await?;
        self.resolve_write_with_builder(input, builder, state).await
    }

    pub(super) async fn resolve_command_insert_into(
        &self,
        input: spec::QueryPlan,
        table: spec::ObjectName,
        mode: spec::InsertMode,
        partition: Vec<(spec::Identifier, Option<spec::Expr>)>,
        if_not_exists: bool,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        use spec::InsertMode;

        if if_not_exists {
            return Err(PlanError::todo(
                "INSERT INTO ... IF NOT EXISTS is not supported",
            ));
        }

        let input = self.resolve_write_input(input, state).await?;

        if !partition.is_empty() {
            return Err(PlanError::todo("PARTITION for write"));
        }
        let mut builder = WritePlanBuilder::new();
        match mode {
            InsertMode::InsertByPosition { overwrite } => {
                let mode = if overwrite {
                    WriteMode::Truncate
                } else {
                    WriteMode::Append {
                        error_if_absent: true,
                    }
                };
                builder = builder.with_mode(mode).with_target(WriteTarget::Table {
                    table,
                    column_match: WriteColumnMatch::ByPosition,
                });
            }
            InsertMode::InsertByName { overwrite } => {
                let mode = if overwrite {
                    WriteMode::Truncate
                } else {
                    WriteMode::Append {
                        error_if_absent: true,
                    }
                };
                builder = builder.with_mode(mode).with_target(WriteTarget::Table {
                    table,
                    column_match: WriteColumnMatch::ByName,
                });
            }
            InsertMode::InsertByColumns { columns, overwrite } => {
                let mode = if overwrite {
                    WriteMode::Truncate
                } else {
                    WriteMode::Append {
                        error_if_absent: true,
                    }
                };
                builder = builder.with_mode(mode).with_target(WriteTarget::Table {
                    table,
                    column_match: WriteColumnMatch::ByColumns { columns },
                });
            }
            InsertMode::Replace { condition } => {
                builder = builder
                    .with_mode(WriteMode::TruncateIf { condition })
                    .with_target(WriteTarget::Table {
                        table,
                        column_match: WriteColumnMatch::ByPosition,
                    });
            }
        };

        self.resolve_write_with_builder(input, builder, state).await
    }
}
