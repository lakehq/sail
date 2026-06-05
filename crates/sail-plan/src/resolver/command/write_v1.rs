use datafusion_expr::LogicalPlan;
use sail_common::spec;

use crate::error::{PlanError, PlanResult};
use crate::resolver::command::write::{WriteColumnMatch, WriteMode, WritePlanBuilder, WriteTarget};
use crate::resolver::state::PlanResolverState;
use crate::resolver::PlanResolver;

impl PlanResolver<'_> {
    /// Resolves the write operation for the Spark DataFrameWriter v1 API.
    pub(super) async fn resolve_command_write(
        &self,
        write: spec::Write,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        use spec::{SaveMode, SaveType, TableSaveMethod};

        let spec::Write {
            input,
            source,
            save_type,
            mode,
            sort_columns,
            partitioning_columns,
            clustering_columns,
            bucket_by,
            options,
        } = write;

        let replace_where = options.iter().find_map(|(k, v)| {
            if k.eq_ignore_ascii_case("replaceWhere") || k.eq_ignore_ascii_case("replace_where") {
                Some(v.clone())
            } else {
                None
            }
        });

        let input = self.resolve_write_input(*input, state).await?;
        let clustering_columns = self.resolve_write_cluster_by_columns(clustering_columns)?;

        let partition_by = self.resolve_write_partition_by_expressions(partitioning_columns)?;
        let mut builder = WritePlanBuilder::new()
            .with_partition_by(partition_by)
            .with_bucket_by(bucket_by)
            .with_sort_by(sort_columns)
            .with_cluster_by(clustering_columns)
            .with_options(options);
        if let Some(source) = source {
            builder = builder.with_format(source);
        }
        let to_write_mode = |mode: Option<SaveMode>| -> PlanResult<WriteMode> {
            let write_mode = match mode {
                Some(SaveMode::ErrorIfExists) | None => WriteMode::ErrorIfExists,
                Some(SaveMode::IgnoreIfExists) => WriteMode::IgnoreIfExists,
                Some(SaveMode::Append) => WriteMode::Append {
                    error_if_absent: false,
                },
                Some(SaveMode::Overwrite) => match replace_where {
                    Some(ref replace_where) => {
                        let ast_expr =
                            sail_sql_analyzer::parser::parse_expression(replace_where.as_str())
                                .map_err(|e| {
                                    PlanError::invalid(format!(
                                        "invalid replaceWhere expression: {replace_where} ({e})"
                                    ))
                                })?;
                        let spec_expr = sail_sql_analyzer::expression::from_ast_expression(
                            ast_expr,
                        )
                        .map_err(|e| {
                            PlanError::invalid(format!(
                                "invalid replaceWhere expression: {replace_where} ({e})"
                            ))
                        })?;
                        WriteMode::TruncateIf {
                            condition: Box::new(spec::ExprWithSource {
                                expr: spec_expr,
                                source: Some(replace_where.clone()),
                            }),
                        }
                    }
                    None => WriteMode::Replace {
                        error_if_absent: false,
                    },
                },
            };
            Ok(write_mode)
        };

        match save_type {
            SaveType::Path(location) => {
                let mode = to_write_mode(mode)?;
                builder = builder
                    .with_target(WriteTarget::DataSource)
                    .with_mode(mode)
                    .with_options(vec![("path".to_string(), location)]);
            }
            SaveType::Sink => {
                let mode = to_write_mode(mode)?;
                // Any "path" option supplied by the user (e.g. via
                // df.write.format(...).option("path", path).save()) remains in
                // options and is picked up by WriteTarget::DataSource.
                builder = builder.with_target(WriteTarget::DataSource).with_mode(mode);
            }
            SaveType::Table {
                table,
                save_method: TableSaveMethod::SaveAsTable,
            } => match mode {
                Some(SaveMode::ErrorIfExists) | None => {
                    builder = builder
                        .with_target(WriteTarget::Table {
                            table,
                            column_match: WriteColumnMatch::ByName,
                        })
                        .with_mode(WriteMode::ErrorIfExists);
                }
                Some(SaveMode::IgnoreIfExists) => {
                    builder = builder
                        .with_target(WriteTarget::Table {
                            table,
                            column_match: WriteColumnMatch::ByName,
                        })
                        .with_mode(WriteMode::IgnoreIfExists);
                }
                Some(SaveMode::Append) => {
                    builder = builder
                        .with_target(WriteTarget::Table {
                            table,
                            column_match: WriteColumnMatch::ByName,
                        })
                        .with_mode(WriteMode::Append {
                            error_if_absent: false,
                        });
                }
                Some(SaveMode::Overwrite) => {
                    builder = builder
                        .with_target(WriteTarget::Table {
                            table,
                            column_match: WriteColumnMatch::ByName,
                        })
                        .with_mode(WriteMode::Replace {
                            error_if_absent: false,
                        });
                }
            },
            SaveType::Table {
                table,
                save_method: TableSaveMethod::InsertInto,
            } => {
                let mode = match mode {
                    Some(SaveMode::Overwrite) => WriteMode::Truncate,
                    _ => WriteMode::Append {
                        error_if_absent: true,
                    },
                };
                builder = builder
                    .with_target(WriteTarget::Table {
                        table,
                        column_match: WriteColumnMatch::ByPosition,
                    })
                    .with_mode(mode);
            }
        };
        self.resolve_write_with_builder(input, builder, state).await
    }
}
