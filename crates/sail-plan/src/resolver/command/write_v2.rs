use datafusion_expr::LogicalPlan;
use sail_common::spec;

use crate::error::PlanResult;
use crate::resolver::command::write::{WriteColumnMatch, WriteMode, WritePlanBuilder, WriteTarget};
use crate::resolver::state::PlanResolverState;
use crate::resolver::PlanResolver;

impl PlanResolver<'_> {
    /// Resolves the write operation for the Spark DataFrameWriter v2 API.
    pub(super) async fn resolve_command_write_to(
        &self,
        write_to: spec::WriteTo,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        use spec::WriteToMode;

        let spec::WriteTo {
            input,
            provider,
            table,
            mode,
            partitioning_columns,
            clustering_columns,
            options,
            table_properties,
        } = write_to;

        let input = self.resolve_write_input(*input, state).await?;
        let partition_by = self.resolve_write_partition_by_expressions(partitioning_columns)?;
        let cluster_by = self.resolve_write_cluster_by_columns(clustering_columns)?;

        let mut builder = WritePlanBuilder::new()
            .with_partition_by(partition_by)
            .with_cluster_by(cluster_by)
            .with_options(options)
            .with_table_properties(table_properties);

        if let Some(provider) = provider {
            builder = builder.with_format(provider);
        }

        match mode {
            WriteToMode::Append => {
                builder = builder
                    .with_target(WriteTarget::Table {
                        table,
                        column_match: WriteColumnMatch::ByName,
                    })
                    .with_mode(WriteMode::Append {
                        error_if_absent: true,
                    });
            }
            WriteToMode::Create => {
                builder = builder
                    .with_target(WriteTarget::Table {
                        table,
                        column_match: WriteColumnMatch::ByName,
                    })
                    .with_mode(WriteMode::ErrorIfExists);
            }
            WriteToMode::CreateOrReplace => {
                builder = builder
                    .with_target(WriteTarget::Table {
                        table,
                        column_match: WriteColumnMatch::ByName,
                    })
                    .with_mode(WriteMode::Replace {
                        error_if_absent: false,
                    });
            }
            WriteToMode::Overwrite { condition } => {
                builder = builder
                    .with_target(WriteTarget::Table {
                        table,
                        column_match: WriteColumnMatch::ByName,
                    })
                    .with_mode(WriteMode::TruncateIf {
                        condition: Box::new(spec::ExprWithSource {
                            expr: *condition,
                            source: None,
                        }),
                    });
            }
            WriteToMode::OverwritePartitions => {
                builder = builder
                    .with_target(WriteTarget::Table {
                        table,
                        column_match: WriteColumnMatch::ByName,
                    })
                    .with_mode(WriteMode::TruncatePartitions);
            }
            WriteToMode::Replace => {
                builder = builder
                    .with_target(WriteTarget::Table {
                        table,
                        column_match: WriteColumnMatch::ByName,
                    })
                    .with_mode(WriteMode::Replace {
                        error_if_absent: true,
                    });
            }
        };
        self.resolve_write_with_builder(input, builder, state).await
    }
}
