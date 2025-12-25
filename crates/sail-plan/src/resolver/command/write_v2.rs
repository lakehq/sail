use datafusion_expr::LogicalPlan;
use sail_common::spec;
use sail_common_datafusion::utils::items::ItemTaker;

use crate::error::{PlanError, PlanResult};
use crate::resolver::command::write::{
    WriteColumnMatch, WriteMode, WritePlanBuilder, WriteTableAction, WriteTarget,
};
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
                    .with_target(WriteTarget::ExistingTable {
                        table,
                        column_match: WriteColumnMatch::ByName,
                    })
                    .with_mode(WriteMode::Append);
            }
            WriteToMode::Create => {
                builder = builder
                    .with_target(WriteTarget::NewTable {
                        table,
                        action: WriteTableAction::Create,
                    })
                    .with_mode(WriteMode::Overwrite);
            }
            WriteToMode::CreateOrReplace => {
                builder = builder
                    .with_target(WriteTarget::NewTable {
                        table,
                        action: WriteTableAction::CreateOrReplace,
                    })
                    .with_mode(WriteMode::Overwrite);
            }
            WriteToMode::Overwrite { condition } => {
                builder = builder
                    .with_target(WriteTarget::ExistingTable {
                        table,
                        column_match: WriteColumnMatch::ByName,
                    })
                    .with_mode(WriteMode::OverwriteIf {
                        condition: Box::new(spec::ExprWithSource {
                            expr: *condition,
                            source: None,
                        }),
                    });
            }
            WriteToMode::OverwritePartitions => {
                builder = builder
                    .with_target(WriteTarget::ExistingTable {
                        table,
                        column_match: WriteColumnMatch::ByName,
                    })
                    .with_mode(WriteMode::OverwritePartitions);
            }
            WriteToMode::Replace => {
                builder = builder
                    .with_target(WriteTarget::NewTable {
                        table,
                        action: WriteTableAction::Replace,
                    })
                    .with_mode(WriteMode::Overwrite);
            }
        };
        self.resolve_write_with_builder(input, builder, state).await
    }

    fn resolve_write_partition_by_expressions(
        &self,
        partition_by: Vec<spec::Expr>,
    ) -> PlanResult<Vec<spec::Identifier>> {
        // TODO: support functions for partitioning columns
        partition_by
            .into_iter()
            .map(|x| match x {
                spec::Expr::UnresolvedAttribute {
                    name,
                    plan_id: None,
                    is_metadata_column: false,
                } => {
                    let name: Vec<String> = name.into();
                    Ok(name.one()?.into())
                }
                _ => Err(PlanError::invalid(
                    "partitioning column must be a column reference",
                )),
            })
            .collect()
    }
}
