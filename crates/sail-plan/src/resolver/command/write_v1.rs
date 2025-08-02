use datafusion_expr::LogicalPlan;
use sail_common::spec;
use sail_common_datafusion::datasource::SinkMode;

use crate::error::PlanResult;
use crate::resolver::command::write::{
    WriteColumnMatch, WritePlanBuilder, WriteTableAction, WriteTarget,
};
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

        let input = self.resolve_write_input(*input, state).await?;
        let clustering_columns = self.resolve_write_cluster_by_columns(clustering_columns)?;

        let mut builder = WritePlanBuilder::new()
            .with_partition_by(partitioning_columns)
            .with_bucket_by(bucket_by)
            .with_sort_by(sort_columns)
            .with_cluster_by(clustering_columns)
            .with_options(options);
        if let Some(source) = source {
            builder = builder.with_format(source);
        }
        match save_type {
            SaveType::Path(location) => {
                let mode = match mode {
                    Some(SaveMode::ErrorIfExists) | None => SinkMode::ErrorIfExists,
                    Some(SaveMode::IgnoreIfExists) => SinkMode::IgnoreIfExists,
                    Some(SaveMode::Append) => SinkMode::Append,
                    Some(SaveMode::Overwrite) => SinkMode::Overwrite,
                };
                builder = builder
                    .with_target(WriteTarget::Path { location })
                    .with_mode(mode);
            }
            SaveType::Table {
                table,
                save_method: TableSaveMethod::SaveAsTable,
            } => match mode {
                Some(SaveMode::ErrorIfExists) | None => {
                    builder = builder
                        .with_target(WriteTarget::NewTable {
                            table,
                            action: WriteTableAction::Create,
                        })
                        .with_mode(SinkMode::ErrorIfExists);
                }
                Some(SaveMode::IgnoreIfExists) => {
                    builder = builder
                        .with_target(WriteTarget::NewTable {
                            table,
                            action: WriteTableAction::Create,
                        })
                        .with_mode(SinkMode::IgnoreIfExists);
                }
                Some(SaveMode::Append) => {
                    builder = builder.with_mode(SinkMode::Append).with_target(
                        WriteTarget::ExistingTable {
                            table,
                            column_match: WriteColumnMatch::ByName,
                        },
                    );
                }
                Some(SaveMode::Overwrite) => {
                    builder =
                        builder
                            .with_mode(SinkMode::Overwrite)
                            .with_target(WriteTarget::NewTable {
                                table,
                                action: WriteTableAction::CreateOrReplace,
                            });
                }
            },
            SaveType::Table {
                table,
                save_method: TableSaveMethod::InsertInto,
            } => {
                let mode = match mode {
                    Some(SaveMode::Overwrite) => SinkMode::Overwrite,
                    _ => SinkMode::Append,
                };
                builder = builder
                    .with_mode(mode)
                    .with_target(WriteTarget::ExistingTable {
                        table,
                        column_match: WriteColumnMatch::ByPosition,
                    });
            }
        };
        self.resolve_write_with_builder(input, builder, state).await
    }
}
