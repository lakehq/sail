use datafusion_expr::LogicalPlan;
use sail_common::spec;

use crate::error::PlanResult;
use crate::resolver::command::write::{
    WriteColumnMatch, WriteMode, WritePlanBuilder, WriteTableAction, WriteTarget,
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
                    Some(SaveMode::ErrorIfExists) | None => WriteMode::ErrorIfExists,
                    Some(SaveMode::IgnoreIfExists) => WriteMode::IgnoreIfExists,
                    Some(SaveMode::Append) => WriteMode::Append,
                    Some(SaveMode::Overwrite) => WriteMode::Overwrite,
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
                        .with_mode(WriteMode::ErrorIfExists);
                }
                Some(SaveMode::IgnoreIfExists) => {
                    builder = builder
                        .with_target(WriteTarget::NewTable {
                            table,
                            action: WriteTableAction::Create,
                        })
                        .with_mode(WriteMode::IgnoreIfExists);
                }
                Some(SaveMode::Append) => {
                    builder = builder
                        .with_target(WriteTarget::ExistingTable {
                            table,
                            column_match: WriteColumnMatch::ByName,
                        })
                        .with_mode(WriteMode::Append);
                }
                Some(SaveMode::Overwrite) => {
                    builder = builder
                        .with_target(WriteTarget::NewTable {
                            table,
                            action: WriteTableAction::CreateOrReplace,
                        })
                        .with_mode(WriteMode::Overwrite);
                }
            },
            SaveType::Table {
                table,
                save_method: TableSaveMethod::InsertInto,
            } => {
                let mode = match mode {
                    Some(SaveMode::Overwrite) => WriteMode::Overwrite,
                    _ => WriteMode::Append,
                };
                builder = builder
                    .with_target(WriteTarget::ExistingTable {
                        table,
                        column_match: WriteColumnMatch::ByPosition,
                    })
                    .with_mode(mode);
            }
        };
        self.resolve_write_with_builder(input, builder, state).await
    }
}
