use datafusion_expr::LogicalPlan;
use sail_common::spec;

use crate::error::{PlanError, PlanResult};
use crate::resolver::command::write::{WriteMode, WritePlanBuilder, WriteTableAction, WriteTarget};
use crate::resolver::state::PlanResolverState;
use crate::resolver::PlanResolver;

impl PlanResolver<'_> {
    /// Resolves the write operation for the Spark streaming query API.
    pub(super) async fn resolve_command_write_stream(
        &self,
        write_stream: spec::WriteStream,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        use spec::WriteStreamSinkDestination;

        let spec::WriteStream {
            input,
            format,
            options,
            partitioning_column_names,
            query_name: _,
            foreach_writer,
            foreach_batch,
            clustering_column_names,
            sink_destination,
        } = write_stream;
        if foreach_writer.is_some() {
            return Err(PlanError::todo("foreach sink in write stream"));
        }
        if foreach_batch.is_some() {
            return Err(PlanError::unsupported(
                "foreach batch is not supported in write stream",
            ));
        }
        let input = self.resolve_write_input(*input, state).await?;
        let clustering_columns = self.resolve_write_cluster_by_columns(clustering_column_names)?;
        let mut builder = WritePlanBuilder::new()
            .with_partition_by(partitioning_column_names)
            .with_cluster_by(clustering_columns)
            .with_format(format)
            .with_options(options)
            .with_mode(WriteMode::Append);
        match sink_destination {
            None => {
                builder = builder.with_target(WriteTarget::Sink);
            }
            Some(WriteStreamSinkDestination::Path { path }) => {
                builder = builder.with_target(WriteTarget::Path { location: path });
            }
            Some(WriteStreamSinkDestination::Table { table }) => {
                builder = builder.with_target(WriteTarget::NewTable {
                    table,
                    action: WriteTableAction::CreateIfNotExists,
                })
            }
        }
        self.resolve_write_with_builder(input, builder, state).await
    }
}
