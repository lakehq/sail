use datafusion_expr::LogicalPlan;
use sail_common::spec;
use sail_common_datafusion::datasource::SinkMode;

use crate::error::PlanResult;
use crate::resolver::command::write::WritePlanBuilder;
use crate::resolver::state::PlanResolverState;
use crate::resolver::PlanResolver;

impl PlanResolver<'_> {
    pub(super) async fn resolve_command_insert_into(
        &self,
        input: spec::QueryPlan,
        table: spec::ObjectName,
        mode: spec::InsertMode,
        partition: Vec<(spec::Identifier, Option<spec::Expr>)>,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        use spec::InsertMode;

        let input = self.resolve_write_input(input, state).await?;

        let mut builder = WritePlanBuilder::new().with_partition(partition);
        match mode {
            InsertMode::InsertByPosition { overwrite } => {
                if overwrite {
                    builder = builder.with_mode(SinkMode::Overwrite);
                } else {
                    builder = builder.with_mode(SinkMode::Append);
                }
            }
            InsertMode::InsertByName { overwrite } => {
                if overwrite {
                    builder = builder.with_mode(SinkMode::Overwrite);
                } else {
                    builder = builder.with_mode(SinkMode::Append);
                }
            }
            InsertMode::InsertByColumns {
                columns: _,
                overwrite,
            } => {
                if overwrite {
                    builder = builder.with_mode(SinkMode::Overwrite);
                } else {
                    builder = builder.with_mode(SinkMode::Append);
                }
            }
            InsertMode::Replace { condition } => {
                let condition = self
                    .resolve_expression(*condition, input.schema(), state)
                    .await?;
                builder = builder.with_mode(SinkMode::OverwriteIf {
                    condition: Box::new(condition),
                });
            }
        };
        builder = builder.with_existing_table_target(table);

        self.resolve_write_with_builder(input, builder, state).await
    }
}
