use datafusion_expr::LogicalPlan;
use sail_common::spec;

use crate::error::{PlanError, PlanResult};
use crate::resolver::command::write::{WriteMode, WriteSpec, WriteTarget};
use crate::resolver::state::PlanResolverState;
use crate::resolver::PlanResolver;

impl PlanResolver<'_> {
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn resolve_command_insert_into(
        &self,
        input: spec::QueryPlan,
        table: spec::ObjectName,
        columns: spec::WriteColumns,
        partition_spec: Vec<(spec::Identifier, Option<spec::Expr>)>,
        replace: Option<spec::Expr>,
        if_not_exists: bool,
        overwrite: bool,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let mode =
            match (replace, if_not_exists, overwrite) {
                (Some(condition), false, false) => WriteMode::OverwriteIf {
                    condition: Box::new(condition),
                },
                (Some(_), _, _) => return Err(PlanError::invalid(
                    "INSERT with REPLACE condition cannot be used with IF NOT EXISTS or OVERWRITE",
                )),
                (None, true, false) => WriteMode::IgnoreIfExists,
                (None, true, true) => {
                    return Err(PlanError::invalid(
                        "INSERT with IF NOT EXISTS cannot be used with OVERWRITE",
                    ))
                }
                (None, false, true) => WriteMode::Overwrite,
                (None, false, false) => WriteMode::ErrorIfExists,
            };
        let write = WriteSpec {
            input: Box::new(input),
            partition: partition_spec,
            format: None,
            target: WriteTarget::Table(table),
            mode,
            columns,
            partition_by: vec![],
            bucket_by: None,
            sort_by: vec![],
            cluster_by: vec![],
            options: vec![],
            table_properties: vec![],
        };
        self.resolve_write_spec(write, state).await
    }
}
