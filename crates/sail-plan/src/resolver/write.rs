use datafusion_expr::LogicalPlan;
use sail_common::spec;

use crate::error::{PlanError, PlanResult};
use crate::resolver::state::PlanResolverState;
use crate::resolver::PlanResolver;

#[allow(dead_code)]
pub(super) enum WriteTarget {
    Path(String),
    Table(spec::ObjectName),
}

#[allow(dead_code)]
pub(super) enum WriteMode {
    ErrorIfExists,
    IgnoreIfExists,
    Append,
    CreateOrOverwrite,
    Overwrite,
    OverwriteIf { condition: Box<spec::Expr> },
    OverwritePartitions,
}

#[allow(dead_code)]
pub(super) struct WriteSpec {
    pub input: Box<spec::QueryPlan>,
    pub partition: Vec<(spec::Identifier, spec::Expr)>,
    pub format: Option<String>,
    pub target: WriteTarget,
    pub mode: WriteMode,
    pub columns: spec::WriteColumns,
    pub partition_by: Vec<spec::Identifier>,
    pub bucket_by: Option<spec::SaveBucketBy>,
    pub sort_by: Vec<spec::SortOrder>,
    pub cluster_by: Vec<spec::Identifier>,
    pub options: Vec<(String, String)>,
    pub table_properties: Vec<(String, String)>,
}

impl PlanResolver<'_> {
    pub(super) async fn resolve_write_spec(
        &self,
        _write: WriteSpec,
        _state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        Err(PlanError::todo("resolve write spec"))
    }

    #[allow(dead_code)]
    fn resolve_default_table_location(&self, table: &[String]) -> PlanResult<String> {
        let name = match table.last() {
            Some(x) if !x.is_empty() => x,
            _ => return Err(PlanError::invalid("empty table name")),
        };
        Ok(format!(
            "{}{}{name}",
            self.config.default_warehouse_directory,
            object_store::path::DELIMITER,
        ))
    }
}
