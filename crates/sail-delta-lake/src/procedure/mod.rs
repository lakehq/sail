//! Native Delta Spark commands reuse the shared procedure execution machinery.
//!
//! TODO: Consider Sail-only `CALL delta.describe_detail(...)`, `CALL delta.describe_history(...)`,
//! `CALL delta.optimize(...)`, and `CALL delta.vacuum(...)` aliases for the native SQL commands.
//! TODO: Consider `CALL delta.checkpoint(...)` and `CALL delta.compact_log(...)` for explicit
//! log maintenance, `CALL delta.upgrade_table_protocol(...)` for validated protocol upgrades,
//! and `CALL delta.add_feature_support(...)` / `CALL delta.drop_feature(...)` for feature changes.
//! TODO: Implement native RESTORE, CONVERT TO DELTA, GENERATE, SHALLOW CLONE, and REORG before
//! considering `CALL delta.restore(...)`, `CALL delta.convert_to_delta(...)`,
//! `CALL delta.generate(...)`, `CALL delta.clone(...)`, and `CALL delta.reorg(...)` aliases.
//! Native ALTER TABLE DROP FEATURE also needs feature-removal and history-protection support.
//! All CALL aliases above are intentionally deferred and are not registered.

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::{Column, Result, not_impl_err, plan_err};
use datafusion_expr::{Expr, Extension, LogicalPlan, LogicalPlanBuilder, col};
use sail_common_datafusion::lakeprocedure::{
    LakeProcedure, LakeProcedureAccess, LakeProcedureCall, LakeProcedureDataType,
    LakeProcedureField, LakeProcedureParameter, LakeProcedurePlan, LakeProcedurePlanningTarget,
    LakeProcedureProvider, LakeProcedureResolution, LakeProcedureTarget, LakeProcedureValue,
};

use crate::DeltaLakeSource;
use crate::inspection::{load_state, memory_source, record_batch};
use crate::spec::inspection::{
    DeltaCommandType, DeltaInspectionType, detail_schema, history_schema, optimize_schema,
    vacuum_schema,
};

mod maintenance;
pub(crate) mod optimize;
mod rewrite_exec;
mod vacuum;
pub(crate) use maintenance::DeltaMaintenanceNode;
pub use maintenance::{DeltaMaintenanceExec, DeltaMaintenancePredicate};
pub use optimize::DeltaOptimizePlan;
pub use rewrite_exec::DeltaOptimizeRewriteExec;

#[async_trait]
impl LakeProcedureProvider for DeltaLakeSource {
    fn supports_sql_call(&self) -> bool {
        false
    }

    fn resolve_procedure(&self, namespace: &[String], name: &str) -> LakeProcedureResolution {
        // A format-owned namespace keeps overlapping utility names independent of provider order.
        if namespace.len() != 1 || !namespace[0].eq_ignore_ascii_case("delta") {
            return LakeProcedureResolution::Unrecognized;
        }
        let Some(kind) = DeltaCommandType::parse(name) else {
            return LakeProcedureResolution::Unrecognized;
        };
        LakeProcedureResolution::Supported(descriptor(kind))
    }

    async fn plan_procedure(
        &self,
        session: &dyn Session,
        target: LakeProcedurePlanningTarget,
        call: &LakeProcedureCall,
    ) -> Result<LakeProcedurePlan> {
        call.validate()?;
        if matches!(
            call.invocation.procedure.name.as_str(),
            "optimize" | "vacuum"
        ) {
            let LakeProcedurePlanningTarget::Table { info, predicates } = target else {
                return plan_err!("Delta maintenance requires a table");
            };
            if optional_bool(&call.invocation, "full")?.unwrap_or(false)
                || optional_string(&call.invocation, "zorder")?.is_some()
            {
                // TODO: Plan OPTIMIZE FULL and ZORDER rewrites before accepting these options.
                return not_impl_err!("OPTIMIZE FULL and ZORDER are not implemented");
            }
            if optional_string(&call.invocation, "mode")?
                .is_some_and(|mode| !mode.eq_ignore_ascii_case("FULL"))
            {
                // TODO: Add log-based VACUUM LITE discovery with retained-history validation.
                return not_impl_err!("VACUUM currently supports FULL mode only");
            }
            return Ok(LakeProcedurePlan::coordinator(LogicalPlan::Extension(
                Extension {
                    node: Arc::new(DeltaMaintenanceNode::try_new(
                        call.clone(),
                        *info,
                        predicates,
                    )?),
                },
            )));
        }
        let kind = match DeltaCommandType::parse(&call.invocation.procedure.name) {
            Some(DeltaCommandType::DescribeDetail) => DeltaInspectionType::Detail,
            Some(DeltaCommandType::DescribeHistory) => DeltaInspectionType::History,
            _ => {
                return not_impl_err!(
                    "Delta procedure '{}' is not implemented",
                    call.invocation.procedure.name
                );
            }
        };
        let limit = match call.invocation.argument("limit") {
            None | Some(LakeProcedureValue::Null) => None,
            Some(LakeProcedureValue::Int32(limit)) if *limit >= 0 => Some(*limit as usize),
            _ => return plan_err!("Delta history limit must be a non-negative integer"),
        };
        let LakeProcedurePlanningTarget::Table { info, .. } = target else {
            return plan_err!("Delta inspection procedures require a table target");
        };
        let state = load_state(session, *info, kind).await?;
        let source = memory_source(record_batch(&state, kind, limit).await?)?;
        let mut builder = LogicalPlanBuilder::scan("delta_inspection", source, None)?;
        if kind == DeltaInspectionType::History {
            builder = builder.sort(vec![col("version").sort(false, false)])?;
        }
        // The scan qualifier is internal; a procedure's result has unqualified fields.
        builder = builder.project(
            call.invocation
                .procedure
                .output
                .iter()
                .map(|field| Expr::Column(Column::from_name(&field.name)).alias(&field.name))
                .collect::<Vec<_>>(),
        )?;
        Ok(LakeProcedurePlan::distributed(builder.build()?))
    }
}

fn descriptor(kind: DeltaCommandType) -> LakeProcedure {
    let mut parameters = vec![LakeProcedureParameter::required(
        "table",
        LakeProcedureDataType::Utf8,
    )];
    let schema = match kind {
        DeltaCommandType::DescribeDetail => detail_schema(),
        DeltaCommandType::DescribeHistory => {
            parameters.push(LakeProcedureParameter::optional(
                "limit",
                LakeProcedureDataType::Int32,
            ));
            history_schema()
        }
        DeltaCommandType::Optimize => {
            parameters.extend([
                LakeProcedureParameter::optional("predicate", LakeProcedureDataType::SqlPredicate),
                LakeProcedureParameter::optional("full", LakeProcedureDataType::Boolean),
                LakeProcedureParameter::optional("zorder", LakeProcedureDataType::Utf8),
            ]);
            optimize_schema()
        }
        DeltaCommandType::Vacuum => {
            parameters.extend([
                LakeProcedureParameter::optional("retention_hours", LakeProcedureDataType::Utf8),
                LakeProcedureParameter::optional("dry_run", LakeProcedureDataType::Boolean),
                LakeProcedureParameter::optional("mode", LakeProcedureDataType::Utf8),
            ]);
            vacuum_schema()
        }
    };
    LakeProcedure {
        name: kind.name().to_string(),
        parameters,
        output: schema
            .fields()
            .iter()
            .map(|field| {
                LakeProcedureField::new(
                    field.name(),
                    field.data_type().clone(),
                    field.is_nullable(),
                )
            })
            .collect(),
        access: if matches!(kind, DeltaCommandType::Optimize | DeltaCommandType::Vacuum) {
            LakeProcedureAccess::MetadataCommit
        } else {
            LakeProcedureAccess::MetadataRead
        },
        target: LakeProcedureTarget::TableOrPath {
            parameter: "table".to_string(),
        },
    }
}

fn optional_string<'a>(
    invocation: &'a sail_common_datafusion::lakeprocedure::LakeProcedureInvocation,
    name: &str,
) -> Result<Option<&'a str>> {
    match invocation.argument(name) {
        None | Some(LakeProcedureValue::Null) => Ok(None),
        Some(LakeProcedureValue::Utf8(value)) => Ok(Some(value)),
        _ => plan_err!("Delta procedure argument '{name}' must be a string"),
    }
}
fn optional_bool(
    invocation: &sail_common_datafusion::lakeprocedure::LakeProcedureInvocation,
    name: &str,
) -> Result<Option<bool>> {
    match invocation.argument(name) {
        None | Some(LakeProcedureValue::Null) => Ok(None),
        Some(LakeProcedureValue::Boolean(value)) => Ok(Some(*value)),
        _ => plan_err!("Delta procedure argument '{name}' must be a boolean"),
    }
}
