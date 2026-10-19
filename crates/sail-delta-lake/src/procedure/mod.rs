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
use crate::metadata_relation::{load_state, memory_source, record_batch};
use crate::spec::inspection::{
    DeltaMetadataRelationType, DeltaProcedureType, detail_schema, history_schema, optimize_schema,
    vacuum_schema,
};

mod maintenance;
mod optimize;
mod vacuum;
pub(crate) use maintenance::DeltaMaintenanceNode;
pub use maintenance::{DeltaMaintenanceExec, DeltaMaintenancePredicate};

#[async_trait]
impl LakeProcedureProvider for DeltaLakeSource {
    fn resolve_procedure(&self, namespace: &[String], name: &str) -> LakeProcedureResolution {
        // A format-owned namespace keeps overlapping utility names independent of provider order.
        if namespace.len() != 1 || !namespace[0].eq_ignore_ascii_case("delta") {
            return LakeProcedureResolution::Unrecognized;
        }
        let Some(kind) = DeltaProcedureType::parse(name) else {
            return LakeProcedureResolution::Unrecognized;
        };
        match descriptor(kind) {
            Some(procedure) => LakeProcedureResolution::Supported(procedure),
            None => LakeProcedureResolution::Unsupported {
                reason: kind.unsupported_reason(),
            },
        }
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
                return not_impl_err!("OPTIMIZE FULL and ZORDER are not implemented");
            }
            if optional_string(&call.invocation, "mode")?
                .is_some_and(|mode| !mode.eq_ignore_ascii_case("FULL"))
            {
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
        let kind = match DeltaProcedureType::parse(&call.invocation.procedure.name) {
            Some(DeltaProcedureType::DescribeDetail) => DeltaMetadataRelationType::Detail,
            Some(DeltaProcedureType::DescribeHistory) => DeltaMetadataRelationType::History,
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
        if kind == DeltaMetadataRelationType::History {
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

fn descriptor(kind: DeltaProcedureType) -> Option<LakeProcedure> {
    let mut parameters = vec![LakeProcedureParameter::required(
        "table",
        LakeProcedureDataType::Utf8,
    )];
    let schema = match kind {
        DeltaProcedureType::DescribeDetail => detail_schema(),
        DeltaProcedureType::DescribeHistory => {
            parameters.push(LakeProcedureParameter::optional(
                "limit",
                LakeProcedureDataType::Int32,
            ));
            history_schema()
        }
        DeltaProcedureType::Optimize => {
            parameters.extend([
                LakeProcedureParameter::optional("predicate", LakeProcedureDataType::SqlPredicate),
                LakeProcedureParameter::optional("full", LakeProcedureDataType::Boolean),
                LakeProcedureParameter::optional("zorder", LakeProcedureDataType::Utf8),
            ]);
            optimize_schema()
        }
        DeltaProcedureType::Vacuum => {
            parameters.extend([
                LakeProcedureParameter::optional("retention_hours", LakeProcedureDataType::Utf8),
                LakeProcedureParameter::optional("dry_run", LakeProcedureDataType::Boolean),
                LakeProcedureParameter::optional("mode", LakeProcedureDataType::Utf8),
            ]);
            vacuum_schema()
        }
        _ => return None,
    };
    Some(LakeProcedure {
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
        access: if matches!(
            kind,
            DeltaProcedureType::Optimize | DeltaProcedureType::Vacuum
        ) {
            LakeProcedureAccess::MetadataCommit
        } else {
            LakeProcedureAccess::MetadataRead
        },
        target: LakeProcedureTarget::TableOrPath {
            parameter: "table".to_string(),
        },
    })
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
