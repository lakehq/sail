//! Iceberg system procedures.

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::RecordBatch;
use datafusion::catalog::Session;
use datafusion::common::{Result, not_impl_err, plan_err};
use datafusion::execution::TaskContext;
use datafusion_expr::{Extension, LogicalPlan};
use sail_common_datafusion::lakeprocedure::{
    LakeProcedureAccess, LakeProcedureCall, LakeProcedureInvocation, LakeProcedurePlan,
    LakeProcedurePlanningTarget, LakeProcedureProvider, LakeProcedureResolution,
};

mod arguments;
mod descriptor;
mod logical;
mod rewrite_data_files;
mod snapshot;
pub(crate) mod table;

use arguments::{
    optional_i64, optional_string, required_i64, required_string, required_timestamp_micros,
};
use descriptor::IcebergProcedureType;
pub(crate) use logical::IcebergProcedureNode;
pub use rewrite_data_files::RewriteDataFilesPlan;
pub(crate) use rewrite_data_files::RewriteDataFilesScanNode;
use rewrite_data_files::plan_rewrite_data_files;
use snapshot::{SnapshotOperation, ancestors_output, commit_snapshot_operation};
use table::{ProcedureTable, load_current_metadata};

use crate::lake_source::IcebergLakeSource;

#[async_trait]
impl LakeProcedureProvider for IcebergLakeSource {
    fn resolve_procedure(&self, namespace: &[String], name: &str) -> LakeProcedureResolution {
        if namespace.len() != 1 || !namespace[0].eq_ignore_ascii_case("system") {
            return LakeProcedureResolution::Unrecognized;
        }
        let Some(procedure_type) = IcebergProcedureType::parse(name) else {
            return LakeProcedureResolution::Unrecognized;
        };
        if let Some(procedure) = procedure_type.descriptor() {
            return LakeProcedureResolution::Supported(procedure);
        }
        LakeProcedureResolution::Unsupported {
            reason: procedure_type.unsupported_reason(),
        }
    }

    async fn plan_procedure(
        &self,
        session: &dyn Session,
        target: LakeProcedurePlanningTarget,
        call: &LakeProcedureCall,
    ) -> Result<LakeProcedurePlan> {
        let procedure_type = IcebergProcedureType::parse(&call.invocation.procedure.name)
            .ok_or_else(|| {
                datafusion::common::DataFusionError::Plan(format!(
                    "Unknown Iceberg system procedure: {}",
                    call.invocation.procedure.name
                ))
            })?;
        if matches!(procedure_type, IcebergProcedureType::RewriteDataFiles) {
            let LakeProcedurePlanningTarget::Table(info) = target else {
                return plan_err!("Iceberg system procedures require a table target");
            };
            let (worker_plan, rewrite_plan) = plan_rewrite_data_files(session, *info, call).await?;
            let implementation = LogicalPlan::Extension(Extension {
                node: Arc::new(IcebergProcedureNode::try_new_rewrite_data_files(
                    call.clone(),
                    worker_plan,
                    rewrite_plan,
                )?),
            });
            return Ok(LakeProcedurePlan::coordinator(implementation));
        }
        let planned_table = match target {
            LakeProcedurePlanningTarget::Table(info) => {
                let table = ProcedureTable::from_source_info(*info).await?;
                match call.invocation.procedure.access {
                    LakeProcedureAccess::MetadataRead => Some(table),
                    LakeProcedureAccess::MetadataCommit => None,
                }
            }
            LakeProcedurePlanningTarget::Catalog { .. } => {
                return plan_err!("Iceberg system procedures require a table target");
            }
        };
        let implementation = LogicalPlan::Extension(Extension {
            node: Arc::new(IcebergProcedureNode::try_new(call.clone(), planned_table)?),
        });
        Ok(match call.invocation.procedure.access {
            LakeProcedureAccess::MetadataRead => LakeProcedurePlan::distributed(implementation),
            LakeProcedureAccess::MetadataCommit => LakeProcedurePlan::coordinator(implementation),
        })
    }
}

pub(crate) async fn execute_iceberg_procedure(
    ctx: &TaskContext,
    table: ProcedureTable,
    invocation: LakeProcedureInvocation,
) -> Result<RecordBatch> {
    let ProcedureTable {
        table_location: _,
        table_properties,
        lakehouse_table,
    } = &table;
    let table_url = table.table_url().await?;
    let Some(procedure_type) = IcebergProcedureType::parse(&invocation.procedure.name) else {
        return not_impl_err!(
            "Iceberg system procedure '{}' is not implemented",
            invocation.procedure.name
        );
    };
    match procedure_type {
        IcebergProcedureType::AncestorsOf => {
            let metadata =
                load_current_metadata(ctx, &table_url, table_properties, lakehouse_table.as_ref())
                    .await?;
            ancestors_output(&metadata, &invocation)
        }
        IcebergProcedureType::RollbackToSnapshot => {
            let snapshot_id = required_i64(&invocation, "snapshot_id")?;
            commit_snapshot_operation(
                ctx,
                &table_url,
                table_properties,
                lakehouse_table.as_ref(),
                SnapshotOperation::RollbackToSnapshot(snapshot_id),
                invocation.procedure.schema(),
            )
            .await
        }
        IcebergProcedureType::RollbackToTimestamp => {
            let timestamp_micros = required_timestamp_micros(&invocation, "timestamp")?;
            commit_snapshot_operation(
                ctx,
                &table_url,
                table_properties,
                lakehouse_table.as_ref(),
                SnapshotOperation::RollbackToTimestamp(timestamp_micros.div_euclid(1_000)),
                invocation.procedure.schema(),
            )
            .await
        }
        IcebergProcedureType::SetCurrentSnapshot => {
            let snapshot_id = optional_i64(&invocation, "snapshot_id")?;
            let reference = optional_string(&invocation, "ref")?;
            if snapshot_id.is_some() == reference.is_some() {
                return plan_err!(
                    "Exactly one of snapshot_id or ref must be provided to set_current_snapshot"
                );
            }
            commit_snapshot_operation(
                ctx,
                &table_url,
                table_properties,
                lakehouse_table.as_ref(),
                SnapshotOperation::SetCurrentSnapshot {
                    snapshot_id,
                    reference,
                },
                invocation.procedure.schema(),
            )
            .await
        }
        IcebergProcedureType::FastForward => {
            let branch = required_string(&invocation, "branch")?;
            let to = required_string(&invocation, "to")?;
            commit_snapshot_operation(
                ctx,
                &table_url,
                table_properties,
                lakehouse_table.as_ref(),
                SnapshotOperation::FastForward { branch, to },
                invocation.procedure.schema(),
            )
            .await
        }
        unsupported => not_impl_err!(
            "Iceberg system procedure '{}' is not implemented",
            unsupported.name()
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recognized_procedures_are_distinct_from_unknown_procedures() {
        let source = IcebergLakeSource;
        let namespace = vec!["system".to_string()];
        assert!(matches!(
            source.resolve_procedure(&namespace, "rollback_to_snapshot"),
            LakeProcedureResolution::Supported(_)
        ));
        assert!(matches!(
            source.resolve_procedure(&namespace, "expire_snapshots"),
            LakeProcedureResolution::Unsupported { .. }
        ));
        assert_eq!(
            source.resolve_procedure(&namespace, "not_an_iceberg_procedure"),
            LakeProcedureResolution::Unrecognized
        );
        assert_eq!(
            source.resolve_procedure(&["maintenance".to_string()], "rollback_to_snapshot"),
            LakeProcedureResolution::Unrecognized
        );
    }

    #[test]
    fn recognizes_all_iceberg_procedure_names_case_insensitively() {
        let names = [
            "rollback_to_snapshot",
            "rollback_to_timestamp",
            "set_current_snapshot",
            "cherrypick_snapshot",
            "rewrite_data_files",
            "rewrite_manifests",
            "remove_orphan_files",
            "expire_snapshots",
            "migrate",
            "snapshot",
            "add_files",
            "ancestors_of",
            "register_table",
            "publish_changes",
            "create_changelog_view",
            "rewrite_position_delete_files",
            "fast_forward",
            "compute_table_stats",
            "compute_partition_stats",
            "rewrite_table_path",
        ];
        assert_eq!(
            IcebergProcedureType::ALL
                .iter()
                .copied()
                .map(IcebergProcedureType::name)
                .collect::<Vec<_>>(),
            names
        );
        for name in names {
            assert_eq!(
                IcebergProcedureType::parse(&name.to_ascii_uppercase())
                    .map(IcebergProcedureType::name),
                Some(name)
            );
        }
        assert_eq!(IcebergProcedureType::parse("unknown_procedure"), None);
    }

    #[test]
    fn descriptors_are_available_only_for_supported_procedures() {
        let supported = IcebergProcedureType::ALL
            .iter()
            .copied()
            .filter(|procedure_type| procedure_type.descriptor().is_some())
            .map(IcebergProcedureType::name)
            .collect::<Vec<_>>();
        assert_eq!(
            supported,
            [
                "rollback_to_snapshot",
                "rollback_to_timestamp",
                "set_current_snapshot",
                "rewrite_data_files",
                "ancestors_of",
                "fast_forward",
            ]
        );
    }
}
