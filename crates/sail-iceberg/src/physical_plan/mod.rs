// SPDX-License-Identifier: Apache-2.0

pub mod action_schema;
pub mod commit;
pub mod delete_apply_exec;
mod delete_writer_common;
pub mod discovery_exec;
pub mod equality_delete_writer_exec;
pub mod file_tasks_exec;
pub mod manifest_scan_exec;
pub mod merge_metadata_exec;
pub(crate) mod merge_row_projection;
pub mod metadata_relation_exec;
pub mod metadata_scan_exec;
pub mod partition_transform_expr;
pub mod plan_builder;
mod position_delete_writer;
pub mod procedure_exec;
mod rewrite_exec;
pub mod scan_by_data_files_exec;
pub(crate) mod write_context;
mod write_location;
mod writer_exec;
mod writer_options;

pub use commit::commit_exec::IcebergCommitExec;
pub use delete_apply_exec::IcebergDeleteApplyExec;
pub use discovery_exec::IcebergDiscoveryExec;
pub use equality_delete_writer_exec::IcebergEqualityDeleteWriterExec;
pub use file_tasks_exec::IcebergFileTasksExec;
pub use manifest_scan_exec::IcebergManifestScanExec;
pub use merge_metadata_exec::IcebergMergeMetadataExec;
pub use metadata_relation_exec::IcebergMetadataRelationExec;
pub use metadata_scan_exec::IcebergMetadataScanExec;
pub use partition_transform_expr::IcebergPartitionTransformExpr;
pub use plan_builder::{IcebergPlanBuilder, IcebergTableConfig};
pub use procedure_exec::{IcebergProcedureExec, validate_iceberg_procedure_call_identity};
pub use rewrite_exec::IcebergRewriteExec;
pub use scan_by_data_files_exec::IcebergScanByDataFilesExec;
pub use write_context::{
    IcebergBaseWriteContext, IcebergWriteContext, prepare_iceberg_write_context,
};
pub use writer_exec::IcebergWriterExec;
pub use writer_options::IcebergWriterExecOptions;

pub use crate::row_lineage::RowLineage;
