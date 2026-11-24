// SPDX-License-Identifier: Apache-2.0

//! The requirements of the Delta protocol for writers of clustered tables, which are the
//! tables with the `clustering` writer feature.
//!
//! Sail does not cluster data. The protocol does not require a writer to cluster, so Sail
//! adds files without a clustering provider, which a clustering implementation may cluster
//! later. What a writer must do is keep the clustering columns in the `delta.clustering`
//! metadata domain, write statistics for the clustering columns, and never partition the
//! table. The clustering columns cannot be changed with Sail.
//!
//! See <https://github.com/delta-io/delta/blob/master/PROTOCOL.md#clustered-table>.

use std::collections::HashMap;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use super::row_tracking::effective_protocol_and_metadata;
use crate::schema::column_mapping_mode;
use crate::spec::{
    ColumnMappingMode, ColumnName, CommitAction, DataType, DeltaError, DeltaOperation, DeltaResult,
    DomainMetadata, Protocol, SaveMode, StructType, TableFeature,
};
use crate::table::DeltaSnapshot;

pub(crate) const CLUSTERING_DOMAIN: &str = "delta.clustering";

/// The configuration of the `delta.clustering` metadata domain.
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ClusteringDomain {
    /// The path of each clustering column, with physical column names.
    clustering_columns: Vec<Vec<String>>,
}

fn no_clustering_columns() -> DeltaResult<DomainMetadata> {
    Ok(DomainMetadata {
        domain: CLUSTERING_DOMAIN.to_string(),
        configuration: serde_json::to_string(&ClusteringDomain {
            clustering_columns: vec![],
        })?,
        removed: false,
    })
}

/// Returns the paths of the clustering columns of a table with the given protocol and
/// metadata domains, with physical column names. A table that is not clustered has none.
pub(crate) fn clustering_columns(
    protocol: &Protocol,
    domain_metadata: &HashMap<String, DomainMetadata>,
) -> DeltaResult<Vec<ColumnName>> {
    if !protocol.has_writer_feature(&TableFeature::Clustering) {
        return Ok(vec![]);
    }
    match domain_metadata.get(CLUSTERING_DOMAIN) {
        Some(domain) if !domain.removed => {
            let domain: ClusteringDomain = serde_json::from_str(&domain.configuration)?;
            Ok(domain
                .clustering_columns
                .into_iter()
                .map(ColumnName::new)
                .collect())
        }
        _ => Ok(vec![]),
    }
}

/// Whether the schema has a column at the path of physical column names.
fn has_column(schema: &StructType, mode: ColumnMappingMode, column: &ColumnName) -> bool {
    let mut current = schema;
    let path = column.path();
    for (index, segment) in path.iter().enumerate() {
        let Some(field) = current
            .fields()
            .find(|field| field.physical_name(mode).eq_ignore_ascii_case(segment))
        else {
            return false;
        };
        if index + 1 < path.len() {
            let DataType::Struct(children) = field.data_type() else {
                return false;
            };
            current = children;
        }
    }
    !path.is_empty()
}

/// Keeps the table that a commit results in a valid clustered table, for each commit
/// attempt on the latest snapshot of the table.
///
/// A table that becomes a clustered table gets the `delta.clustering` metadata domain
/// without clustering columns. A commit that replaces the table, and whose schema does not
/// have a clustering column, leaves the table without clustering columns, as a table that
/// is replaced without `CLUSTER BY` in Delta Spark. Any other commit must keep the
/// clustering columns, and no commit can partition the table.
pub(super) fn finalize_clustering(
    actions: &mut Vec<CommitAction>,
    read_snapshot: Option<&Arc<DeltaSnapshot>>,
    operation: &DeltaOperation,
) -> DeltaResult<()> {
    let Some((protocol, metadata)) = effective_protocol_and_metadata(actions, read_snapshot) else {
        return Ok(());
    };
    if !protocol.has_writer_feature(&TableFeature::Clustering) {
        return Ok(());
    }
    if !metadata.partition_columns().is_empty() {
        return Err(DeltaError::Unsupported(
            "A Delta table with the clustering table feature cannot be partitioned".to_string(),
        ));
    }
    if actions.iter().any(
        |action| matches!(action, CommitAction::DomainMetadata(x) if x.domain == CLUSTERING_DOMAIN),
    ) {
        return Ok(());
    }
    let clustered = read_snapshot.filter(|snapshot| {
        snapshot
            .protocol()
            .has_writer_feature(&TableFeature::Clustering)
    });
    let Some(snapshot) = clustered else {
        if read_snapshot.is_some() && !replaces_table(operation) {
            return Err(DeltaError::Unsupported(
                "Enabling the clustering table feature on an existing Delta table is not supported"
                    .to_string(),
            ));
        }
        actions.push(CommitAction::DomainMetadata(no_clustering_columns()?));
        return Ok(());
    };
    // The schema changes only with a commit that has a metadata action.
    if !actions
        .iter()
        .any(|action| matches!(action, CommitAction::Metadata(_)))
    {
        return Ok(());
    }
    let schema = metadata.parse_schema()?;
    let mode = column_mapping_mode(metadata.configuration());
    let missing = clustering_columns(snapshot.protocol(), snapshot.domain_metadata())?
        .into_iter()
        .filter(|column| !has_column(&schema, mode, column))
        .map(|column| column.path().join("."))
        .collect::<Vec<_>>();
    if missing.is_empty() {
        return Ok(());
    }
    if !replaces_table(operation) {
        return Err(DeltaError::Unsupported(format!(
            "Removing the clustering columns ({}) of a Delta table is not supported",
            missing.join(", ")
        )));
    }
    actions.push(CommitAction::DomainMetadata(no_clustering_columns()?));
    Ok(())
}

/// Whether the operation replaces the table, or overwrites the table as a whole.
fn replaces_table(operation: &DeltaOperation) -> bool {
    matches!(
        operation,
        DeltaOperation::Create {
            mode: SaveMode::Overwrite,
            ..
        } | DeltaOperation::Write {
            mode: SaveMode::Overwrite,
            predicate: None,
            ..
        }
    )
}
