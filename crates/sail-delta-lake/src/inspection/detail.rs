use std::collections::{BTreeSet, HashMap};

use datafusion::arrow::array::RecordBatch;
use serde::{Deserialize, Serialize};

use super::{InspectionState, history, timestamp_micros};
use crate::schema::logical_to_physical_arrow_paths;
use crate::spec::inspection::detail_schema;
use crate::spec::{DeltaError, DeltaResult, TableFeature};

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct DetailRow<'a> {
    format: &'static str,
    id: &'a str,
    name: Option<&'a str>,
    description: Option<&'a str>,
    location: &'a str,
    created_at: Option<i64>,
    last_modified: i64,
    partition_columns: &'a [String],
    clustering_columns: Vec<String>,
    num_files: usize,
    size_in_bytes: i64,
    properties: &'a HashMap<String, String>,
    min_reader_version: i32,
    min_writer_version: i32,
    table_features: BTreeSet<&'a str>,
}

pub(super) async fn batch(state: &InspectionState) -> DeltaResult<RecordBatch> {
    let snapshot = &state.snapshot;
    let metadata = snapshot.metadata();
    let protocol = snapshot.protocol();
    let (reader_features, writer_features) = protocol.table_features_for_upgrade();
    let features = reader_features
        .iter()
        .chain(&writer_features)
        .map(TableFeature::as_str)
        .collect::<BTreeSet<_>>();
    let size = snapshot.adds().iter().try_fold(0_i64, |size, file| {
        size.checked_add(file.size)
            .ok_or_else(|| DeltaError::generic("Delta table size overflow"))
    })?;
    let row = DetailRow {
        format: "delta",
        id: metadata.id(),
        name: state.table_name.as_deref().or(metadata.name()),
        description: metadata.description(),
        location: snapshot.table_url().as_str().trim_end_matches('/'),
        created_at: metadata.created_time().map(timestamp_micros).transpose()?,
        last_modified: history::last_modified(state).await?,
        partition_columns: metadata.partition_columns(),
        clustering_columns: clustering_columns(state)?,
        num_files: snapshot.adds().len(),
        size_in_bytes: size,
        properties: metadata.configuration(),
        min_reader_version: protocol.min_reader_version(),
        min_writer_version: protocol.min_writer_version(),
        table_features: features,
    };
    serde_arrow::to_record_batch(detail_schema().fields(), &[row]).map_err(DeltaError::generic_err)
}

fn clustering_columns(state: &InspectionState) -> DeltaResult<Vec<String>> {
    let snapshot = &state.snapshot;
    if !snapshot
        .protocol()
        .has_writer_feature(&TableFeature::Clustering)
    {
        return Ok(vec![]);
    }
    let Some(domain) = snapshot.domain_metadata().get("delta.clustering") else {
        return Ok(vec![]);
    };
    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Clustering {
        clustering_columns: Vec<Vec<String>>,
    }
    let clustering: Clustering = serde_json::from_str(&domain.configuration)?;
    let names = logical_to_physical_arrow_paths(
        snapshot.schema(),
        snapshot.effective_column_mapping_mode(),
    );
    clustering
        .clustering_columns
        .iter()
        .map(|physical| {
            let logical = names
                .iter()
                .find_map(|(logical, candidate)| {
                    (physical.len() == candidate.len()
                        && physical
                            .iter()
                            .zip(candidate)
                            .all(|(a, b)| a.eq_ignore_ascii_case(b)))
                    .then_some(logical)
                })
                .ok_or_else(|| {
                    DeltaError::generic(format!(
                        "Clustering column {physical:?} is absent from the Delta schema"
                    ))
                })?;
            Ok(logical
                .iter()
                .map(|part| {
                    if part.starts_with(|c: char| c.is_ascii_alphabetic() || c == '_')
                        && part.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
                    {
                        part.clone()
                    } else {
                        format!("`{}`", part.replace('`', "``"))
                    }
                })
                .collect::<Vec<_>>()
                .join("."))
        })
        .collect()
}
