// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;

use datafusion_expr::Expr;

use crate::datasource::predicate::Predicate;
use crate::spec::{DataFile, ManifestContentType, ManifestList, PartitionSpec, Schema};

pub fn prune_files(
    filters: &[Expr],
    limit: Option<usize>,
    files: Vec<DataFile>,
    iceberg_schema: &Schema,
) -> (Vec<DataFile>, Option<Vec<bool>>) {
    if filters.is_empty() && limit.is_none() {
        return (files, None);
    }
    let predicate = Predicate::conjunction(iceberg_schema, filters);
    let mask = files
        .iter()
        .map(|file| predicate.file(file, None).may_match())
        .collect::<Vec<_>>();
    let mut rows = 0u64;
    let kept = files
        .into_iter()
        .zip(&mask)
        .filter_map(|(file, keep)| {
            if !keep || limit.is_some_and(|limit| rows >= limit as u64) {
                return None;
            }
            rows = rows.saturating_add(file.record_count());
            Some(file)
        })
        .collect();
    (kept, Some(mask))
}

pub fn prune_manifests_by_partition_summaries<'a>(
    manifest_list: &'a ManifestList,
    table_schema: &Schema,
    partition_specs: &HashMap<i32, PartitionSpec>,
    filters: &[Expr],
) -> Vec<&'a crate::spec::manifest_list::ManifestFile> {
    let predicate = Predicate::conjunction(table_schema, filters);
    manifest_list
        .entries()
        .iter()
        .filter(|manifest| manifest.content == ManifestContentType::Data)
        .filter(|manifest| {
            let Some(spec) = partition_specs.get(&manifest.partition_spec_id) else {
                return true;
            };
            manifest
                .partitions
                .as_ref()
                .is_none_or(|summaries| predicate.manifest(spec, summaries).may_match())
        })
        .collect()
}

pub fn prune_data_files_by_partition_values(
    files: Vec<DataFile>,
    table_schema: &Schema,
    partition_spec: &PartitionSpec,
    filters: &[Expr],
) -> Vec<DataFile> {
    let predicate = Predicate::conjunction(table_schema, filters);
    files
        .into_iter()
        .filter(|file| {
            predicate
                .partition(partition_spec, &file.partition)
                .may_match()
        })
        .collect()
}
