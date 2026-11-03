use std::sync::Arc;

use datafusion_expr::TableSource;
use sail_catalog::manager::CatalogManager;
use sail_common_datafusion::datasource::{SinkMode, SourceInfo, find_path_in_options};
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_lineage::{LifecycleStateChange, LineageDataset, LineageOperation, LineageService};

use crate::resolver::PlanResolver;

/// The output datasets recorded during plan resolution.
#[derive(Debug, Default)]
pub(crate) struct LineageOutputs {
    pub operation: Option<LineageOperation>,
    pub datasets: Vec<LineageDataset>,
}

impl PlanResolver<'_> {
    pub(super) fn lineage_service(&self) -> Option<Arc<LineageService>> {
        self.ctx.extension::<LineageService>().ok()
    }

    /// Returns the datasets that a data source will read from.
    /// This must be called before the source info is consumed by the table format.
    pub(super) fn lineage_source_datasets(
        &self,
        info: &SourceInfo,
        format: &str,
        table: Option<&[String]>,
    ) -> Vec<LineageDataset> {
        if self.lineage_service().is_none() {
            return vec![];
        }
        if format.eq_ignore_ascii_case("jdbc") {
            return sail_lineage::jdbc_input_datasets(&info.options);
        }
        let table = info
            .catalog_table()
            .or(table)
            .map(|x| self.qualify_lineage_table(x));
        let dataset = |location: Option<String>| LineageDataset {
            identifier: None,
            location,
            table: table.clone(),
            format: Some(format.to_string()),
            // The schema is set when the data source is registered.
            schema: None,
            lifecycle: None,
        };
        if info.paths.is_empty() {
            let location = find_path_in_options(&info.options);
            if location.is_some() || table.is_some() {
                vec![dataset(location)]
            } else {
                vec![]
            }
        } else {
            info.paths
                .iter()
                .map(|x| dataset(Some(x.clone())))
                .collect()
        }
    }

    /// Records the datasets that a data source reads from.
    pub(super) fn register_lineage_source(
        &self,
        source: &Arc<dyn TableSource>,
        mut datasets: Vec<LineageDataset>,
    ) {
        let Some(service) = self.lineage_service() else {
            return;
        };
        // The schema of the source is more accurate than the user-specified schema,
        // since it may include inferred columns.
        let schema = source.schema();
        // A dataset with an explicit identifier (e.g. a table in a JDBC query) may be
        // one of several tables that the source reads, so the source schema does not apply.
        for dataset in datasets.iter_mut().filter(|x| x.identifier.is_none()) {
            dataset.schema = Some(schema.clone());
        }
        service.register_source(source, datasets);
    }

    pub(super) fn register_lineage_source_alias(
        &self,
        source: &Arc<dyn TableSource>,
        wrapper: &Arc<dyn TableSource>,
    ) {
        if let Some(service) = self.lineage_service() {
            service.register_source_alias(source, wrapper);
        }
    }

    /// Records a dataset that the plan writes to.
    pub(super) fn record_lineage_output(
        &self,
        operation: LineageOperation,
        mut dataset: LineageDataset,
    ) {
        if self.lineage_service().is_none() {
            return;
        }
        dataset.table = dataset.table.map(|x| self.qualify_lineage_table(&x));
        if let Ok(mut outputs) = self.lineage_outputs.lock() {
            outputs.operation.get_or_insert(operation);
            outputs.datasets.push(dataset);
        }
    }

    /// Returns the fully qualified table name, or the name as is if it cannot be qualified.
    fn qualify_lineage_table(&self, table: &[String]) -> Vec<String> {
        self.ctx
            .extension::<CatalogManager>()
            .ok()
            .and_then(|x| x.qualify_object_reference(table).ok())
            .unwrap_or_else(|| table.to_vec())
    }

    pub(crate) fn take_lineage_outputs(&self) -> LineageOutputs {
        self.lineage_outputs
            .lock()
            .map(|mut x| std::mem::take(&mut *x))
            .unwrap_or_default()
    }

    /// Returns the lifecycle state change of the output dataset for the sink mode.
    /// `created` indicates whether the write creates the catalog table.
    pub(super) fn lineage_lifecycle(
        mode: &SinkMode,
        created: bool,
    ) -> Option<LifecycleStateChange> {
        if created {
            return Some(LifecycleStateChange::Create);
        }
        match mode {
            SinkMode::Overwrite | SinkMode::OverwriteIf { .. } | SinkMode::OverwritePartitions => {
                Some(LifecycleStateChange::Overwrite)
            }
            SinkMode::ErrorIfExists => Some(LifecycleStateChange::Create),
            SinkMode::Append | SinkMode::IgnoreIfExists => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lineage_lifecycle() {
        let lifecycle = PlanResolver::lineage_lifecycle;
        // Creating a table takes precedence over the sink mode.
        assert_eq!(
            lifecycle(&SinkMode::Overwrite, true),
            Some(LifecycleStateChange::Create)
        );
        assert_eq!(
            lifecycle(&SinkMode::Append, true),
            Some(LifecycleStateChange::Create)
        );
        assert_eq!(
            lifecycle(&SinkMode::Overwrite, false),
            Some(LifecycleStateChange::Overwrite)
        );
        assert_eq!(
            lifecycle(&SinkMode::OverwritePartitions, false),
            Some(LifecycleStateChange::Overwrite)
        );
        assert_eq!(
            lifecycle(&SinkMode::ErrorIfExists, false),
            Some(LifecycleStateChange::Create)
        );
        assert_eq!(lifecycle(&SinkMode::Append, false), None);
    }
}
