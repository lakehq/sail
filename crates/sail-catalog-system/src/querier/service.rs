use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::common::Result;
use datafusion::physical_expr::PhysicalExpr;
use sail_common_datafusion::array::serde::ArrowSerializer;
use sail_common_datafusion::extension::SessionExtension;

use crate::gen::catalog::SystemTable;
use crate::querier::SessionQuerier;
use crate::SYSTEM_TRACING_OPTIONS;

pub struct SystemQuerierService {
    pub session: Box<dyn SessionQuerier>,
}

impl SystemQuerierService {
    pub async fn execute(
        &self,
        table: SystemTable,
        projection: Option<Vec<usize>>,
        filters: Vec<Arc<dyn PhysicalExpr>>,
        fetch: Option<usize>,
    ) -> Result<RecordBatch> {
        let serializer = ArrowSerializer::new(SYSTEM_TRACING_OPTIONS.clone());
        match table {
            SystemTable::Sessions => serializer.to_record_batch(&self.session.sessions().await?),
        }
    }
}

impl SessionExtension for SystemQuerierService {
    fn name() -> &'static str {
        "SystemQuerierService"
    }
}
