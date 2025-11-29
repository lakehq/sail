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

use std::sync::Arc;

use datafusion::catalog::Session;
use datafusion::common::Result;
use datafusion::datasource::TableProvider;
use url::Url;

use crate::options::TableIcebergOptions;
use crate::spec::{PartitionSpec, Schema, Snapshot};
use crate::table::Table;

/// Create an Iceberg table provider for reading
pub async fn create_iceberg_provider(
    ctx: &dyn Session,
    table_url: Url,
    options: TableIcebergOptions,
) -> Result<Arc<dyn TableProvider>> {
    let table = Table::load(ctx, table_url).await?;
    let provider = table.to_provider(&options)?;
    Ok(Arc::new(provider))
}

/// Load metadata and pick snapshot per options (precedence: snapshot_id > ref > timestamp > current).
#[allow(dead_code)]
pub(crate) async fn load_table_metadata_with_options(
    ctx: &dyn Session,
    table_url: &Url,
    options: TableIcebergOptions,
) -> Result<(Schema, Snapshot, Vec<PartitionSpec>)> {
    log::trace!(
        "Loading table metadata (with options) from: {}, options: {:?}",
        table_url,
        options
    );
    let table = Table::load(ctx, table_url.clone()).await?;
    table.scan_state(&options)
}
