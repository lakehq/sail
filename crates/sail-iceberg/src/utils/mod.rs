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

pub mod conversions;
pub mod snapshot_id;
pub mod timestamp;
pub mod transform;

use std::sync::Arc;

use datafusion::catalog::Session;
use datafusion::execution::context::TaskContext;
use datafusion_common::{DataFusionError, Result};
use url::Url;

pub const WRITE_RELATIVE_PROP: &str = "sail.iceberg.write_relative_paths";

pub enum WritePathMode {
    Absolute,
    Relative,
}

impl WritePathMode {
    pub fn from_properties(props: Option<&std::collections::HashMap<String, String>>) -> Self {
        if let Some(p) = props {
            if let Some(v) = p.get(WRITE_RELATIVE_PROP) {
                if v.eq_ignore_ascii_case("true") || v == "1" {
                    return WritePathMode::Relative;
                }
            }
        }
        WritePathMode::Absolute
    }
}

pub fn join_table_uri(table_uri: &str, rel: &str, mode: &WritePathMode) -> String {
    match mode {
        WritePathMode::Absolute => format!("{}{}", table_uri, rel),
        WritePathMode::Relative => rel.to_string(),
    }
}

pub fn get_object_store_from_context(
    context: &Arc<TaskContext>,
    table_url: &Url,
) -> Result<Arc<dyn object_store::ObjectStore>> {
    context
        .runtime_env()
        .object_store_registry
        .get_store(table_url)
        .map_err(|e| DataFusionError::External(Box::new(e)))
}

pub fn get_object_store_from_session(
    session: &dyn Session,
    table_url: &Url,
) -> Result<Arc<dyn object_store::ObjectStore>> {
    session
        .runtime_env()
        .object_store_registry
        .get_store(table_url)
        .map_err(|e| DataFusionError::External(Box::new(e)))
}
