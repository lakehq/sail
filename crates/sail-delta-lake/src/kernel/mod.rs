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

pub mod arrow;
pub mod models;
pub mod snapshot;
pub mod statistics;
pub mod transaction;

pub(crate) mod checkpoints;
mod config;
mod error;
mod operation;
mod table_properties;

use std::sync::LazyLock;

pub use config::DeltaTableConfig;
pub use delta_kernel::engine::arrow_conversion::{TryIntoArrow, TryIntoKernel};
pub use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::engine::arrow_expression::ArrowEvaluationHandler;
pub use delta_kernel::engine::parse_json;
pub use delta_kernel::expressions::ColumnName;
pub use delta_kernel::scan::scan_row_schema;
pub use delta_kernel::schema::{SchemaRef, SchemaTransform};
pub use delta_kernel::table_properties::DataSkippingNumIndexedCols;
pub use delta_kernel::{EvaluationHandler, Expression, ExpressionEvaluator, PredicateRef};
pub use error::{DeltaResult, DeltaTableError};
pub use operation::{DeltaOperation, MergePredicate, SaveMode};
pub use table_properties::TablePropertiesExt;

pub(crate) static ARROW_HANDLER: LazyLock<ArrowEvaluationHandler> =
    LazyLock::new(|| ArrowEvaluationHandler {});
