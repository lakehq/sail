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

pub(crate) mod catalog_support;
pub mod datasource;
pub mod error;
pub mod io;
pub mod lake_source;
pub mod logical;
pub(crate) mod metadata_table;
pub mod operations;
pub mod options;
pub mod physical;
pub mod physical_plan;
mod procedure;
pub(crate) mod properties;
pub(crate) mod row_level_metadata;
pub mod schema_evolution;
pub mod spec;
pub mod table;
pub mod utils;

pub use datasource::type_converter::*;
pub use datasource::*;
pub use lake_source::*;
pub use logical::IcebergTableSource;
pub use operations::Transaction;
pub use operations::action::*;
pub use operations::append::*;
pub use operations::snapshot::*;
pub use operations::write::base_writer::*;
pub use operations::write::file_writer::*;
pub use operations::write::{IcebergWriter, WriteOutcome};
pub use physical::IcebergPhysicalPlanner;
pub use physical_plan::discovery_exec::IcebergDiscoveryExec;
pub use physical_plan::manifest_scan_exec::{IcebergManifestScanExec, manifest_scan_schema};
pub use physical_plan::scan_by_data_files_exec::IcebergScanByDataFilesExec;
pub use physical_plan::{IcebergBaseWriteContext, IcebergWriteContext, IcebergWriterExecOptions};
pub use schema_evolution::*;
pub use spec::*;
