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

//! Native Parquet writing infrastructure shared by listing data sources and
//! table-format adapters.

mod demux;
mod file_writer;
mod physical_plan;

pub use file_writer::{ParquetFileWriter, WrittenParquetFile};
pub use physical_plan::{ParquetWriteExecutionOptions, ParquetWriterExec};
