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

pub mod converter;
pub mod manager;
pub mod mapping;
pub mod normalize;

pub use converter::{
    arrow_schema_from_struct_type, kernel_to_logical_arrow, logical_arrow_to_kernel,
};
pub use manager::{annotate_for_column_mapping, evolve_schema, get_physical_schema};
pub use mapping::{
    annotate_new_fields_for_column_mapping, annotate_schema_for_column_mapping,
    compute_max_column_id,
};
pub use normalize::normalize_delta_schema;
