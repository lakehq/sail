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

use super::{FastAppendAction, Transaction};
use crate::spec::manifest::ManifestMetadata;
use crate::spec::{FormatVersion, ManifestContentType, PartitionSpec, Schema};

impl Transaction {
    pub fn fast_append(&self) -> FastAppendAction {
        FastAppendAction::new()
    }

    pub fn default_manifest_metadata(&self, schema: &Schema) -> ManifestMetadata {
        // Use spec id 0 by default until we plumb real specs
        let partition_spec = PartitionSpec::builder().with_spec_id(0).build();
        ManifestMetadata::new(
            std::sync::Arc::new(schema.clone()),
            schema.schema_id(),
            partition_spec,
            FormatVersion::V2,
            ManifestContentType::Data,
        )
    }
}
