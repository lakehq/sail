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

use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::Result;
use datafusion::logical_expr::TableSource;

use crate::datasource::SourceInfo;

/// Storage access needed to materialize a lake relation.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub enum LakeRelationAccess {
    MetadataRead,
    DataRead,
}

/// Time-travel behavior exposed by a lake relation.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub enum LakeRelationTimeTravel {
    Unsupported,
    Supported,
}

/// A format-owned relation resolved relative to a catalog table.
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct LakeRelation {
    name: String,
    access: LakeRelationAccess,
    time_travel: LakeRelationTimeTravel,
}

impl LakeRelation {
    pub fn new(
        name: impl Into<String>,
        access: LakeRelationAccess,
        time_travel: LakeRelationTimeTravel,
    ) -> Self {
        Self {
            name: name.into(),
            access,
            time_travel,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn access(&self) -> LakeRelationAccess {
        self.access
    }

    pub fn time_travel(&self) -> LakeRelationTimeTravel {
        self.time_travel
    }
}

/// Result of asking a lake source to interpret a relation suffix.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum LakeRelationResolution {
    /// The suffix is not owned by this lake source.
    Unrecognized,
    /// The suffix is part of the format contract but is not implemented by Sail.
    Unsupported { reason: String },
    /// The suffix is implemented and can be materialized by the provider.
    Supported(LakeRelation),
}

/// Format-owned named relations, such as Iceberg metadata tables.
#[async_trait]
pub trait LakeRelationProvider: Send + Sync {
    fn resolve_relation(&self, name: &str) -> LakeRelationResolution;

    async fn create_relation(
        &self,
        ctx: &dyn Session,
        info: SourceInfo,
        relation: LakeRelation,
    ) -> Result<Arc<dyn TableSource>>;
}
