use serde::{Deserialize, Serialize};

use crate::spec::transform::Transform;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub struct UnboundPartitionField {
    pub source_id: i32,
    pub name: String,
    pub transform: Transform,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub struct UnboundPartitionSpec {
    pub fields: Vec<UnboundPartitionField>,
}
