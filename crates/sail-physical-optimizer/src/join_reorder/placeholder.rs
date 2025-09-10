//! # Placeholder Column Expression
//!
//! This module implements a temporary PhysicalExpr type used during join reordering
//! to maintain stable column identifiers while building the optimal plan structure.
//!
//! The key insight is to separate "building plan structure" from "determining physical indices".
//! PlaceholderColumn allows us to build the entire optimized plan tree using stable
//! (relation_id, base_col_idx) identifiers, and then convert to physical indices in a
//! final pass.

use std::any::Any;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Schema};
use datafusion::common::{internal_err, Result};
use datafusion::physical_expr::{PhysicalExpr, PhysicalExprRef};
use datafusion::physical_expr_common::physical_expr::{DynEq, DynHash};
use datafusion::physical_plan::ColumnarValue;

/// A placeholder column expression that uses stable identifiers instead of physical indices.
///
/// This expression is only used internally during join reordering and should never appear
/// in a final execution plan. It serves as an intermediate representation that allows us
/// to build the optimal plan structure without worrying about physical column indices.
#[derive(Debug, Clone)]
pub(crate) struct PlaceholderColumn {
    /// Stable identifier: (relation_id, base_col_idx)
    pub(crate) stable_id: (usize, usize),
    /// Column name for debugging and schema construction
    pub(crate) name: String,
    /// Data type of the column
    pub(crate) data_type: DataType,
}

impl PlaceholderColumn {
    /// Creates a new PlaceholderColumn
    pub(crate) fn new(stable_id: (usize, usize), name: String, data_type: DataType) -> Self {
        Self {
            stable_id,
            name,
            data_type,
        }
    }
}

impl fmt::Display for PlaceholderColumn {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "#{}.{}", self.stable_id.0, self.stable_id.1)
    }
}

impl PhysicalExpr for PlaceholderColumn {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(self.data_type.clone())
    }

    fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
        // Default to nullable for safety
        Ok(true)
    }

    fn evaluate(
        &self,
        _batch: &datafusion::arrow::record_batch::RecordBatch,
    ) -> Result<ColumnarValue> {
        internal_err!(
            "PlaceholderColumn should never be evaluated. It must be replaced with a real Column before execution."
        )
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(self)
    }

    fn fmt_sql(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "#{}.{}", self.stable_id.0, self.stable_id.1)
    }
}

impl PartialEq<dyn Any> for PlaceholderColumn {
    fn eq(&self, other: &dyn Any) -> bool {
        if let Some(other) = other.downcast_ref::<PlaceholderColumn>() {
            self.stable_id == other.stable_id && self.name == other.name
        } else {
            false
        }
    }
}

impl DynEq for PlaceholderColumn {
    fn dyn_eq(&self, other: &dyn Any) -> bool {
        self.eq(other)
    }
}

impl DynHash for PlaceholderColumn {
    fn dyn_hash(&self, state: &mut dyn Hasher) {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        self.stable_id.hash(&mut hasher);
        self.name.hash(&mut hasher);
        state.write_u64(hasher.finish());
    }
}

/// Helper function to create a PlaceholderColumn PhysicalExprRef
pub(crate) fn placeholder_column(
    stable_id: (usize, usize),
    name: String,
    data_type: DataType,
) -> PhysicalExprRef {
    Arc::new(PlaceholderColumn::new(stable_id, name, data_type))
}
