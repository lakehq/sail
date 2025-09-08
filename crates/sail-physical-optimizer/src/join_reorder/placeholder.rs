use std::any::Any;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Schema};
use datafusion::common::{internal_err, Result};
use datafusion::physical_expr::{PhysicalExpr, PhysicalExprRef};
use datafusion::physical_expr_common::physical_expr::{DynEq, DynHash};
use datafusion::physical_plan::ColumnarValue;

#[derive(Debug, Clone)]
pub(crate) struct PlaceholderColumn {
    pub(crate) stable_id: usize,
    pub(crate) name_for_schema: String,
    pub(crate) data_type_for_schema: DataType,
}

impl PlaceholderColumn {
    pub(crate) fn new(
        stable_id: usize,
        name_for_schema: String,
        data_type_for_schema: DataType,
    ) -> Self {
        Self {
            stable_id,
            name_for_schema,
            data_type_for_schema,
        }
    }
}

impl fmt::Display for PlaceholderColumn {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "#{}", self.stable_id)
    }
}

impl PhysicalExpr for PlaceholderColumn {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(self.data_type_for_schema.clone())
    }

    fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
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
        write!(f, "#{}", self.stable_id)
    }
}

impl PartialEq<dyn Any> for PlaceholderColumn {
    fn eq(&self, other: &dyn Any) -> bool {
        if let Some(other) = other.downcast_ref::<PlaceholderColumn>() {
            self.stable_id == other.stable_id && self.name_for_schema == other.name_for_schema
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
        self.name_for_schema.hash(&mut hasher);
        state.write_u64(hasher.finish());
    }
}

pub(crate) fn placeholder_column(
    stable_id: usize,
    name: String,
    data_type: DataType,
) -> PhysicalExprRef {
    Arc::new(PlaceholderColumn::new(stable_id, name, data_type))
}
