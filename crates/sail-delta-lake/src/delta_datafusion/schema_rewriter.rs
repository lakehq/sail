use std::fmt::Debug;
use std::sync::Arc;

use datafusion::arrow::datatypes::{FieldRef, Schema, SchemaRef};
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::common::{Result, ScalarValue};
use datafusion::physical_expr::schema_rewriter::{
    DefaultPhysicalExprAdapter, PhysicalExprAdapter, PhysicalExprAdapterFactory,
};
use datafusion::physical_expr::PhysicalExpr;

/// A Physical Expression Adapter Factory which provides casting and rewriting of physical expressions
/// for Delta Lake conventions.
#[derive(Debug)]
pub struct DeltaPhysicalExprAdapterFactory {}

impl PhysicalExprAdapterFactory for DeltaPhysicalExprAdapterFactory {
    fn create(
        &self,
        logical_file_schema: SchemaRef,
        physical_file_schema: SchemaRef,
    ) -> Arc<dyn PhysicalExprAdapter> {
        Arc::new(DeltaPhysicalExprAdapter {
            logical_file_schema,
            physical_file_schema,
            partition_values: Vec::new(),
        })
    }
}

/// A Physical Expression Adapter that handles Delta Lake specific expression rewriting
#[derive(Debug)]
pub(crate) struct DeltaPhysicalExprAdapter {
    logical_file_schema: SchemaRef,
    physical_file_schema: SchemaRef,
    partition_values: Vec<(FieldRef, ScalarValue)>,
}

impl PhysicalExprAdapter for DeltaPhysicalExprAdapter {
    fn rewrite(&self, expr: Arc<dyn PhysicalExpr>) -> Result<Arc<dyn PhysicalExpr>> {
        let rewriter = DeltaPhysicalExprRewriter {
            logical_file_schema: &self.logical_file_schema,
            physical_file_schema: &self.physical_file_schema,
            partition_values: &self.partition_values,
        };
        expr.transform(|expr| rewriter.rewrite_expr(Arc::clone(&expr)))
            .data()
    }

    fn with_partition_values(
        &self,
        partition_values: Vec<(FieldRef, ScalarValue)>,
    ) -> Arc<dyn PhysicalExprAdapter> {
        Arc::new(DeltaPhysicalExprAdapter {
            partition_values,
            ..self.clone()
        })
    }
}

impl Clone for DeltaPhysicalExprAdapter {
    fn clone(&self) -> Self {
        Self {
            logical_file_schema: Arc::clone(&self.logical_file_schema),
            physical_file_schema: Arc::clone(&self.physical_file_schema),
            partition_values: self.partition_values.clone(),
        }
    }
}

struct DeltaPhysicalExprRewriter<'a> {
    logical_file_schema: &'a Schema,
    physical_file_schema: &'a Schema,
    partition_values: &'a [(FieldRef, ScalarValue)],
}

impl<'a> DeltaPhysicalExprRewriter<'a> {
    fn rewrite_expr(
        &self,
        expr: Arc<dyn PhysicalExpr>,
    ) -> Result<Transformed<Arc<dyn PhysicalExpr>>> {
        // TODO: extended later with Delta Lake specific optimizations

        let default_adapter = DefaultPhysicalExprAdapter::new(
            Arc::new(self.logical_file_schema.clone()),
            Arc::new(self.physical_file_schema.clone()),
        );

        let adapter_with_partitions =
            default_adapter.with_partition_values(self.partition_values.to_vec());

        let rewritten = adapter_with_partitions.rewrite(expr.clone())?;

        // Check if the expression was actually changed
        if std::ptr::eq(
            expr.as_ref() as *const dyn PhysicalExpr,
            rewritten.as_ref() as *const dyn PhysicalExpr,
        ) {
            Ok(Transformed::no(expr))
        } else {
            Ok(Transformed::yes(rewritten))
        }
    }
}
