use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::error::Result;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::ExecutionPlan;

use crate::join_reorder::decomposer::DecomposedPlan;
use crate::join_reorder::finalizer::PlanFinalizer;
use crate::join_reorder::placeholder::placeholder_column;
use crate::join_reorder::plan::JoinNode;

pub(crate) struct PlanBuilder<'a> {
    decomposed: &'a DecomposedPlan,
    original_schema: SchemaRef,
}

impl<'a> PlanBuilder<'a> {
    pub(crate) fn new(decomposed: &'a DecomposedPlan, original_schema: SchemaRef) -> Self {
        Self {
            decomposed,
            original_schema,
        }
    }

    pub(crate) fn build(self, optimal_node: Arc<JoinNode>) -> Result<Arc<dyn ExecutionPlan>> {
        let proto_plan = optimal_node.build_prototype_plan_recursive(
            &self.decomposed.join_relations,
            &self.decomposed.column_catalog,
        )?;
        let proto_plan_with_projection = self.create_prototype_schema_projection(proto_plan)?;

        let finalizer = PlanFinalizer::new(
            &self.decomposed.column_catalog,
            &self.decomposed.join_relations,
        );
        let final_plan = finalizer.finalize(proto_plan_with_projection)?;

        Ok(final_plan)
    }

    fn create_prototype_schema_projection(
        &self,
        proto_plan: Arc<dyn ExecutionPlan>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let mut projection_exprs = Vec::new();

        for original_idx in 0..self.original_schema.fields().len() {
            if let Some(&origin) = self.decomposed.original_output_map.get(&original_idx) {
                let field = self.original_schema.field(original_idx);
                let placeholder =
                    placeholder_column(origin.1, field.name().clone(), field.data_type().clone());
                projection_exprs.push((placeholder, field.name().clone()));
            }
        }

        let projection = ProjectionExec::try_new(projection_exprs, proto_plan)?;
        Ok(Arc::new(projection))
    }
}
