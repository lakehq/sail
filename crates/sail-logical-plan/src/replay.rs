use std::fmt::Formatter;
use std::sync::Arc;

use datafusion_common::{DFSchemaRef, Result, internal_err};
use datafusion_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use sail_common_datafusion::replay::ReplayId;
use sail_common_datafusion::utils::items::ItemTaker;

/// A named evaluation boundary, not common-subexpression elimination. Clones keep
/// the name; independent relation references must be assigned different names.
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Hash)]
pub struct ReplayNode {
    id: ReplayId,
    input: Arc<LogicalPlan>,
}

impl ReplayNode {
    pub fn new(id: ReplayId, input: Arc<LogicalPlan>) -> Self {
        Self { id, input }
    }

    pub fn id(&self) -> ReplayId {
        self.id
    }

    pub fn input(&self) -> &Arc<LogicalPlan> {
        &self.input
    }
}

impl UserDefinedLogicalNodeCore for ReplayNode {
    fn name(&self) -> &str {
        "Replay"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![self.input.as_ref()]
    }

    fn schema(&self) -> &DFSchemaRef {
        self.input.schema()
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "Replay: {}", self.id)
    }

    fn with_exprs_and_inputs(&self, exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        exprs.zero()?;
        let input = inputs.one()?;
        // Logical rewrites (e.g. DISTINCT ON lowering and constant folding) can
        // refine nullability. The physical replay freezes the analyzed schema.
        let before = self.input.schema();
        let after = input.schema();
        if before.fields().len() != after.fields().len()
            || before.metadata() != after.metadata()
            || !before.iter().zip(after.iter()).all(|((bq, bf), (aq, af))| {
                bq == aq
                    && bf.as_ref().clone().with_nullable(true)
                        == af.as_ref().clone().with_nullable(true)
            })
        {
            return internal_err!(
                "replay input schema cannot change: {}: expected {:?}, got {:?}",
                self.id,
                self.input.schema(),
                input.schema()
            );
        }
        Ok(Self::new(self.id, Arc::new(input)))
    }

    // Sail's filter-pushdown rule also blocks predicates with no column refs.
    // The default limit policy and all-column requirement prevent consumers
    // from specializing the producer's shape.
    fn necessary_children_exprs(&self, _output_columns: &[usize]) -> Option<Vec<Vec<usize>>> {
        Some(vec![(0..self.input.schema().fields().len()).collect()])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::ToDFSchema;
    use datafusion_expr::EmptyRelation;

    #[test]
    fn logical_replay_allows_inferred_nullability_not_payload_changes() -> Result<()> {
        let input = |name: &str, data_type: DataType, nullable: bool| -> Result<LogicalPlan> {
            Ok(LogicalPlan::EmptyRelation(EmptyRelation {
                produce_one_row: false,
                schema: Schema::new(vec![Field::new(name, data_type, nullable)])
                    .to_dfschema_ref()?,
            }))
        };
        let node = ReplayNode::new(
            ReplayId {
                scope: uuid::Uuid::new_v4(),
                ordinal: 0,
            },
            Arc::new(input("id", DataType::Int64, false)?),
        );
        let rewritten =
            node.with_exprs_and_inputs(vec![], vec![input("id", DataType::Int64, true)?])?;
        assert_eq!(rewritten.id(), node.id());
        assert!(rewritten.input().schema().field(0).is_nullable());
        assert!(
            node.with_exprs_and_inputs(vec![], vec![input("other", DataType::Int64, false)?])
                .is_err()
        );
        assert!(
            node.with_exprs_and_inputs(vec![], vec![input("id", DataType::Int32, false)?])
                .is_err()
        );
        Ok(())
    }
}
