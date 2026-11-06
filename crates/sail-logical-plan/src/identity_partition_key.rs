use std::collections::HashSet;
use std::fmt::Formatter;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field};
use datafusion_common::{Column, DFSchema, DFSchemaRef, Result, plan_err};
use datafusion_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use educe::Educe;
use sail_common_datafusion::utils::items::ItemTaker;

/// Appends one allocation partition key per materialized generation mask.
/// The producer guarantees non-null mask values; schema nullability can be
/// conservative and is not the runtime invariant.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Educe)]
#[educe(PartialOrd)]
pub struct IdentityPartitionKeyNode {
    input: Arc<LogicalPlan>,
    keys: Vec<(String, String)>,
    #[educe(PartialOrd(ignore))]
    schema: DFSchemaRef,
}

impl IdentityPartitionKeyNode {
    pub fn try_new(input: Arc<LogicalPlan>, keys: Vec<(String, String)>) -> Result<Self> {
        if keys.is_empty() {
            return plan_err!("IdentityPartitionKey requires at least one generation mask");
        }
        let mut masks = HashSet::new();
        let mut names = input
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect::<HashSet<_>>();
        let mut fields = input
            .schema()
            .iter()
            .map(|(qualifier, field)| (qualifier.cloned(), Arc::clone(field)))
            .collect::<Vec<_>>();
        for (mask, key) in &keys {
            let index = input.schema().index_of_column(&Column::from_name(mask))?;
            let field = input.schema().field(index);
            if field.data_type() != &DataType::Boolean {
                return plan_err!("IdentityPartitionKey requires a Boolean mask: {mask}");
            }
            if !masks.insert(mask) || key.is_empty() || !names.insert(key.clone()) {
                return plan_err!(
                    "IdentityPartitionKey requires distinct masks and fresh key names"
                );
            }
            fields.push((None, Arc::new(Field::new(key, DataType::Int32, false))));
        }
        let schema = DFSchema::new_with_metadata(fields, input.schema().metadata().clone())?
            .with_functional_dependencies(input.schema().functional_dependencies().clone())?;
        Ok(Self {
            input,
            keys,
            schema: Arc::new(schema),
        })
    }

    pub fn keys(&self) -> &[(String, String)] {
        &self.keys
    }
}

impl UserDefinedLogicalNodeCore for IdentityPartitionKeyNode {
    fn name(&self) -> &str {
        "IdentityPartitionKey"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![self.input.as_ref()]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "IdentityPartitionKey: {:?}", self.keys)
    }

    fn with_exprs_and_inputs(&self, exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        exprs.zero()?;
        Self::try_new(Arc::new(inputs.one()?), self.keys.clone())
    }

    fn necessary_children_exprs(&self, _output_columns: &[usize]) -> Option<Vec<Vec<usize>>> {
        Some(vec![(0..self.input.schema().fields().len()).collect()])
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::Schema;
    use datafusion_expr::logical_plan::EmptyRelation;

    use super::*;

    #[test]
    fn accepts_nullable_boolean_schema_after_rewrites() -> Result<()> {
        let input = LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(DFSchema::try_from(Schema::new(vec![Field::new(
                "mask",
                DataType::Boolean,
                true,
            )]))?),
        });
        let node = IdentityPartitionKeyNode::try_new(
            Arc::new(input.clone()),
            vec![("mask".into(), "key".into())],
        )?;
        let rewritten = node.with_exprs_and_inputs(vec![], vec![input])?;
        assert_eq!(node.schema(), rewritten.schema());
        assert!(rewritten.schema().field(0).is_nullable());
        assert_eq!(rewritten.schema().field(1).data_type(), &DataType::Int32);
        assert!(!rewritten.schema().field(1).is_nullable());
        Ok(())
    }
}
