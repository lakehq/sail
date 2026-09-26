use std::fmt::Formatter;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion_common::{DFSchema, DFSchemaRef, Result, TableReference};
use datafusion_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use educe::Educe;
use sail_common_datafusion::rename::schema::rename_schema;
use sail_common_datafusion::udf::StreamUDF;
use sail_common_datafusion::utils::items::ItemTaker;

#[derive(Clone, Debug, Eq, Educe)]
#[educe(PartialOrd)]
pub struct MapPartitionsNode {
    input: Arc<LogicalPlan>,
    udf: Arc<dyn StreamUDF>,
    #[educe(PartialOrd(ignore))]
    schema: DFSchemaRef,
}

impl MapPartitionsNode {
    pub fn try_new(
        input: Arc<LogicalPlan>,
        output_names: Vec<String>,
        output_qualifiers: Vec<Option<TableReference>>,
        udf: Arc<dyn StreamUDF>,
    ) -> Result<Self> {
        let schema = rename_schema(&udf.output_schema(), &output_names)?;
        Ok(Self {
            input,
            udf,
            schema: Arc::new(DFSchema::from_field_specific_qualified_schema(
                output_qualifiers,
                &schema,
            )?),
        })
    }

    pub fn udf(&self) -> &Arc<dyn StreamUDF> {
        &self.udf
    }
}

impl PartialEq for MapPartitionsNode {
    fn eq(&self, other: &Self) -> bool {
        // `Arc<dyn StreamUDF>` cannot derive `PartialEq`, but the trait object
        // supports object-safe equality through `DynObject`.
        self.input == other.input
            && self.udf.as_ref() == other.udf.as_ref()
            && self.schema == other.schema
    }
}

impl Hash for MapPartitionsNode {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.input.hash(state);
        self.udf.hash(state);
        self.schema.hash(state);
    }
}

impl UserDefinedLogicalNodeCore for MapPartitionsNode {
    fn name(&self) -> &str {
        "MapPartitions"
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
        write!(f, "MapPartitions")
    }

    fn with_exprs_and_inputs(&self, exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        exprs.zero()?;
        let input = Arc::new(inputs.one()?);
        let columns = input
            .schema()
            .columns()
            .iter()
            .map(|column| self.input.schema().index_of_column(column))
            .collect::<Result<Vec<_>>>()?;
        if columns != (0..self.input.schema().fields().len()).collect::<Vec<_>>()
            && let Some(projection) = self.udf.project_input(&columns)?
        {
            return Ok(Self {
                input,
                udf: projection.udf,
                schema: Arc::new(DFSchema::new_with_metadata(
                    projection
                        .output_columns
                        .iter()
                        .map(|&i| {
                            let (qualifier, field) = self.schema.qualified_field(i);
                            (qualifier.cloned(), Arc::clone(field))
                        })
                        .collect(),
                    self.schema.metadata().clone(),
                )?),
            });
        }
        Ok(Self {
            input,
            ..self.clone()
        })
    }

    fn necessary_children_exprs(&self, output_columns: &[usize]) -> Option<Vec<Vec<usize>>> {
        let input_columns = self.input.schema().fields().len();
        Some(vec![
            self.udf
                .required_input_columns(output_columns, input_columns)
                .unwrap_or_else(|| (0..input_columns).collect()),
        ])
    }
}
