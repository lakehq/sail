use std::fmt::{Display, Formatter};
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, FieldRef, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{Result, ScalarValue, internal_err};
use datafusion::config::ConfigOptions;
use datafusion::functions::core::get_field;
use datafusion::functions::core::getfield::GetFieldFunc;
use datafusion::physical_expr::expressions::{Column, Literal};
use datafusion::physical_expr::{PhysicalExpr, ScalarFunctionExpr};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl};

use super::SparkGetField;
use crate::schema_evolution::FIELD_DEFAULT_METADATA_KEY;

// Keep the dependency distinct from an ordinary, unmasked native field access.
// Projection rewrites must never substitute an already evaluated native result.
pub const STRUCT_FIELD_DEPENDENCY_NAME: &str = "__sail_struct_field_dependency";

/// Expose the native field path to Parquet while preserving every ancestor's validity.
/// The native child is a dependency description and is never evaluated directly.
#[derive(Debug, Clone, Eq)]
pub struct SparkGetFieldExpr {
    access: Arc<dyn PhysicalExpr>,
    field: FieldRef,
}

impl PartialEq for SparkGetFieldExpr {
    fn eq(&self, other: &Self) -> bool {
        self.access.eq(&other.access) && self.field == other.field
    }
}

impl std::hash::Hash for SparkGetFieldExpr {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.access.hash(state);
        self.field.hash(state);
    }
}

impl SparkGetFieldExpr {
    pub fn try_new(
        args: Vec<Arc<dyn PhysicalExpr>>,
        schema: &Schema,
        config: Arc<ConfigOptions>,
    ) -> Result<Self> {
        let access =
            ScalarFunctionExpr::try_new(get_field(), args.clone(), schema, config.clone())?;
        let field = access.return_field(schema)?;
        Self::from_access(
            Arc::new(ScalarFunctionExpr::new(
                STRUCT_FIELD_DEPENDENCY_NAME,
                get_field(),
                args,
                field.clone(),
                config,
            )),
            field,
        )
    }

    pub fn from_access(access: Arc<dyn PhysicalExpr>, field: FieldRef) -> Result<Self> {
        let Some(function) = ScalarFunctionExpr::try_downcast_func::<GetFieldFunc>(access.as_ref())
        else {
            return internal_err!("SparkGetFieldExpr requires a native field dependency");
        };
        if function.name() != STRUCT_FIELD_DEPENDENCY_NAME
            || function.args().len() < 2
            || function.args()[1..].iter().any(|arg| {
                arg.downcast_ref::<Literal>()
                    .and_then(|literal| literal.value().try_as_str().flatten())
                    .is_none()
            })
            || function.return_type() != field.data_type()
        {
            return internal_err!("invalid SparkGetFieldExpr dependency");
        }
        Ok(Self { access, field })
    }

    pub fn access(&self) -> Result<&ScalarFunctionExpr> {
        ScalarFunctionExpr::try_downcast_func::<GetFieldFunc>(self.access.as_ref())
            .ok_or_else(|| datafusion::common::internal_datafusion_err!("invalid field dependency"))
    }

    pub fn field(&self) -> &FieldRef {
        &self.field
    }
}

impl Display for SparkGetFieldExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "spark_get_field({})", self.access)
    }
}

impl PhysicalExpr for SparkGetFieldExpr {
    fn data_type(&self, _schema: &Schema) -> Result<DataType> {
        Ok(self.field.data_type().clone())
    }

    fn nullable(&self, _schema: &Schema) -> Result<bool> {
        Ok(self.field.is_nullable())
    }

    fn return_field(&self, _schema: &Schema) -> Result<FieldRef> {
        Ok(self.field.clone())
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let access = self.access()?;
        let parent = &access.args()[0];
        let mut field = parent.return_field(batch.schema().as_ref())?;
        let mut value = parent.evaluate(batch)?;
        let udf = SparkGetField::new();
        let config_options = Arc::new(access.config_options().clone());
        for (index, argument) in access.args()[1..].iter().enumerate() {
            let name = argument
                .downcast_ref::<Literal>()
                .and_then(|literal| literal.value().try_as_str().flatten())
                .ok_or_else(|| {
                    datafusion::common::internal_datafusion_err!("invalid field name")
                })?;
            let DataType::Struct(fields) = field.data_type() else {
                return internal_err!(
                    "field dependency requires a struct, got {}",
                    field.data_type()
                );
            };
            let child = fields
                .iter()
                .find(|child| child.name() == name)
                .ok_or_else(|| {
                    datafusion::common::internal_datafusion_err!("field {name} is missing")
                })?;
            let result_field = if index + 2 == access.args().len() {
                self.field.clone()
            } else {
                Arc::new(
                    child
                        .as_ref()
                        .clone()
                        .with_nullable(field.is_nullable() || child.is_nullable()),
                )
            };
            value = udf.invoke_with_args(ScalarFunctionArgs {
                args: vec![value, argument.evaluate(batch)?],
                arg_fields: vec![field, argument.return_field(batch.schema().as_ref())?],
                number_rows: batch.num_rows(),
                return_field: result_field.clone(),
                config_options: Arc::clone(&config_options),
            })?;
            field = result_field;
        }
        Ok(value)
    }

    fn fmt_sql(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self, f)
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.access]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        let [access] = children.as_slice() else {
            return internal_err!("SparkGetFieldExpr requires exactly one child");
        };
        // A scalar replacement would discard the parent validity. Fail instead of
        // silently evaluating an unmasked primitive after an unsupported rewrite.
        Ok(Arc::new(Self::from_access(
            access.clone(),
            self.field.clone(),
        )?))
    }
}

/// Restore null-safe field paths recognized by the Parquet decoder.
pub fn rewrite_parquet_field_access(
    expression: Arc<dyn PhysicalExpr>,
    schema: &Schema,
) -> Result<Transformed<Arc<dyn PhysicalExpr>>> {
    expression.transform_down(|expression| {
        let Some(access) =
            ScalarFunctionExpr::try_downcast_func::<SparkGetField>(expression.as_ref())
        else {
            return Ok(Transformed::no(expression));
        };
        // Restore the primitive and list paths accepted by Parquet decoder predicates.
        // Keep schema-evolution defaults on the existing structural-cast path.
        if access.return_type().is_nested()
            && !matches!(
                access.return_type(),
                DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(_, _)
            )
        {
            return Ok(Transformed::no(expression));
        }
        let Some((index, path)) = struct_field_path(&expression) else {
            return Ok(Transformed::no(expression));
        };
        let Some(mut field) = schema.fields().get(index) else {
            return Ok(Transformed::no(expression));
        };
        if field.metadata().contains_key(FIELD_DEFAULT_METADATA_KEY) {
            return Ok(Transformed::no(expression));
        }
        for name in &path {
            let DataType::Struct(fields) = field.data_type() else {
                return Ok(Transformed::no(expression));
            };
            let Some(child) = fields.iter().find(|field| field.name() == name) else {
                return Ok(Transformed::no(expression));
            };
            field = child;
            if field.metadata().contains_key(FIELD_DEFAULT_METADATA_KEY) {
                return Ok(Transformed::no(expression));
            }
        }
        let mut args: Vec<Arc<dyn PhysicalExpr>> =
            vec![Arc::new(Column::new(schema.field(index).name(), index))];
        args.extend(path.into_iter().map(|name| {
            Arc::new(Literal::new(ScalarValue::Utf8(Some(name)))) as Arc<dyn PhysicalExpr>
        }));
        Ok(Transformed::yes(Arc::new(SparkGetFieldExpr::try_new(
            args,
            schema,
            Arc::new(access.config_options().clone()),
        )?) as Arc<dyn PhysicalExpr>))
    })
}

/// Extract a named field path rooted at an input column.
pub fn struct_field_path(expression: &Arc<dyn PhysicalExpr>) -> Option<(usize, Vec<String>)> {
    let mut expression = expression;
    let mut path = vec![];
    while let Some(access) =
        ScalarFunctionExpr::try_downcast_func::<SparkGetField>(expression.as_ref())
    {
        let [parent, field] = access.args() else {
            return None;
        };
        let field = field
            .downcast_ref::<Literal>()?
            .value()
            .try_as_str()
            .flatten()?;
        path.push(field.to_string());
        expression = parent;
    }
    if path.is_empty() {
        return None;
    }
    let column = expression.downcast_ref::<Column>()?;
    path.reverse();
    Some((column.index(), path))
}
