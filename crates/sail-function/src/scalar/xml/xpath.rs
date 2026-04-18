use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, ListBuilder, StringArray, StringBuilder};
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion_common::utils::take_function_args;
use datafusion_common::{plan_err, DataFusionError, Result};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use xee_xpath::{Documents, Item, Queries, Query};

use crate::functions_utils::make_scalar_function;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct Xpath {
    signature: Signature,
}

impl Default for Xpath {
    fn default() -> Self {
        Self::new()
    }
}

impl Xpath {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for Xpath {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "xpath"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::List(Arc::new(Field::new_list_field(
            DataType::Utf8,
            true,
        ))))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() != 2 {
            return plan_err!(
                "`xpath` function requires 2 arguments, got {}",
                arg_types.len()
            );
        }
        arg_types
            .iter()
            .map(|data_type| match data_type {
                DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8 | DataType::Null => {
                    Ok(DataType::Utf8)
                }
                other => {
                    plan_err!("The `xpath` function can only accept strings, but got {other:?}.")
                }
            })
            .collect()
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        make_scalar_function(xpath_inner, vec![])(&args)
    }
}

fn xpath_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    let [xmls, paths] = take_function_args("xpath", args)?;
    let xmls = xmls.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
        DataFusionError::Internal("xpath expected a string array for xml".to_string())
    })?;
    let paths = paths
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            DataFusionError::Internal("xpath expected a string array for path".to_string())
        })?;

    let mut builder = ListBuilder::new(StringBuilder::new());
    for row in 0..xmls.len() {
        let values = if xmls.is_null(row) || paths.is_null(row) {
            None
        } else {
            evaluate_xpath(xmls.value(row), paths.value(row))?
        };
        match values {
            Some(values) => {
                for value in values {
                    builder.values().append_option(value);
                }
                builder.append(true);
            }
            None => builder.append(false),
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn evaluate_xpath(xml: &str, path: &str) -> Result<Option<Vec<Option<String>>>> {
    if xml.is_empty() || path.is_empty() {
        return Ok(None);
    }

    let mut documents = Documents::new();
    let document = documents
        .add_string_without_uri(xml)
        .map_err(|error| DataFusionError::Execution(format!("Invalid XML document: {error}")))?;
    let query = Queries::default()
        .sequence(path)
        .map_err(|error| DataFusionError::Execution(format!("Invalid XPath '{path}': {error}")))?;
    let value = query.execute(&mut documents, document).map_err(|error| {
        DataFusionError::Execution(format!("Error loading expression '{path}': {error}"))
    })?;

    Ok(Some(
        value
            .iter()
            .map(|item| match item {
                Item::Node(node) => Ok(node_value(documents.xot(), node)),
                Item::Atomic(_) | Item::Function(_) => Err(DataFusionError::Execution(format!(
                    "Error loading expression '{path}': Cannot convert XPath result to a NodeList"
                ))),
            })
            .collect::<Result<Vec<_>>>()?,
    ))
}

fn node_value(xot: &xot::Xot, node: xot::Node) -> Option<String> {
    match xot.value(node) {
        xot::Value::Document | xot::Value::Element(_) => None,
        xot::Value::Attribute(attribute) => Some(attribute.value().to_string()),
        xot::Value::Text(text) => Some(text.get().to_string()),
        xot::Value::Comment(comment) => Some(comment.get().to_string()),
        xot::Value::Namespace(namespace) => {
            Some(xot.namespace_str(namespace.namespace()).to_string())
        }
        xot::Value::ProcessingInstruction(instruction) => {
            Some(instruction.data().unwrap_or("").to_string())
        }
    }
}
