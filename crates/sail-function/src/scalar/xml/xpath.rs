use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, ListBuilder, StringArray, StringBuilder};
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion_common::utils::take_function_args;
use datafusion_common::{plan_err, DataFusionError, Result};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility};
use sxd_document::parser;
use sxd_xpath::nodeset::Node;
use sxd_xpath::{Context, Factory, Value};

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
            return plan_err!("`xpath` function requires 2 arguments, got {}", arg_types.len());
        }
        arg_types
            .iter()
            .map(|data_type| match data_type {
                DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8 | DataType::Null => {
                    Ok(DataType::Utf8)
                }
                other => plan_err!("The `xpath` function can only accept strings, but got {other:?}."),
            })
            .collect()
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        make_scalar_function(xpath_inner, vec![])(&args)
    }
}

pub fn xpath_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::new_from_impl(Xpath::new()))
}

fn xpath_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    let [xmls, paths] = take_function_args("xpath", args)?;
    let xmls = xmls
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| DataFusionError::Internal("xpath expected a string array for xml".to_string()))?;
    let paths = paths
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| DataFusionError::Internal("xpath expected a string array for path".to_string()))?;

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

    let package = parser::parse(xml)
        .map_err(|error| DataFusionError::Execution(format!("Invalid XML document: {error}\n{xml}")))?;
    let expression = Factory::new()
        .build(path)
        .map_err(|error| DataFusionError::Execution(format!("Invalid XPath '{path}'{error}")))?;
    let expression = expression
        .ok_or_else(|| DataFusionError::Execution(format!("Invalid XPath '{path}'")))?;
    let value = expression
        .evaluate(&Context::new(), package.as_document().root())
        .map_err(|error| DataFusionError::Execution(format!("Error loading expression '{path}': {error}")))?;

    match value {
        Value::Nodeset(nodeset) => Ok(Some(
            nodeset
                .document_order()
                .into_iter()
                .map(node_value)
                .collect(),
        )),
        _ => Err(DataFusionError::Execution(format!(
            "Error loading expression '{path}': Can not convert XPath result to a NodeList"
        ))),
    }
}

fn node_value(node: Node<'_>) -> Option<String> {
    match node {
        Node::Root(_) | Node::Element(_) => None,
        Node::Attribute(attribute) => Some(attribute.value().to_string()),
        Node::Text(text) => Some(text.text().to_string()),
        Node::Comment(comment) => Some(comment.text().to_string()),
        Node::Namespace(namespace) => Some(namespace.uri().to_string()),
        Node::ProcessingInstruction(instruction) => Some(instruction.value().unwrap_or("").to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::evaluate_xpath;

    #[test]
    fn returns_text_nodes() {
        let result = evaluate_xpath("<a><b>b1</b><b>b2</b></a>", "a/b/text()").unwrap();
        assert_eq!(
            result,
            Some(vec![Some("b1".to_string()), Some("b2".to_string())])
        );
    }

    #[test]
    fn returns_nulls_for_element_nodes() {
        let result = evaluate_xpath("<a><b>b1</b><b>b2</b></a>", "a/b").unwrap();
        assert_eq!(result, Some(vec![None, None]));
    }

    #[test]
    fn returns_empty_list_for_no_match() {
        let result = evaluate_xpath("<a><b>b1</b></a>", "a/c").unwrap();
        assert_eq!(result, Some(vec![]));
    }

    #[test]
    fn returns_null_for_empty_inputs() {
        assert_eq!(evaluate_xpath("", "a/b").unwrap(), None);
        assert_eq!(evaluate_xpath("<a><b>1</b></a>", "").unwrap(), None);
    }

    #[test]
    fn errors_for_non_nodeset_result() {
        let error = evaluate_xpath("<a><b>1</b></a>", "sum(a/b)").unwrap_err();
        assert!(error.to_string().contains("NodeList"));
    }
}
