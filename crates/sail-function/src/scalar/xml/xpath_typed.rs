use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, BooleanBuilder, Float32Builder, Float64Builder, Int16Builder, Int32Builder,
    Int64Builder, StringArray, StringBuilder,
};
use datafusion::arrow::datatypes::{DataType, FieldRef};
use datafusion_common::utils::take_function_args;
use datafusion_common::{DataFusionError, Result, plan_err};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use xee_xpath::{Documents, Queries, Query};

use crate::functions_utils::make_scalar_function;

pub fn xpath_typed_name_to_kind(name: &str) -> Result<XpathTypedKind> {
    match name {
        "xpath_boolean" => Ok(XpathTypedKind::Boolean),
        "xpath_double" => Ok(XpathTypedKind::Double),
        "xpath_float" => Ok(XpathTypedKind::Float),
        "xpath_int" => Ok(XpathTypedKind::Int),
        "xpath_long" => Ok(XpathTypedKind::Long),
        "xpath_number" => Ok(XpathTypedKind::Number),
        "xpath_short" => Ok(XpathTypedKind::Short),
        "xpath_string" => Ok(XpathTypedKind::String),
        _ => plan_err!("Invalid xpath typed function name: {name}"),
    }
}

pub use sail_type_system::policies::xpath_kind::XpathTypedKind;

/// A scalar UDF that evaluates a typed XPath expression against an XML document.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct XpathTyped {
    kind: XpathTypedKind,
    signature: Signature,
}

impl XpathTyped {
    pub fn new(kind: XpathTypedKind) -> Self {
        Self {
            kind,
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for XpathTyped {
    fn name(&self) -> &str {
        self.kind.name()
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        sail_type_system::policies::scalar_xml_xpath_typed_typing::xpath_typed_return_type(
            _arg_types,
        )
    }

    /// Spark: `XPathExtract.nullable = true`, unconditional; every typed variant inherits it
    /// and declares no `nullable` of its own.
    /// <https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/xml/xpath.scala#L40>
    fn return_field_from_args(&self, _args: ReturnFieldArgs) -> Result<FieldRef> {
        sail_type_system::policies::xpath_kind::return_field_from_args(self.kind, _args)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        sail_type_system::policies::xpath_kind::coerce_types(self.kind, arg_types)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        let kind = self.kind;
        make_scalar_function(move |a| xpath_typed_inner(kind, a), vec![])(&args)
    }
}

fn xpath_typed_inner(kind: XpathTypedKind, args: &[ArrayRef]) -> Result<ArrayRef> {
    let name = kind.name();
    let [xmls, paths] = take_function_args(name, args)?;
    let xmls = xmls.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
        DataFusionError::Internal(format!("{name} expected a string array for xml"))
    })?;
    let paths = paths
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            DataFusionError::Internal(format!("{name} expected a string array for path"))
        })?;
    match kind {
        XpathTypedKind::Boolean => build_boolean_array(xmls, paths),
        XpathTypedKind::Double | XpathTypedKind::Number => build_float64_array(xmls, paths),
        XpathTypedKind::Float => build_float32_array(xmls, paths),
        XpathTypedKind::Int => build_int32_array(xmls, paths),
        XpathTypedKind::Long => build_int64_array(xmls, paths),
        XpathTypedKind::Short => build_int16_array(xmls, paths),
        XpathTypedKind::String => build_string_array(xmls, paths),
    }
}

fn evaluate_xpath_boolean(xml: &str, path: &str) -> Result<Option<bool>> {
    if xml.is_empty() || path.is_empty() {
        return Ok(None);
    }
    let wrapped = format!("boolean({path})");
    let mut documents = Documents::new();
    let document = documents
        .add_string_without_uri(xml)
        .map_err(|e| DataFusionError::Execution(format!("Invalid XML document: {e}")))?;
    let query = Queries::default()
        .one(&wrapped, |_, item| Ok(item.try_into_value::<bool>()?))
        .map_err(|e| DataFusionError::Execution(format!("Invalid XPath '{path}': {e}")))?;
    let result = query
        .execute(&mut documents, document)
        .map_err(|e| DataFusionError::Execution(format!("Error evaluating XPath '{path}': {e}")))?;
    Ok(Some(result))
}

fn evaluate_xpath_number(xml: &str, path: &str) -> Result<Option<f64>> {
    if xml.is_empty() || path.is_empty() {
        return Ok(None);
    }
    let wrapped = format!("number({path})");
    let mut documents = Documents::new();
    let document = documents
        .add_string_without_uri(xml)
        .map_err(|e| DataFusionError::Execution(format!("Invalid XML document: {e}")))?;
    let query = Queries::default()
        .one(&wrapped, |_, item| Ok(item.try_into_value::<f64>()?))
        .map_err(|e| DataFusionError::Execution(format!("Invalid XPath '{path}': {e}")))?;
    let result = query
        .execute(&mut documents, document)
        .map_err(|e| DataFusionError::Execution(format!("Error evaluating XPath '{path}': {e}")))?;
    Ok(Some(result))
}

fn evaluate_xpath_string(xml: &str, path: &str) -> Result<Option<String>> {
    if xml.is_empty() || path.is_empty() {
        return Ok(None);
    }
    let wrapped = format!("string({path})");
    let mut documents = Documents::new();
    let document = documents
        .add_string_without_uri(xml)
        .map_err(|e| DataFusionError::Execution(format!("Invalid XML document: {e}")))?;
    let query = Queries::default()
        .one(&wrapped, |_, item| Ok(item.try_into_value::<String>()?))
        .map_err(|e| DataFusionError::Execution(format!("Invalid XPath '{path}': {e}")))?;
    let result = query
        .execute(&mut documents, document)
        .map_err(|e| DataFusionError::Execution(format!("Error evaluating XPath '{path}': {e}")))?;
    Ok(Some(result))
}

fn build_boolean_array(xmls: &StringArray, paths: &StringArray) -> Result<ArrayRef> {
    let mut builder = BooleanBuilder::new();
    for row in 0..xmls.len() {
        if xmls.is_null(row) || paths.is_null(row) {
            builder.append_null();
        } else {
            match evaluate_xpath_boolean(xmls.value(row), paths.value(row))? {
                Some(v) => builder.append_value(v),
                None => builder.append_null(),
            }
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn build_float64_array(xmls: &StringArray, paths: &StringArray) -> Result<ArrayRef> {
    let mut builder = Float64Builder::new();
    for row in 0..xmls.len() {
        if xmls.is_null(row) || paths.is_null(row) {
            builder.append_null();
        } else {
            match evaluate_xpath_number(xmls.value(row), paths.value(row))? {
                Some(v) => builder.append_value(v),
                None => builder.append_null(),
            }
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn build_float32_array(xmls: &StringArray, paths: &StringArray) -> Result<ArrayRef> {
    let mut builder = Float32Builder::new();
    for row in 0..xmls.len() {
        if xmls.is_null(row) || paths.is_null(row) {
            builder.append_null();
        } else {
            match evaluate_xpath_number(xmls.value(row), paths.value(row))? {
                Some(v) => builder.append_value(v as f32),
                None => builder.append_null(),
            }
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn build_int32_array(xmls: &StringArray, paths: &StringArray) -> Result<ArrayRef> {
    let mut builder = Int32Builder::new();
    for row in 0..xmls.len() {
        if xmls.is_null(row) || paths.is_null(row) {
            builder.append_null();
        } else {
            match evaluate_xpath_number(xmls.value(row), paths.value(row))? {
                Some(v) => builder.append_value(v as i32),
                None => builder.append_null(),
            }
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn build_int64_array(xmls: &StringArray, paths: &StringArray) -> Result<ArrayRef> {
    let mut builder = Int64Builder::new();
    for row in 0..xmls.len() {
        if xmls.is_null(row) || paths.is_null(row) {
            builder.append_null();
        } else {
            match evaluate_xpath_number(xmls.value(row), paths.value(row))? {
                Some(v) => builder.append_value(v as i64),
                None => builder.append_null(),
            }
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn build_int16_array(xmls: &StringArray, paths: &StringArray) -> Result<ArrayRef> {
    let mut builder = Int16Builder::new();
    for row in 0..xmls.len() {
        if xmls.is_null(row) || paths.is_null(row) {
            builder.append_null();
        } else {
            match evaluate_xpath_number(xmls.value(row), paths.value(row))? {
                Some(v) => builder.append_value(v as i16),
                None => builder.append_null(),
            }
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn build_string_array(xmls: &StringArray, paths: &StringArray) -> Result<ArrayRef> {
    let mut builder = StringBuilder::new();
    for row in 0..xmls.len() {
        if xmls.is_null(row) || paths.is_null(row) {
            builder.append_null();
        } else {
            match evaluate_xpath_string(xmls.value(row), paths.value(row))? {
                Some(v) => builder.append_value(v),
                None => builder.append_null(),
            }
        }
    }
    Ok(Arc::new(builder.finish()))
}
