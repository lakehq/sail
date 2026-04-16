use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, BooleanBuilder, Float32Builder, Float64Builder, Int16Builder, Int32Builder,
    Int64Builder, StringArray, StringBuilder,
};
use datafusion::arrow::datatypes::DataType;
use datafusion_common::utils::take_function_args;
use datafusion_common::{plan_err, DataFusionError, Result};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
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

/// The kind of typed XPath function.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum XpathTypedKind {
    Boolean,
    Double,
    Float,
    Int,
    Long,
    Number,
    Short,
    String,
}

impl XpathTypedKind {
    fn name(self) -> &'static str {
        match self {
            Self::Boolean => "xpath_boolean",
            Self::Double => "xpath_double",
            Self::Float => "xpath_float",
            Self::Int => "xpath_int",
            Self::Long => "xpath_long",
            Self::Number => "xpath_number",
            Self::Short => "xpath_short",
            Self::String => "xpath_string",
        }
    }
    fn return_type(self) -> DataType {
        match self {
            Self::Boolean => DataType::Boolean,
            Self::Double | Self::Number => DataType::Float64,
            Self::Float => DataType::Float32,
            Self::Int => DataType::Int32,
            Self::Long => DataType::Int64,
            Self::Short => DataType::Int16,
            Self::String => DataType::Utf8,
        }
    }
}

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
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        self.kind.name()
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(self.kind.return_type())
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let name = self.kind.name();
        if arg_types.len() != 2 {
            return plan_err!(
                "`{name}` function requires 2 arguments, got {}",
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
                    plan_err!("The `{name}` function can only accept strings, but got {other:?}.")
                }
            })
            .collect()
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
