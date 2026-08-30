use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::common::Result;
use datafusion::execution::TaskContext;
use datafusion_common::plan_datafusion_err;
use serde::{Deserialize, Serialize};

use crate::catalog::LakehouseTableBinding;
use crate::datasource::SourceInfo;
use crate::lakeformat::LakeFormatId;

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum LakeProcedureDataType {
    Boolean,
    Int32,
    Int64,
    Utf8,
    TimestampMicros,
}

impl LakeProcedureDataType {
    pub fn arrow_type(self) -> DataType {
        match self {
            Self::Boolean => DataType::Boolean,
            Self::Int32 => DataType::Int32,
            Self::Int64 => DataType::Int64,
            Self::Utf8 => DataType::Utf8,
            Self::TimestampMicros => DataType::Timestamp(TimeUnit::Microsecond, None),
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum LakeProcedureValue {
    Null,
    Boolean(bool),
    Int32(i32),
    Int64(i64),
    Utf8(String),
    TimestampMicros(i64),
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct LakeProcedureParameter {
    pub name: String,
    pub data_type: LakeProcedureDataType,
    pub required: bool,
}

impl LakeProcedureParameter {
    pub fn required(name: impl Into<String>, data_type: LakeProcedureDataType) -> Self {
        Self {
            name: name.into(),
            data_type,
            required: true,
        }
    }

    pub fn optional(name: impl Into<String>, data_type: LakeProcedureDataType) -> Self {
        Self {
            name: name.into(),
            data_type,
            required: false,
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct LakeProcedureField {
    pub name: String,
    pub data_type: LakeProcedureDataType,
    pub nullable: bool,
}

impl LakeProcedureField {
    pub fn new(name: impl Into<String>, data_type: LakeProcedureDataType, nullable: bool) -> Self {
        Self {
            name: name.into(),
            data_type,
            nullable,
        }
    }

    fn arrow_field(&self) -> Field {
        Field::new(&self.name, self.data_type.arrow_type(), self.nullable)
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum LakeProcedureAccess {
    MetadataRead,
    MetadataCommit,
}

/// Object selected by a procedure descriptor.
#[derive(Debug, Clone, Eq, PartialEq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum LakeProcedureTarget {
    Catalog,
    Table { parameter: String },
}

impl LakeProcedureTarget {
    pub fn table(parameter: impl Into<String>) -> Self {
        Self::Table {
            parameter: parameter.into(),
        }
    }

    pub fn table_parameter(&self) -> Option<&str> {
        match self {
            Self::Catalog => None,
            Self::Table { parameter } => Some(parameter),
        }
    }
}

/// Whether the distributed scheduler may automatically re-run the procedure.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum LakeProcedureRetryPolicy {
    Safe,
    Forbidden,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct LakeProcedure {
    pub name: String,
    pub parameters: Vec<LakeProcedureParameter>,
    pub output: Vec<LakeProcedureField>,
    pub access: LakeProcedureAccess,
    pub target: LakeProcedureTarget,
    pub retry_policy: LakeProcedureRetryPolicy,
}

impl LakeProcedure {
    pub fn schema(&self) -> SchemaRef {
        Arc::new(Schema::new(
            self.output
                .iter()
                .map(LakeProcedureField::arrow_field)
                .collect::<Vec<_>>(),
        ))
    }

    pub fn validate(&self) -> Result<()> {
        if self.name.trim().is_empty() {
            return Err(plan_datafusion_err!("lake procedure name cannot be empty"));
        }
        let mut parameter_names = std::collections::HashSet::new();
        for parameter in &self.parameters {
            let name = parameter.name.trim().to_ascii_lowercase();
            if name.is_empty()
                || parameter.name.trim() != parameter.name
                || !parameter_names.insert(name)
            {
                return Err(plan_datafusion_err!(
                    "lake procedure '{}' has an invalid or duplicate parameter '{}'",
                    self.name,
                    parameter.name
                ));
            }
        }
        let mut output_names = std::collections::HashSet::new();
        for field in &self.output {
            let name = field.name.trim().to_ascii_lowercase();
            if name.is_empty() || field.name.trim() != field.name || !output_names.insert(name) {
                return Err(plan_datafusion_err!(
                    "lake procedure '{}' has an invalid or duplicate output field '{}'",
                    self.name,
                    field.name
                ));
            }
        }
        if let LakeProcedureTarget::Table { parameter } = &self.target {
            let Some(target) = self
                .parameters
                .iter()
                .find(|candidate| candidate.name.eq_ignore_ascii_case(parameter))
            else {
                return Err(plan_datafusion_err!(
                    "lake procedure '{}' target parameter '{}' is not declared",
                    self.name,
                    parameter
                ));
            };
            if !target.required || target.data_type != LakeProcedureDataType::Utf8 {
                return Err(plan_datafusion_err!(
                    "lake procedure '{}' target parameter '{}' must be a required string",
                    self.name,
                    parameter
                ));
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct LakeProcedureInvocation {
    pub procedure: LakeProcedure,
    pub arguments: Vec<LakeProcedureValue>,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct LakeProcedureInvocationId(pub String);

/// Stable table binding captured while resolving a procedure call.
#[derive(Debug, Clone, Eq, PartialEq, Hash, PartialOrd, Serialize, Deserialize)]
pub struct LakeProcedureTableTarget {
    pub binding: LakehouseTableBinding,
}

/// Fully bound, serializable procedure call passed from planning to execution.
#[derive(Debug, Clone, Eq, PartialEq, Hash, PartialOrd, Serialize, Deserialize)]
pub struct LakeProcedureCall {
    pub invocation_id: LakeProcedureInvocationId,
    pub catalog: String,
    pub namespace: Vec<String>,
    pub format_id: LakeFormatId,
    pub target: Option<LakeProcedureTableTarget>,
    pub invocation: LakeProcedureInvocation,
}

impl LakeProcedureCall {
    pub fn validate(&self) -> Result<()> {
        self.invocation.procedure.validate()?;
        if self.invocation_id.0.trim().is_empty() {
            return Err(plan_datafusion_err!(
                "lake procedure invocation identity cannot be empty"
            ));
        }
        if self.catalog.trim().is_empty() {
            return Err(plan_datafusion_err!(
                "lake procedure catalog cannot be empty"
            ));
        }
        if self.invocation.arguments.len() != self.invocation.procedure.parameters.len() {
            return Err(plan_datafusion_err!(
                "lake procedure '{}' argument count does not match its descriptor",
                self.invocation.procedure.name
            ));
        }
        for (parameter, value) in self
            .invocation
            .procedure
            .parameters
            .iter()
            .zip(&self.invocation.arguments)
        {
            if matches!(value, LakeProcedureValue::Null) && parameter.required {
                return Err(plan_datafusion_err!(
                    "required lake procedure argument '{}' cannot be null",
                    parameter.name
                ));
            }
            if !matches!(value, LakeProcedureValue::Null)
                && procedure_value_type(value) != Some(parameter.data_type)
            {
                return Err(plan_datafusion_err!(
                    "lake procedure argument '{}' does not match its descriptor type",
                    parameter.name
                ));
            }
        }
        match (&self.invocation.procedure.target, &self.target) {
            (LakeProcedureTarget::Catalog, None) => {}
            (LakeProcedureTarget::Table { .. }, Some(target)) => {
                if !target
                    .binding
                    .catalog_table
                    .first()
                    .is_some_and(|catalog| catalog.eq_ignore_ascii_case(&self.catalog))
                {
                    return Err(plan_datafusion_err!(
                        "lake procedure target catalog does not match its procedure catalog"
                    ));
                }
                if LakeFormatId::try_new(target.binding.format.as_str())? != self.format_id {
                    return Err(plan_datafusion_err!(
                        "lake procedure target format does not match plugin '{}'",
                        self.format_id
                    ));
                }
            }
            _ => {
                return Err(plan_datafusion_err!(
                    "lake procedure target does not match its descriptor"
                ));
            }
        }
        Ok(())
    }
}

fn procedure_value_type(value: &LakeProcedureValue) -> Option<LakeProcedureDataType> {
    match value {
        LakeProcedureValue::Null => None,
        LakeProcedureValue::Boolean(_) => Some(LakeProcedureDataType::Boolean),
        LakeProcedureValue::Int32(_) => Some(LakeProcedureDataType::Int32),
        LakeProcedureValue::Int64(_) => Some(LakeProcedureDataType::Int64),
        LakeProcedureValue::Utf8(_) => Some(LakeProcedureDataType::Utf8),
        LakeProcedureValue::TimestampMicros(_) => Some(LakeProcedureDataType::TimestampMicros),
    }
}

/// Runtime object handed to a format-owned procedure implementation.
#[derive(Debug, Clone)]
pub enum LakeProcedureExecutionTarget {
    Catalog { catalog: String },
    Table(Box<SourceInfo>),
}

impl LakeProcedureInvocation {
    pub fn argument(&self, name: &str) -> Option<&LakeProcedureValue> {
        self.procedure
            .parameters
            .iter()
            .position(|parameter| parameter.name.eq_ignore_ascii_case(name))
            .and_then(|index| self.arguments.get(index))
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum LakeProcedureResolution {
    Unrecognized,
    Unsupported { reason: String },
    Supported(LakeProcedure),
}

#[async_trait]
pub trait LakeProcedureProvider: Send + Sync {
    fn resolve_procedure(&self, namespace: &[String], name: &str) -> LakeProcedureResolution;

    async fn execute_procedure(
        &self,
        ctx: &TaskContext,
        target: LakeProcedureExecutionTarget,
        invocation: LakeProcedureInvocation,
    ) -> Result<RecordBatch>;
}
