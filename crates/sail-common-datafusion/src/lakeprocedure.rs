use std::collections::HashSet;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::catalog::Session;
use datafusion::common::Result;
use datafusion_common::plan_datafusion_err;
use datafusion_expr::LogicalPlan;
use serde::{Deserialize, Serialize};

use crate::catalog::{LakehouseOperation, LakehouseTableBinding};
use crate::datasource::SourceInfo;

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

    fn accepts(self, value: &LakeProcedureValue) -> bool {
        matches!(
            (self, value),
            (_, LakeProcedureValue::Null)
                | (Self::Boolean, LakeProcedureValue::Boolean(_))
                | (Self::Int32, LakeProcedureValue::Int32(_))
                | (Self::Int64, LakeProcedureValue::Int64(_))
                | (Self::Utf8, LakeProcedureValue::Utf8(_))
                | (
                    Self::TimestampMicros,
                    LakeProcedureValue::TimestampMicros(_)
                )
        )
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
        if self.name.is_empty() {
            return Err(plan_datafusion_err!(
                "lake procedure name must not be empty"
            ));
        }

        let mut parameter_names = HashSet::with_capacity(self.parameters.len());
        for parameter in &self.parameters {
            if parameter.name.is_empty() {
                return Err(plan_datafusion_err!(
                    "lake procedure '{}' has an empty parameter name",
                    self.name
                ));
            }
            if !parameter_names.insert(parameter.name.to_ascii_lowercase()) {
                return Err(plan_datafusion_err!(
                    "lake procedure '{}' has duplicate parameter '{}'",
                    self.name,
                    parameter.name
                ));
            }
        }

        let mut output_names = HashSet::with_capacity(self.output.len());
        for field in &self.output {
            if field.name.is_empty() {
                return Err(plan_datafusion_err!(
                    "lake procedure '{}' has an empty output field name",
                    self.name
                ));
            }
            if !output_names.insert(field.name.to_ascii_lowercase()) {
                return Err(plan_datafusion_err!(
                    "lake procedure '{}' has duplicate output field '{}'",
                    self.name,
                    field.name
                ));
            }
        }

        if let LakeProcedureTarget::Table { parameter } = &self.target {
            let target = self
                .parameters
                .iter()
                .find(|candidate| candidate.name.eq_ignore_ascii_case(parameter))
                .ok_or_else(|| {
                    plan_datafusion_err!(
                        "lake procedure '{}' target parameter '{}' is not declared",
                        self.name,
                        parameter
                    )
                })?;
            if target.data_type != LakeProcedureDataType::Utf8 || !target.required {
                return Err(plan_datafusion_err!(
                    "lake procedure '{}' target parameter '{}' must be a required string",
                    self.name,
                    target.name
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
    pub lake_source: String,
    pub target: Option<LakeProcedureTableTarget>,
    pub invocation: LakeProcedureInvocation,
}

impl LakeProcedureCall {
    pub fn validate(&self) -> Result<()> {
        self.invocation.procedure.validate()?;
        if self.invocation_id.0.is_empty() {
            return Err(plan_datafusion_err!(
                "lake procedure invocation ID must not be empty"
            ));
        }
        if self.catalog.is_empty()
            || self.lake_source.is_empty()
            || self.namespace.is_empty()
            || self.namespace.iter().any(String::is_empty)
        {
            return Err(plan_datafusion_err!(
                "lake procedure invocation must identify its catalog, namespace, and lake source"
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
            if !parameter.data_type.accepts(value) {
                return Err(plan_datafusion_err!(
                    "lake procedure '{}' argument '{}' does not match its descriptor",
                    self.invocation.procedure.name,
                    parameter.name
                ));
            }
        }
        match (&self.invocation.procedure.target, &self.target) {
            (LakeProcedureTarget::Catalog, None) => {}
            (LakeProcedureTarget::Table { parameter }, Some(target)) => {
                if !matches!(
                    self.invocation.argument(parameter),
                    Some(LakeProcedureValue::Utf8(_))
                ) {
                    return Err(plan_datafusion_err!(
                        "lake procedure target argument '{}' must be a non-null string",
                        parameter
                    ));
                }
                let binding = &target.binding;
                if !binding
                    .catalog_table
                    .first()
                    .is_some_and(|catalog| catalog.eq_ignore_ascii_case(&self.catalog))
                    || !binding
                        .format
                        .as_str()
                        .eq_ignore_ascii_case(&self.lake_source)
                    || binding.operation != LakehouseOperation::Maintenance
                {
                    return Err(plan_datafusion_err!(
                        "lake procedure table binding does not match its catalog, lake source, or maintenance operation"
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

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn catalog_procedure() -> LakeProcedure {
        LakeProcedure {
            name: "inspect".to_string(),
            parameters: vec![LakeProcedureParameter::required(
                "snapshot_id",
                LakeProcedureDataType::Int64,
            )],
            output: vec![LakeProcedureField::new(
                "snapshot_id",
                LakeProcedureDataType::Int64,
                false,
            )],
            access: LakeProcedureAccess::MetadataRead,
            target: LakeProcedureTarget::Catalog,
            retry_policy: LakeProcedureRetryPolicy::Safe,
        }
    }

    fn catalog_call(
        procedure: LakeProcedure,
        arguments: Vec<LakeProcedureValue>,
    ) -> LakeProcedureCall {
        LakeProcedureCall {
            invocation_id: LakeProcedureInvocationId("invocation-1".to_string()),
            catalog: "test".to_string(),
            namespace: vec!["system".to_string()],
            lake_source: "iceberg".to_string(),
            target: None,
            invocation: LakeProcedureInvocation {
                procedure,
                arguments,
            },
        }
    }

    #[test]
    fn procedure_descriptor_rejects_case_insensitive_duplicate_names() {
        let mut procedure = catalog_procedure();
        procedure.parameters.push(LakeProcedureParameter::optional(
            "SNAPSHOT_ID",
            LakeProcedureDataType::Int64,
        ));

        let error = procedure.validate().unwrap_err().to_string();

        assert!(error.contains("duplicate parameter 'SNAPSHOT_ID'"));
    }

    #[test]
    fn procedure_descriptor_requires_a_declared_string_table_target() {
        let mut procedure = catalog_procedure();
        procedure.target = LakeProcedureTarget::table("table");

        let error = procedure.validate().unwrap_err().to_string();

        assert!(error.contains("target parameter 'table' is not declared"));
    }

    #[test]
    fn procedure_call_rejects_argument_type_mismatch() {
        let call = catalog_call(
            catalog_procedure(),
            vec![LakeProcedureValue::Utf8("not-a-snapshot".to_string())],
        );

        let error = call.validate().unwrap_err().to_string();

        assert!(error.contains("argument 'snapshot_id' does not match"));
    }

    #[test]
    fn procedure_call_accepts_a_null_value_for_optional_arguments() {
        let mut procedure = catalog_procedure();
        procedure.parameters[0].required = false;
        let call = catalog_call(procedure, vec![LakeProcedureValue::Null]);

        call.validate().unwrap();
    }

    #[test]
    fn procedure_call_accepts_an_explicit_null_for_a_required_argument() {
        let call = catalog_call(catalog_procedure(), vec![LakeProcedureValue::Null]);

        call.validate().unwrap();
    }
}

/// Catalog object prepared for a format-owned procedure during logical planning.
///
/// A table target contains the access context and source metadata needed to build scans, writes,
/// exchanges, and commits. It is intentionally separate from [`LakeProcedureCall`], which only
/// carries a stable serializable binding into physical execution.
#[derive(Debug, Clone)]
pub enum LakeProcedurePlanningTarget {
    Catalog { catalog: String },
    Table(Box<SourceInfo>),
}

/// Where the root stage of a planned procedure runs.
///
/// This does not constrain the provider-owned implementation. A coordinator root can still have
/// distributed worker stages separated by exchanges before its final coordinator stage.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash, PartialOrd, Ord)]
pub enum LakeProcedureRootPlacement {
    Coordinator,
    Distributed,
}

/// Provider-owned implementation plan plus engine scheduling requirements for one invocation.
#[derive(Debug, Clone)]
pub struct LakeProcedurePlan {
    pub implementation: LogicalPlan,
    pub root_placement: LakeProcedureRootPlacement,
}

impl LakeProcedurePlan {
    pub fn coordinator(implementation: LogicalPlan) -> Self {
        Self {
            implementation,
            root_placement: LakeProcedureRootPlacement::Coordinator,
        }
    }

    pub fn distributed(implementation: LogicalPlan) -> Self {
        Self {
            implementation,
            root_placement: LakeProcedureRootPlacement::Distributed,
        }
    }
}

/// Runtime object freshly rebound for a format-owned local procedure implementation.
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

    /// Plans the format-owned implementation of a fully bound procedure call.
    ///
    /// The returned implementation becomes the input of the engine-owned procedure boundary. It
    /// may be a local command, a distributed relational plan, or a distributed plan followed by a
    /// coordinator commit. The plan also selects root placement for this invocation. Every
    /// physical extension produced from it must be supported by the remote physical-plan codec.
    async fn plan_procedure(
        &self,
        session: &dyn Session,
        target: LakeProcedurePlanningTarget,
        call: &LakeProcedureCall,
    ) -> Result<LakeProcedurePlan>;
}
