use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::common::Result;
use datafusion::execution::TaskContext;
use serde::{Deserialize, Serialize};

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

#[derive(Debug, Clone, Eq, PartialEq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct LakeProcedure {
    pub name: String,
    pub parameters: Vec<LakeProcedureParameter>,
    pub output: Vec<LakeProcedureField>,
    pub access: LakeProcedureAccess,
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
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct LakeProcedureInvocation {
    pub procedure: LakeProcedure,
    pub arguments: Vec<LakeProcedureValue>,
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

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum LakeProcedureCatalogResolution {
    Unrecognized,
    Ambiguous {
        lake_sources: Vec<String>,
    },
    Unsupported {
        lake_source: String,
        reason: String,
    },
    Supported {
        lake_source: String,
        procedure: LakeProcedure,
    },
}

#[async_trait]
pub trait LakeProcedureProvider: Send + Sync {
    fn resolve_procedure(&self, namespace: &[String], name: &str) -> LakeProcedureResolution;

    async fn execute_procedure(
        &self,
        ctx: &TaskContext,
        info: SourceInfo,
        invocation: LakeProcedureInvocation,
    ) -> Result<RecordBatch>;
}
