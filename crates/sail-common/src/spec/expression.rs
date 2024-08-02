use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::spec::data_type::DataType;
use crate::spec::literal::Literal;
use crate::spec::QueryPlan;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", rename_all_fields = "camelCase")]
pub enum Expr {
    Literal(Literal),
    UnresolvedAttribute {
        name: ObjectName,
        plan_id: Option<i64>,
    },
    UnresolvedFunction {
        function_name: String,
        arguments: Vec<Expr>,
        is_distinct: bool,
        is_user_defined_function: bool,
    },
    UnresolvedStar {
        target: Option<ObjectName>,
    },
    Alias {
        expr: Box<Expr>,
        /// A single identifier, or multiple identifiers for multi-alias.
        name: Vec<Identifier>,
        metadata: Option<HashMap<String, String>>,
    },
    Cast {
        expr: Box<Expr>,
        cast_to_type: DataType,
    },
    UnresolvedRegex {
        /// The regular expression to match column names.
        col_name: String,
        plan_id: Option<i64>,
    },
    SortOrder(SortOrder),
    LambdaFunction {
        function: Box<Expr>,
        arguments: Vec<UnresolvedNamedLambdaVariable>,
    },
    Window {
        window_function: Box<Expr>,
        partition_spec: Vec<Expr>,
        order_spec: Vec<SortOrder>,
        frame_spec: Option<WindowFrame>,
    },
    UnresolvedExtractValue {
        child: Box<Expr>,
        extraction: Box<Expr>,
    },
    UpdateFields {
        struct_expression: Box<Expr>,
        field_name: ObjectName,
        value_expression: Option<Box<Expr>>,
    },
    UnresolvedNamedLambdaVariable(UnresolvedNamedLambdaVariable),
    CommonInlineUserDefinedFunction(CommonInlineUserDefinedFunction),
    CallFunction {
        function_name: String,
        arguments: Vec<Expr>,
    },
    // extensions
    Placeholder(String),
    Rollup(Vec<Expr>),
    Cube(Vec<Expr>),
    GroupingSets(Vec<Vec<Expr>>),
    InSubquery {
        expr: Box<Expr>,
        subquery: Box<QueryPlan>,
        negated: bool,
    },
    ScalarSubquery {
        subquery: Box<QueryPlan>,
    },
    Exists {
        subquery: Box<QueryPlan>,
        negated: bool,
    },
    InList {
        expr: Box<Expr>,
        list: Vec<Expr>,
        negated: bool,
    },
    IsFalse(Box<Expr>),
    IsNotFalse(Box<Expr>),
    IsTrue(Box<Expr>),
    IsNotTrue(Box<Expr>),
    IsNull(Box<Expr>),
    IsNotNull(Box<Expr>),
    IsUnknown(Box<Expr>),
    IsNotUnknown(Box<Expr>),
    Between {
        expr: Box<Expr>,
        negated: bool,
        low: Box<Expr>,
        high: Box<Expr>,
    },
    IsDistinctFrom {
        left: Box<Expr>,
        right: Box<Expr>,
    },
    IsNotDistinctFrom {
        left: Box<Expr>,
        right: Box<Expr>,
    },
}

/// An identifier with only one part.
/// It is the raw value without quotes or escape characters.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Identifier(String);

impl From<String> for Identifier {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl From<Identifier> for String {
    fn from(id: Identifier) -> Self {
        id.0
    }
}

impl<'a> From<&'a Identifier> for &'a str {
    fn from(id: &'a Identifier) -> Self {
        id.0.as_str()
    }
}

/// An object name with potentially multiple parts.
/// Each part is a raw value without quotes or escape characters.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ObjectName(Vec<Identifier>);

impl From<Vec<String>> for ObjectName {
    fn from(name: Vec<String>) -> Self {
        Self(name.into_iter().map(Identifier::from).collect())
    }
}

impl From<ObjectName> for Vec<String> {
    fn from(name: ObjectName) -> Self {
        name.0.into_iter().map(String::from).collect()
    }
}

impl ObjectName {
    pub fn new_qualified(name: Identifier, mut qualifier: Vec<Identifier>) -> Self {
        qualifier.push(name);
        Self(qualifier)
    }

    pub fn new_unqualified(name: Identifier) -> Self {
        Self(vec![name])
    }

    pub fn child(self, name: Identifier) -> Self {
        let mut names = self.0;
        names.push(name);
        Self(names)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SortOrder {
    pub child: Box<Expr>,
    pub direction: SortDirection,
    pub null_ordering: NullOrdering,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum SortDirection {
    Unspecified,
    Ascending,
    Descending,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum NullOrdering {
    Unspecified,
    NullsFirst,
    NullsLast,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WindowFrame {
    pub frame_type: WindowFrameType,
    pub lower: WindowFrameBoundary,
    pub upper: WindowFrameBoundary,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum WindowFrameType {
    Undefined,
    Row,
    Range,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum WindowFrameBoundary {
    CurrentRow,
    Unbounded,
    Value(Box<Expr>),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CommonInlineUserDefinedFunction {
    pub function_name: String,
    pub deterministic: bool,
    pub arguments: Vec<Expr>,
    #[serde(flatten)]
    pub function: FunctionDefinition,
}

#[allow(clippy::enum_variant_names)]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", rename_all_fields = "camelCase")]
pub enum FunctionDefinition {
    PythonUdf {
        output_type: DataType,
        eval_type: i32,
        command: Vec<u8>,
        python_version: String,
    },
    ScalarScalaUdf {
        payload: Vec<u8>,
        input_types: Vec<DataType>,
        output_type: DataType,
        nullable: bool,
    },
    JavaUdf {
        class_name: String,
        output_type: Option<DataType>,
        aggregate: bool,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CommonInlineUserDefinedTableFunction {
    pub function_name: String,
    pub deterministic: bool,
    pub arguments: Vec<Expr>,
    #[serde(flatten)]
    pub function: TableFunctionDefinition,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", rename_all_fields = "camelCase")]
pub enum TableFunctionDefinition {
    PythonUdtf {
        return_type: DataType,
        eval_type: i32,
        command: Vec<u8>,
        python_version: String,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UnresolvedNamedLambdaVariable {
    pub name: ObjectName,
}
