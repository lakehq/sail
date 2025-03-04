use num_enum::{IntoPrimitive, TryFromPrimitive};
use serde::{Deserialize, Serialize};

use crate::error::CommonError;
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
    UnresolvedFunction(UnresolvedFunction),
    UnresolvedStar {
        target: Option<ObjectName>,
        wildcard_options: WildcardOptions,
    },
    Alias {
        expr: Box<Expr>,
        /// A single identifier, or multiple identifiers for multi-alias.
        name: Vec<Identifier>,
        metadata: Option<Vec<(String, String)>>,
    },
    Cast {
        expr: Box<Expr>,
        cast_to_type: DataType,
        /// Whether to rename the expression to `CAST(... AS ...)`.
        rename: bool,
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
        window: Window,
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
        function_name: ObjectName,
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
    SimilarTo {
        expr: Box<Expr>,
        pattern: Box<Expr>,
        negated: bool,
        escape_char: Option<char>,
        case_insensitive: bool,
    },
    Table {
        expr: Box<Expr>,
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

impl From<&str> for Identifier {
    fn from(s: &str) -> Self {
        Self(s.to_string())
    }
}

impl From<Identifier> for String {
    fn from(id: Identifier) -> Self {
        id.0
    }
}

impl AsRef<str> for Identifier {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

/// An object name with potentially multiple parts.
/// Each part is a raw value without quotes or escape characters.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ObjectName(Vec<Identifier>);

impl ObjectName {
    pub fn bare(name: impl Into<Identifier>) -> Self {
        Self(vec![name.into()])
    }

    pub fn child(self, name: impl Into<Identifier>) -> Self {
        let mut names = self.0;
        names.push(name.into());
        Self(names)
    }

    pub fn parts(&self) -> &[Identifier] {
        &self.0
    }
}

impl<T, S> From<T> for ObjectName
where
    T: IntoIterator<Item = S>,
    S: Into<Identifier>,
{
    fn from(value: T) -> Self {
        Self(value.into_iter().map(|x| x.into()).collect())
    }
}

impl From<ObjectName> for Vec<String> {
    fn from(name: ObjectName) -> Self {
        name.0.into_iter().map(String::from).collect()
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
pub struct UnresolvedFunction {
    pub function_name: ObjectName,
    /// A list of positional arguments.
    pub arguments: Vec<Expr>,
    /// A list of named arguments, which must come after positional arguments.
    pub named_arguments: Vec<(Identifier, Expr)>,
    pub is_distinct: bool,
    pub is_user_defined_function: bool,
    pub ignore_nulls: Option<bool>,
    pub filter: Option<Box<Expr>>,
    pub order_by: Option<Vec<SortOrder>>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", rename_all_fields = "camelCase")]
pub enum Window {
    Named(Identifier),
    Unnamed {
        cluster_by: Vec<Expr>,
        partition_by: Vec<Expr>,
        order_by: Vec<SortOrder>,
        frame: Option<WindowFrame>,
    },
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
    Row,
    Range,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum WindowFrameBoundary {
    CurrentRow,
    UnboundedPreceding,
    UnboundedFollowing,
    Preceding(Box<Expr>),
    Following(Box<Expr>),
    /// An alternative way to specify a window frame boundary, where
    /// a negative value is a preceding boundary and a positive value is a following boundary.
    Value(Box<Expr>),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CommonInlineUserDefinedFunction {
    pub function_name: Identifier,
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
        eval_type: PySparkUdfType,
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
    pub function_name: Identifier,
    pub deterministic: bool,
    pub arguments: Vec<Expr>,
    #[serde(flatten)]
    pub function: TableFunctionDefinition,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", rename_all_fields = "camelCase")]
pub enum TableFunctionDefinition {
    PythonUdtf {
        return_type: DataType,
        eval_type: PySparkUdfType,
        command: Vec<u8>,
        python_version: String,
    },
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Serialize,
    Deserialize,
    TryFromPrimitive,
    IntoPrimitive,
)]
#[serde(rename_all = "camelCase")]
#[num_enum(error_type(name = CommonError, constructor = PySparkUdfType::invalid))]
#[repr(i32)]
pub enum PySparkUdfType {
    None = 0,
    Batched = 100,
    ArrowBatched = 101,
    ScalarPandas = 200,
    GroupedMapPandas = 201,
    GroupedAggPandas = 202,
    WindowAggPandas = 203,
    ScalarPandasIter = 204,
    MapPandasIter = 205,
    CogroupedMapPandas = 206,
    MapArrowIter = 207,
    GroupedMapPandasWithState = 208,
    Table = 300,
    ArrowTable = 301,
}

impl PySparkUdfType {
    fn invalid(v: i32) -> CommonError {
        CommonError::invalid(format!("invalid PySpark UDF type: {}", v))
    }

    pub fn is_table_function(&self) -> bool {
        matches!(self, PySparkUdfType::Table | PySparkUdfType::ArrowTable)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UnresolvedNamedLambdaVariable {
    pub name: ObjectName,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct WildcardOptions {
    pub ilike_pattern: Option<String>,
    pub exclude_columns: Option<Vec<Identifier>>,
    pub except_columns: Option<Vec<Identifier>>,
    pub replace_columns: Option<Vec<WildcardReplaceColumn>>,
    pub rename_columns: Option<Vec<IdentifierWithAlias>>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WildcardReplaceColumn {
    pub expression: Box<Expr>,
    pub column_name: Identifier,
    pub as_keyword: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IdentifierWithAlias {
    pub identifier: Identifier,
    pub alias: Identifier,
}
