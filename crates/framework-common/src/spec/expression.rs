use crate::spec::data_type::DataType;
use crate::spec::literal::Literal;
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    Literal(Literal),
    UnresolvedAttribute {
        unparsed_identifier: String,
        plan_id: Option<i64>,
    },
    UnresolvedFunction {
        function_name: String,
        arguments: Vec<Expr>,
        is_distinct: bool,
        is_user_defined_function: bool,
    },
    UnresolvedStar {
        unparsed_target: Option<String>,
    },
    Alias {
        expr: Box<Expr>,
        name: Vec<String>,
        metadata: Option<HashMap<String, String>>,
    },
    Cast {
        expr: Box<Expr>,
        cast_to_type: DataType,
    },
    UnresolvedRegex {
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
        field_name: String,
        value_expression: Option<Box<Expr>>,
    },
    UnresolvedNamedLambdaVariable {
        name_parts: Vec<String>,
    },
    CommonInlineUserDefinedFunction(CommonInlineUserDefinedFunction),
    CallFunction {
        function_name: String,
        arguments: Vec<Expr>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct SortOrder {
    pub child: Box<Expr>,
    pub direction: SortDirection,
    pub null_ordering: NullOrdering,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SortDirection {
    Unspecified,
    Ascending,
    Descending,
}

#[derive(Debug, Clone, PartialEq)]
pub enum NullOrdering {
    Unspecified,
    NullsFirst,
    NullsLast,
}

#[derive(Debug, Clone, PartialEq)]
pub struct WindowFrame {
    pub frame_type: WindowFrameType,
    pub lower: WindowFrameBoundary,
    pub upper: WindowFrameBoundary,
}

#[derive(Debug, Clone, PartialEq)]
pub enum WindowFrameType {
    Undefined,
    Row,
    Range,
}

#[derive(Debug, Clone, PartialEq)]
pub enum WindowFrameBoundary {
    CurrentRow,
    Unbounded,
    Value(Box<Expr>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct CommonInlineUserDefinedFunction {
    pub function_name: String,
    pub deterministic: bool,
    pub arguments: Vec<Expr>,
    pub function: FunctionType,
}

#[derive(Debug, Clone, PartialEq)]
pub enum FunctionType {
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

#[derive(Debug, Clone, PartialEq)]
pub struct CommonInlineUserDefinedTableFunction {
    pub function_name: String,
    pub deterministic: bool,
    pub arguments: Vec<Expr>,
    pub function: TableFunctionType,
}

#[derive(Debug, Clone, PartialEq)]
pub enum TableFunctionType {
    PythonUdtf {
        return_type: Option<DataType>,
        eval_type: i32,
        command: Vec<u8>,
        python_version: String,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct UnresolvedNamedLambdaVariable {
    pub name_parts: Vec<String>,
}
