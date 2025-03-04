use std::collections::HashMap;

use sail_common::spec;
use sail_sql_analyzer::data_type::from_ast_data_type;
use sail_sql_analyzer::expression::{
    from_ast_expression, from_ast_object_name, from_ast_qualified_wildcard,
};
use sail_sql_analyzer::parser::{
    parse_data_type, parse_expression, parse_named_expression, parse_object_name,
    parse_qualified_wildcard,
};
use sail_sql_analyzer::query::from_ast_named_expression;

use crate::error::{ProtoFieldExt, SparkError, SparkResult};
use crate::spark::connect::expression::cast::CastToType;
use crate::spark::connect::expression::sort_order::{NullOrdering, SortDirection};
use crate::spark::connect::expression::window::window_frame::frame_boundary::Boundary;
use crate::spark::connect::expression::window::window_frame::{FrameBoundary, FrameType};
use crate::spark::connect::expression::window::WindowFrame;
use crate::spark::connect::expression::{
    Alias, Cast, ExprType, ExpressionString, LambdaFunction, SortOrder, UnresolvedAttribute,
    UnresolvedExtractValue, UnresolvedFunction, UnresolvedNamedLambdaVariable, UnresolvedRegex,
    UnresolvedStar, UpdateFields, Window,
};
use crate::spark::connect::{
    common_inline_user_defined_function as udf, common_inline_user_defined_table_function as udtf,
    CallFunction, CommonInlineUserDefinedFunction, CommonInlineUserDefinedTableFunction,
    Expression, JavaUdf, PythonUdf, PythonUdtf, ScalarScalaUdf,
};

impl TryFrom<Expression> for spec::Expr {
    type Error = SparkError;

    fn try_from(expr: Expression) -> SparkResult<spec::Expr> {
        let Expression { expr_type } = expr;
        let expr_type = expr_type.required("expression type")?;
        match expr_type {
            ExprType::Literal(literal) => Ok(spec::Expr::Literal(literal.try_into()?)),
            ExprType::UnresolvedAttribute(UnresolvedAttribute {
                unparsed_identifier,
                plan_id,
            }) => {
                // The unparsed identifier such as `a.b` is supposed to be parsed as nested
                // object names. However, there may be raw identifier such as `array(1)` which
                // cannot be parsed. Therefore, when parsing fails, we create an object name
                // containing the single raw identifier.
                let name = parse_object_name(unparsed_identifier.as_str())
                    .and_then(from_ast_object_name)
                    .unwrap_or_else(|_| spec::ObjectName::from(vec![unparsed_identifier]));
                Ok(spec::Expr::UnresolvedAttribute { name, plan_id })
            }
            ExprType::UnresolvedFunction(UnresolvedFunction {
                function_name,
                arguments,
                is_distinct,
                is_user_defined_function,
            }) => Ok(spec::Expr::UnresolvedFunction(spec::UnresolvedFunction {
                function_name: spec::ObjectName::bare(function_name),
                arguments: arguments
                    .into_iter()
                    .map(|x| x.try_into())
                    .collect::<SparkResult<_>>()?,
                named_arguments: vec![],
                is_distinct,
                is_user_defined_function,
                ignore_nulls: None,
                filter: None,
                order_by: None,
            })),
            ExprType::ExpressionString(ExpressionString { expression }) => {
                let expr = parse_expression(expression.as_str())
                    .and_then(from_ast_expression)
                    .or_else(|_| {
                        parse_named_expression(expression.as_str())
                            .and_then(from_ast_named_expression)
                    })?;
                Ok(expr)
            }
            ExprType::UnresolvedStar(UnresolvedStar { unparsed_target }) => {
                let target = unparsed_target
                    .map(|x| from_ast_qualified_wildcard(parse_qualified_wildcard(x.as_str())?))
                    .transpose()?;
                Ok(spec::Expr::UnresolvedStar {
                    target,
                    wildcard_options: Default::default(),
                })
            }
            ExprType::Alias(alias) => {
                let Alias {
                    expr,
                    name,
                    metadata,
                } = *alias;
                let expr = expr.required("alias expression")?;
                let metadata: Option<HashMap<String, String>> = metadata
                    .map(|x| {
                        serde_json::from_str(&x).map_err(SparkError::from).and_then(
                            |x: serde_json::Value| {
                                x.as_object()
                                    .ok_or_else(|| SparkError::invalid("alias metadata"))
                                    .map(|x| {
                                        x.into_iter()
                                            .map(|(k, v)| (k.clone(), v.to_string()))
                                            .collect()
                                    })
                            },
                        )
                    })
                    .transpose()?;
                let name: Vec<spec::Identifier> = name.into_iter().map(|x| x.into()).collect();
                Ok(spec::Expr::Alias {
                    expr: Box::new((*expr).try_into()?),
                    name,
                    metadata: metadata.map(|x| x.into_iter().collect()),
                })
            }
            ExprType::Cast(cast) => {
                let Cast { expr, cast_to_type } = *cast;
                let expr = expr.required("cast expression")?;
                let cast_to_type = cast_to_type.required("cast type")?;
                let cast_to_type = match cast_to_type {
                    CastToType::Type(x) => x.try_into()?,
                    CastToType::TypeStr(s) => from_ast_data_type(parse_data_type(s.as_str())?)?,
                };
                Ok(spec::Expr::Cast {
                    expr: Box::new((*expr).try_into()?),
                    cast_to_type,
                    rename: false,
                })
            }
            ExprType::UnresolvedRegex(UnresolvedRegex { col_name, plan_id }) => {
                Ok(spec::Expr::UnresolvedRegex { col_name, plan_id })
            }
            ExprType::SortOrder(sort_order) => Ok(spec::Expr::SortOrder((*sort_order).try_into()?)),
            ExprType::LambdaFunction(lambda) => {
                let LambdaFunction {
                    function,
                    arguments,
                } = *lambda;
                let function = function.required("lambda function")?;
                Ok(spec::Expr::LambdaFunction {
                    function: Box::new((*function).try_into()?),
                    arguments: arguments
                        .into_iter()
                        .map(|x| x.try_into())
                        .collect::<SparkResult<_>>()?,
                })
            }
            ExprType::Window(window) => {
                let Window {
                    window_function,
                    partition_spec,
                    order_spec,
                    frame_spec,
                } = *window;
                let window_function = window_function.required("window function")?;
                Ok(spec::Expr::Window {
                    window_function: Box::new((*window_function).try_into()?),
                    window: spec::Window::Unnamed {
                        cluster_by: vec![],
                        partition_by: partition_spec
                            .into_iter()
                            .map(|x| x.try_into())
                            .collect::<SparkResult<_>>()?,
                        order_by: order_spec
                            .into_iter()
                            .map(|x| x.try_into())
                            .collect::<SparkResult<_>>()?,
                        frame: frame_spec.map(|x| (*x).try_into()).transpose()?,
                    },
                })
            }
            ExprType::UnresolvedExtractValue(extract) => {
                let UnresolvedExtractValue { child, extraction } = *extract;
                let child = child.required("child")?;
                let extraction = extraction.required("extraction")?;
                Ok(spec::Expr::UnresolvedExtractValue {
                    child: Box::new((*child).try_into()?),
                    extraction: Box::new((*extraction).try_into()?),
                })
            }
            ExprType::UpdateFields(update) => {
                let UpdateFields {
                    struct_expression,
                    field_name,
                    value_expression,
                } = *update;
                let struct_expression = struct_expression.required("struct expression")?;
                Ok(spec::Expr::UpdateFields {
                    struct_expression: Box::new((*struct_expression).try_into()?),
                    field_name: from_ast_object_name(parse_object_name(field_name.as_str())?)?,
                    value_expression: value_expression
                        .map(|x| -> SparkResult<_> { Ok(Box::new((*x).try_into()?)) })
                        .transpose()?,
                })
            }
            ExprType::UnresolvedNamedLambdaVariable(variable) => Ok(
                spec::Expr::UnresolvedNamedLambdaVariable(variable.try_into()?),
            ),
            ExprType::CommonInlineUserDefinedFunction(function) => Ok(
                spec::Expr::CommonInlineUserDefinedFunction(function.try_into()?),
            ),
            ExprType::CallFunction(CallFunction {
                function_name,
                arguments,
            }) => {
                let function_name =
                    from_ast_object_name(parse_object_name(function_name.as_str())?)?;
                Ok(spec::Expr::CallFunction {
                    function_name,
                    arguments: arguments
                        .into_iter()
                        .map(|x| x.try_into())
                        .collect::<SparkResult<_>>()?,
                })
            }
            ExprType::Extension(_) => Err(SparkError::todo("extension expression")),
        }
    }
}

impl TryFrom<SortOrder> for spec::SortOrder {
    type Error = SparkError;

    fn try_from(sort_order: SortOrder) -> SparkResult<spec::SortOrder> {
        let SortOrder {
            child,
            direction,
            null_ordering,
        } = sort_order;
        let child = child.required("sort order expression")?;
        let direction = SortDirection::try_from(direction)?;
        let null_ordering = NullOrdering::try_from(null_ordering)?;
        Ok(spec::SortOrder {
            child: Box::new((*child).try_into()?),
            direction: direction.try_into()?,
            null_ordering: null_ordering.try_into()?,
        })
    }
}

impl TryFrom<SortDirection> for spec::SortDirection {
    type Error = SparkError;

    fn try_from(sort_direction: SortDirection) -> SparkResult<spec::SortDirection> {
        match sort_direction {
            SortDirection::Unspecified => Ok(spec::SortDirection::Unspecified),
            SortDirection::Ascending => Ok(spec::SortDirection::Ascending),
            SortDirection::Descending => Ok(spec::SortDirection::Descending),
        }
    }
}

impl TryFrom<NullOrdering> for spec::NullOrdering {
    type Error = SparkError;

    fn try_from(null_ordering: NullOrdering) -> SparkResult<spec::NullOrdering> {
        match null_ordering {
            NullOrdering::SortNullsUnspecified => Ok(spec::NullOrdering::Unspecified),
            NullOrdering::SortNullsFirst => Ok(spec::NullOrdering::NullsFirst),
            NullOrdering::SortNullsLast => Ok(spec::NullOrdering::NullsLast),
        }
    }
}

impl TryFrom<WindowFrame> for spec::WindowFrame {
    type Error = SparkError;

    fn try_from(window_frame: WindowFrame) -> SparkResult<spec::WindowFrame> {
        fn boundary(boundary: Option<Box<FrameBoundary>>) -> Option<Boundary> {
            boundary.and_then(|x| {
                let FrameBoundary { boundary } = *x;
                boundary
            })
        }

        let WindowFrame {
            frame_type,
            lower,
            upper,
        } = window_frame;
        let frame_type = FrameType::try_from(frame_type)?;
        let lower = boundary(lower).required("lower window frame boundary")?;
        let upper = boundary(upper).required("upper window frame boundary")?;
        let frame_type = frame_type.try_into()?;
        let lower = WindowBoundaryKind::Lower(lower).try_into()?;
        let upper = WindowBoundaryKind::Upper(upper).try_into()?;
        Ok(spec::WindowFrame {
            frame_type,
            lower,
            upper,
        })
    }
}

impl TryFrom<FrameType> for spec::WindowFrameType {
    type Error = SparkError;

    fn try_from(frame_type: FrameType) -> SparkResult<spec::WindowFrameType> {
        match frame_type {
            FrameType::Undefined => Err(SparkError::invalid("unspecified window frame type")),
            FrameType::Row => Ok(spec::WindowFrameType::Row),
            FrameType::Range => Ok(spec::WindowFrameType::Range),
        }
    }
}

enum WindowBoundaryKind {
    Lower(Boundary),
    Upper(Boundary),
}

impl TryFrom<WindowBoundaryKind> for spec::WindowFrameBoundary {
    type Error = SparkError;

    fn try_from(kind: WindowBoundaryKind) -> SparkResult<spec::WindowFrameBoundary> {
        match kind {
            WindowBoundaryKind::Lower(Boundary::CurrentRow(true))
            | WindowBoundaryKind::Upper(Boundary::CurrentRow(true)) => {
                Ok(spec::WindowFrameBoundary::CurrentRow)
            }
            WindowBoundaryKind::Lower(Boundary::Unbounded(true)) => {
                Ok(spec::WindowFrameBoundary::UnboundedPreceding)
            }
            WindowBoundaryKind::Upper(Boundary::Unbounded(true)) => {
                Ok(spec::WindowFrameBoundary::UnboundedFollowing)
            }
            WindowBoundaryKind::Lower(Boundary::Value(expr))
            | WindowBoundaryKind::Upper(Boundary::Value(expr)) => Ok(
                spec::WindowFrameBoundary::Value(Box::new((*expr).try_into()?)),
            ),
            WindowBoundaryKind::Lower(Boundary::CurrentRow(false))
            | WindowBoundaryKind::Lower(Boundary::Unbounded(false))
            | WindowBoundaryKind::Upper(Boundary::CurrentRow(false))
            | WindowBoundaryKind::Upper(Boundary::Unbounded(false)) => {
                Err(SparkError::invalid("invalid window frame boundary"))
            }
        }
    }
}

impl TryFrom<CommonInlineUserDefinedFunction> for spec::CommonInlineUserDefinedFunction {
    type Error = SparkError;

    fn try_from(
        function: CommonInlineUserDefinedFunction,
    ) -> SparkResult<spec::CommonInlineUserDefinedFunction> {
        let CommonInlineUserDefinedFunction {
            function_name,
            deterministic,
            arguments,
            function,
        } = function;
        let function = function.required("common inline UDF function")?;
        Ok(spec::CommonInlineUserDefinedFunction {
            function_name: function_name.into(),
            deterministic,
            arguments: arguments
                .into_iter()
                .map(|x| x.try_into())
                .collect::<SparkResult<_>>()?,
            function: function.try_into()?,
        })
    }
}

impl TryFrom<udf::Function> for spec::FunctionDefinition {
    type Error = SparkError;

    fn try_from(function: udf::Function) -> SparkResult<spec::FunctionDefinition> {
        use udf::Function;

        match function {
            Function::PythonUdf(PythonUdf {
                output_type,
                eval_type,
                command,
                python_ver,
            }) => {
                let output_type = output_type.required("Python UDF output type")?;
                Ok(spec::FunctionDefinition::PythonUdf {
                    output_type: output_type.try_into()?,
                    eval_type: spec::PySparkUdfType::try_from(eval_type)?,
                    command,
                    python_version: python_ver,
                })
            }
            Function::ScalarScalaUdf(ScalarScalaUdf {
                payload,
                input_types,
                output_type,
                nullable,
            }) => {
                let output_type = output_type.required("Scalar Scala UDF output type")?;
                Ok(spec::FunctionDefinition::ScalarScalaUdf {
                    payload,
                    input_types: input_types
                        .into_iter()
                        .map(|x| x.try_into())
                        .collect::<SparkResult<_>>()?,
                    output_type: output_type.try_into()?,
                    nullable,
                })
            }
            Function::JavaUdf(JavaUdf {
                class_name,
                output_type,
                aggregate,
            }) => Ok(spec::FunctionDefinition::JavaUdf {
                class_name,
                output_type: output_type.map(|x| x.try_into()).transpose()?,
                aggregate,
            }),
        }
    }
}

impl TryFrom<CommonInlineUserDefinedTableFunction> for spec::CommonInlineUserDefinedTableFunction {
    type Error = SparkError;

    fn try_from(
        function: CommonInlineUserDefinedTableFunction,
    ) -> SparkResult<spec::CommonInlineUserDefinedTableFunction> {
        let CommonInlineUserDefinedTableFunction {
            function_name,
            deterministic,
            arguments,
            function,
        } = function;
        let function = function.required("common inline UDTF function")?;
        Ok(spec::CommonInlineUserDefinedTableFunction {
            function_name: function_name.into(),
            deterministic,
            arguments: arguments
                .into_iter()
                .map(|x| x.try_into())
                .collect::<SparkResult<_>>()?,
            function: function.try_into()?,
        })
    }
}

impl TryFrom<udtf::Function> for spec::TableFunctionDefinition {
    type Error = SparkError;

    fn try_from(function: udtf::Function) -> SparkResult<spec::TableFunctionDefinition> {
        use udtf::Function;

        match function {
            Function::PythonUdtf(PythonUdtf {
                return_type,
                eval_type,
                command,
                python_ver,
            }) => {
                let return_type = return_type.required("Python UDTF return type")?;
                Ok(spec::TableFunctionDefinition::PythonUdtf {
                    return_type: return_type.try_into()?,
                    eval_type: spec::PySparkUdfType::try_from(eval_type)?,
                    command,
                    python_version: python_ver,
                })
            }
        }
    }
}

impl TryFrom<UnresolvedNamedLambdaVariable> for spec::UnresolvedNamedLambdaVariable {
    type Error = SparkError;

    fn try_from(
        variable: UnresolvedNamedLambdaVariable,
    ) -> SparkResult<spec::UnresolvedNamedLambdaVariable> {
        let UnresolvedNamedLambdaVariable { name_parts } = variable;
        Ok(spec::UnresolvedNamedLambdaVariable {
            name: name_parts.into(),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::thread;

    use sail_common::spec;
    use sail_common::tests::test_gold_set;
    use sail_sql_analyzer::parser::parse_named_expression;
    use sail_sql_analyzer::query::from_ast_named_expression;

    use crate::error::SparkError;

    #[test]
    fn test_sql_to_expression() -> Result<(), Box<dyn std::error::Error>> {
        // Run the test in a separate thread with a large stack size
        // so that it can handle deeply nested expressions.
        let builder = thread::Builder::new().stack_size(160 * 1024 * 1024);
        let handle = builder.spawn(|| {
            test_gold_set(
                "tests/gold_data/expression/*.json",
                |sql: String| {
                    let expr = from_ast_named_expression(parse_named_expression(&sql)?)?;
                    if sql.len() > 128 {
                        Ok(spec::Expr::Literal(spec::Literal::Utf8 {
                            value: Some("Result omitted for long expression.".to_string()),
                        }))
                    } else {
                        Ok(expr)
                    }
                },
                |e: String| SparkError::internal(e),
            )
        })?;
        Ok(handle
            .join()
            .map_err(|_| SparkError::internal("failed to join thread"))??)
    }
}
