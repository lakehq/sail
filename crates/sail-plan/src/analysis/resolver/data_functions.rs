use std::sync::Arc;

use arrow_schema::{DataType, TimeUnit};
use chumsky::prelude::*;
use datafusion_common::ScalarValue;
use datafusion_expr::ScalarUDF;
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::scalar::csv::spark_from_csv::SparkFromCSV;
use sail_function::scalar::csv::spark_to_csv::SparkToCsv;
use sail_function::scalar::datetime::spark_date::SparkDate;
use sail_function::scalar::datetime::spark_time::SparkTime;
use sail_function::scalar::datetime::spark_timestamp::SparkTimestamp;
use sail_function::scalar::json::{
    SparkFromJson, json_as_text_udf, json_length_udf, json_object_keys_udf, to_json_udf,
};
use sail_function::scalar::xml::from_xml::SparkFromXml;
use sail_function::scalar::xml::to_xml::SparkToXml;

use super::Resolver;
use crate::analysis::deferred::Deferred;
use crate::analysis::ir::*;
use crate::error::{PlanError, PlanResult};

impl Resolver<'_, '_> {
    pub(super) fn data_function(
        &mut self,
        name: &str,
        values: &mut Vec<LogicalExpression>,
    ) -> PlanResult<Option<LogicalExpression>> {
        let target = match name {
            "bigint" => Some(DataType::Int64),
            "binary" => Some(DataType::Binary),
            "boolean" => Some(DataType::Boolean),
            "decimal" => Some(DataType::Decimal128(10, 0)),
            "double" => Some(DataType::Float64),
            "float" => Some(DataType::Float32),
            "int" => Some(DataType::Int32),
            "smallint" => Some(DataType::Int16),
            "string" => Some(DataType::Utf8),
            "tinyint" => Some(DataType::Int8),
            _ => None,
        };
        if let Some(target) = target {
            return Ok(Some(self.cast(std::mem::take(values).one()?, target)?));
        }
        if !matches!(
            name,
            "date"
                | "time"
                | "timestamp"
                | "from_csv"
                | "to_csv"
                | "from_json"
                | "to_json"
                | "from_xml"
                | "to_xml"
                | "get_json_object"
                | "json_array_length"
                | "json_object_keys"
        ) {
            return Ok(None);
        }
        let mut values = std::mem::take(values);
        let timezone = self.context.config.session_timezone.clone();
        let result = match name {
            "date" | "time" | "timestamp" => {
                let value = values.one()?;
                if value
                    .result_type
                    .resolved("conversion input")?
                    .data_type
                    .is_string()
                {
                    let function = match name {
                        "date" => ScalarUDF::from(SparkDate::new(false)),
                        "time" => ScalarUDF::from(SparkTime::new(false)),
                        _ => ScalarUDF::from(SparkTimestamp::try_new(
                            Some(timezone),
                            self.context.config.ansi_mode,
                            false,
                        )?),
                    };
                    self.scalar_udf(Arc::new(function), vec![value])?
                } else {
                    let target = match name {
                        "date" => DataType::Date32,
                        "time" => DataType::Time64(TimeUnit::Microsecond),
                        _ => DataType::Timestamp(TimeUnit::Microsecond, None),
                    };
                    self.cast(value, target)?
                }
            }
            "from_csv" | "from_json" | "from_xml" => {
                if let Some(schema) = values.get_mut(1)
                    && !matches!(schema.node, Deferred::Resolved(ExpressionNode::Literal(_)))
                    && let Ok(value) = crate::analysis::lowering::evaluate_constant(schema)
                {
                    let value = if name == "from_xml" {
                        match value {
                            ScalarValue::Utf8View(v) | ScalarValue::LargeUtf8(v) => {
                                ScalarValue::Utf8(v)
                            }
                            value => value,
                        }
                    } else {
                        value
                    };
                    *schema = self.literal(value);
                }
                let function = match name {
                    "from_csv" => ScalarUDF::from(SparkFromCSV::new(timezone)),
                    "from_json" => ScalarUDF::from(SparkFromJson::new(timezone)),
                    _ => ScalarUDF::from(SparkFromXml::new(timezone)),
                };
                self.scalar_udf(Arc::new(function), values)?
            }
            "to_csv" => {
                self.scalar_udf(Arc::new(ScalarUDF::from(SparkToCsv::new(timezone))), values)?
            }
            "to_xml" => {
                self.scalar_udf(Arc::new(ScalarUDF::from(SparkToXml::new(timezone))), values)?
            }
            "to_json" => {
                if !matches!(values.len(), 1 | 2) {
                    return Err(PlanError::invalid(format!(
                        "to_json expects 1 or 2 arguments, got {}",
                        values.len()
                    )));
                }
                self.scalar_udf(to_json_udf(), values)?
            }
            "json_object_keys" => self.scalar_udf(json_object_keys_udf(), vec![values.one()?])?,
            "json_array_length" => {
                let value = self.scalar_udf(json_length_udf(), vec![values.one()?])?;
                self.cast(value, DataType::Int32)?
            }
            "get_json_object" => {
                let (value, path) = values.two()?;
                let paths = if let Deferred::Resolved(ExpressionNode::Literal(ScalarValue::Utf8(
                    Some(path),
                ))) = &path.node
                {
                    let Some(paths) = json_path_parser().parse(path).into_result().ok() else {
                        return Ok(Some(self.literal(ScalarValue::Utf8(None))));
                    };
                    paths
                        .into_iter()
                        .map(|v| self.literal(v))
                        .collect::<Vec<_>>()
                } else {
                    let copy = self.copy_expression(&path)?;
                    let pattern = self.literal(ScalarValue::Utf8(Some("$.%".into())));
                    let matches = self.pattern(copy, pattern, false, false, None)?;
                    let three = self.literal(ScalarValue::Int64(Some(3)));
                    let key = self.scalar("substr", vec![path, three])?;
                    let yes = self.literal(ScalarValue::Boolean(Some(true)));
                    let empty = self.literal(ScalarValue::Utf8(Some("".into())));
                    vec![self.case(vec![(matches, key), (yes, empty)], None)?]
                };
                let mut arguments = vec![value];
                arguments.extend(paths);
                self.scalar_udf(json_as_text_udf(), arguments)?
            }
            _ => return Err(PlanError::internal("unknown data conversion function")),
        };
        Ok(Some(result))
    }
}

fn json_path_parser<'src>()
-> impl Parser<'src, &'src str, Vec<ScalarValue>, extra::Err<Rich<'src, char>>> {
    // `.key` — a run of characters other than `.` or `[`.
    let dot_key = just('.')
        .ignore_then(
            none_of(['.', '['])
                .repeated()
                .at_least(1)
                .collect::<String>(),
        )
        .map(|value| ScalarValue::Utf8(Some(value)));

    // `['key']` — single-quoted (may contain dots). Double-quoted brackets are
    // not accepted, matching Spark (which returns NULL for them).
    let bracket_key = just('[')
        .ignore_then(just('\''))
        .ignore_then(none_of('\'').repeated().collect::<String>())
        .then_ignore(just('\''))
        .then_ignore(just(']'))
        .map(|value| ScalarValue::Utf8(Some(value)));

    // `[0]` — array index.
    let index = just('[')
        .ignore_then(text::int(10).try_map(|digits: &str, span| {
            digits
                .parse::<i64>()
                .map(|value| ScalarValue::Int64(Some(value)))
                .map_err(|e| Rich::custom(span, e.to_string()))
        }))
        .then_ignore(just(']'));

    just('$')
        .ignore_then(
            choice((dot_key, bracket_key, index))
                .repeated()
                .collect::<Vec<_>>(),
        )
        .then_ignore(end())
}
