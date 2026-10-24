use std::sync::Arc;

use arrow_schema::DataType;
use datafusion_common::ScalarValue;
use datafusion_expr::{Operator, ScalarUDF};
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::scalar::array::spark_array::SparkArray;
use sail_function::scalar::map::map_entries::SparkMapEntries;
use sail_function::scalar::map::map_from::{SparkMapFromArrays, SparkMapFromEntries};
use sail_function::scalar::map::str_to_map::StrToMap;
use sail_function::scalar::misc::raise_error::RaiseError;

use super::Resolver;
use crate::analysis::ir::*;
use crate::config::MapKeyDedupPolicy;
use crate::error::{PlanError, PlanResult};

impl Resolver<'_, '_> {
    pub(super) fn collection_function(
        &mut self,
        name: &str,
        values: &mut Vec<LogicalExpression>,
    ) -> PlanResult<Option<LogicalExpression>> {
        if !matches!(
            name,
            "size"
                | "cardinality"
                | "deep_size"
                | "element_at"
                | "try_element_at"
                | "map"
                | "map_concat"
                | "map_contains_key"
                | "map_entries"
                | "map_from_arrays"
                | "map_from_entries"
                | "map_values"
                | "str_to_map"
        ) {
            return Ok(None);
        }
        let values = std::mem::take(values);
        let last_wins = self.context.config.map_key_dedup_policy == MapKeyDedupPolicy::LastWin;
        let result = match name {
            "deep_size" => self.scalar("cardinality", vec![values.one()?])?,
            "size" | "cardinality" => {
                let value = values.one()?;
                let data_type = &value.result_type.resolved("size input")?.data_type;
                let function = if data_type.is_list() {
                    "array_length"
                } else if matches!(data_type, DataType::Map(..)) {
                    "cardinality"
                } else {
                    return Err(PlanError::invalid(format!(
                        "size expects List or Map as argument, got {data_type:?}"
                    )));
                };
                let result = self.scalar(function, vec![value])?;
                let result = self.cast(result, DataType::Int32)?;
                if self.context.config.legacy_size_of_null && !self.context.config.ansi_mode {
                    let fallback = self.literal(ScalarValue::Int32(Some(-1)));
                    self.scalar("coalesce", vec![result, fallback])?
                } else {
                    result
                }
            }
            "element_at" | "try_element_at" => {
                let (collection, element) = values.two()?;
                let data_type = &collection
                    .result_type
                    .resolved("element_at input")?
                    .data_type;
                if data_type.is_list() {
                    let index = self.copy_expression(&element)?;
                    let zero = self.literal(ScalarValue::Int32(Some(0)));
                    let invalid = self.binary(index, Operator::Eq, zero)?;
                    let error = self.raise_error(format!("{name}: the index 0 is invalid. An index shall be either < 0 or > 0 (the first element has index 1)"))?;
                    let index = self.copy_expression(&element)?;
                    let absolute = self.scalar("abs", vec![index])?;
                    let absolute_copy = self.copy_expression(&absolute)?;
                    let one = self.literal(ScalarValue::Int64(Some(1)));
                    let below = self.binary(absolute_copy, Operator::Lt, one)?;
                    let array = self.copy_expression(&collection)?;
                    let length = self.scalar("array_length", vec![array])?;
                    let above = self.binary(absolute, Operator::Gt, length)?;
                    let outside = self.binary(below, Operator::Or, above)?;
                    let fallback = if name == "try_element_at" {
                        self.literal(ScalarValue::Null)
                    } else {
                        self.raise_error("element_at: the index is out of bounds".into())?
                    };
                    let value = self.scalar("array_element", vec![collection, element])?;
                    let always = self.literal(ScalarValue::Boolean(Some(true)));
                    self.case(
                        vec![(invalid, error), (outside, fallback), (always, value)],
                        None,
                    )?
                } else if matches!(data_type, DataType::Map(..)) {
                    let values = self.scalar("map_extract", vec![collection, element])?;
                    let first = self.literal(ScalarValue::Int64(Some(1)));
                    self.scalar("array_element", vec![values, first])?
                } else {
                    self.raise_error(format!(
                        "{name} expects List or Map type as first argument, got {data_type:?}"
                    ))?
                }
            }
            "map" => {
                if !values.len().is_multiple_of(2) {
                    return Err(PlanError::invalid(format!(
                        "map(k1, v1, k2, v2, ...): expect number of args to be multiple of 2, got {}",
                        values.len()
                    )));
                }
                let mut keys = vec![];
                let mut entries = vec![];
                for (i, value) in values.into_iter().enumerate() {
                    if i % 2 == 0 {
                        keys.push(value);
                    } else {
                        entries.push(value);
                    }
                }
                let keys = self.ansi_map_arguments(keys)?;
                let entries = self.ansi_map_arguments(entries)?;
                let nullable = entries.iter().try_fold(false, |nullable, v| {
                    Ok::<_, PlanError>(nullable || v.result_type.resolved("map value")?.nullable)
                })?;
                let function = Arc::new(ScalarUDF::from(SparkArray::new()));
                let keys = self.scalar_udf(function.clone(), keys)?;
                let entries = self.scalar_udf(function, entries)?;
                let entries = self.list_nullability(entries, true)?;
                let result = self.scalar_udf(
                    Arc::new(ScalarUDF::from(SparkMapFromArrays::new(last_wins))),
                    vec![keys, entries],
                )?;
                self.map_nullability(result, nullable)?
            }
            "map_from_arrays" => {
                let (keys, values) = values.two()?;
                let nullable = match &values.result_type.resolved("map values")?.data_type {
                    DataType::List(field) | DataType::LargeList(field) => field.is_nullable(),
                    _ => true,
                };
                let values = self.list_nullability(values, true)?;
                let result = self.scalar_udf(
                    Arc::new(ScalarUDF::from(SparkMapFromArrays::new(last_wins))),
                    vec![keys, values],
                )?;
                self.map_nullability(result, nullable)?
            }
            "map_from_entries" => {
                let entries = values.one()?;
                let data_type = &entries.result_type.resolved("map entries")?.data_type;
                let nullable =
                    crate::analysis::typing::collection::map_entries_value_contains_null(data_type);
                let target =
                    crate::analysis::typing::collection::map_entries_nullability(data_type, true);
                let entries = self.cast(entries, target)?;
                let result = self.scalar_udf(
                    Arc::new(ScalarUDF::from(SparkMapFromEntries::new(last_wins))),
                    vec![entries],
                )?;
                self.map_nullability(result, nullable)?
            }
            "map_entries" => self.scalar_udf(
                Arc::new(ScalarUDF::from(SparkMapEntries::new())),
                vec![values.one()?],
            )?,
            "map_values" => {
                let value = values.one()?;
                let nullable = crate::analysis::typing::collection::map_value_contains_null(
                    &value.result_type.resolved("map value")?.data_type,
                );
                let value = self.map_nullability(value, true)?;
                let value = self.scalar("map_values", vec![value])?;
                self.list_nullability(value, nullable)?
            }
            "map_contains_key" => {
                let (map, key) = values.two()?;
                let keys = self.scalar("map_keys", vec![map])?;
                self.scalar("array_has", vec![keys, key])?
            }
            "str_to_map" => {
                let (value, separators) = values.at_least_one()?;
                let mut separators = separators.into_iter();
                let pair = separators
                    .next()
                    .unwrap_or_else(|| self.literal(ScalarValue::Utf8(Some(",".into()))));
                let entry = separators
                    .next()
                    .unwrap_or_else(|| self.literal(ScalarValue::Utf8(Some(":".into()))));
                self.scalar_udf(
                    Arc::new(ScalarUDF::from(StrToMap::new(last_wins))),
                    vec![value, pair, entry],
                )?
            }
            "map_concat" => {
                if values.is_empty() {
                    return Ok(Some(self.literal(ScalarValue::Null)));
                }
                let mut keys = vec![];
                let mut entries = vec![];
                let mut null = None;
                for map in values {
                    let copy = self.copy_expression(&map)?;
                    keys.push(self.scalar("map_keys", vec![copy])?);
                    let copy = self.copy_expression(&map)?;
                    entries.push(self.scalar("map_values", vec![copy])?);
                    let check = self.is_null(map);
                    null = Some(match null {
                        Some(previous) => self.binary(previous, Operator::Or, check)?,
                        None => check,
                    });
                }
                let keys = self.scalar("array_concat", keys)?;
                let entries = self.scalar("array_concat", entries)?;
                let value = self.scalar_udf(
                    Arc::new(ScalarUDF::from(SparkMapFromArrays::new(last_wins))),
                    vec![keys, entries],
                )?;
                let condition =
                    null.ok_or_else(|| PlanError::internal("map_concat arguments missing"))?;
                let null = self.literal(ScalarValue::Null);
                self.case(vec![(condition, null)], Some(value))?
            }
            _ => return Err(PlanError::internal("unknown collection function")),
        };
        Ok(Some(result))
    }

    pub(super) fn raise_error(&mut self, message: String) -> PlanResult<LogicalExpression> {
        let message = self.literal(ScalarValue::Utf8(Some(message)));
        self.scalar_udf(Arc::new(ScalarUDF::from(RaiseError::new())), vec![message])
    }

    pub(super) fn list_nullability(
        &mut self,
        value: LogicalExpression,
        nullable: bool,
    ) -> PlanResult<LogicalExpression> {
        let target = crate::analysis::typing::collection::list_nullability(
            &value.result_type.resolved("list input")?.data_type,
            nullable,
        );
        self.cast(value, target)
    }

    fn map_nullability(
        &mut self,
        value: LogicalExpression,
        nullable: bool,
    ) -> PlanResult<LogicalExpression> {
        let target = crate::analysis::typing::collection::map_nullability(
            &value.result_type.resolved("map input")?.data_type,
            nullable,
        );
        self.cast(value, target)
    }

    fn ansi_map_arguments(
        &mut self,
        arguments: Vec<LogicalExpression>,
    ) -> PlanResult<Vec<LogicalExpression>> {
        let types = arguments
            .iter()
            .map(|a| Ok(a.result_type.resolved("map argument")?.data_type.clone()))
            .collect::<PlanResult<Vec<_>>>()?;
        if !self.context.config.ansi_mode || !types.iter().any(DataType::is_string) {
            return Ok(arguments);
        }
        let target = types
            .iter()
            .try_fold(DataType::Null, |previous, data_type| {
                crate::analysis::typing::collection::ansi_map_common_type(&previous, data_type)
            })
            .ok_or_else(|| PlanError::analysis("map arguments have incompatible types"))?;
        arguments
            .into_iter()
            .zip(types)
            .map(|(argument, source)| {
                if source == target {
                    return Ok(argument);
                }
                let value = self.cast(argument, target.clone())?;
                if source.is_string() && !target.is_binary() {
                    let condition = self.literal(ScalarValue::Boolean(Some(true)));
                    self.case(vec![(condition, value)], None)
                } else {
                    Ok(value)
                }
            })
            .collect()
    }
}
