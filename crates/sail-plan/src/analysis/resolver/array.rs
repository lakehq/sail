use std::sync::Arc;

use arrow_schema::DataType;
use datafusion_common::ScalarValue;
use datafusion_expr::{Operator, ScalarUDF};
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::scalar::array::arrays_zip::ArraysZip;
use sail_function::scalar::array::spark_array_compact::SparkArrayCompact;

use super::Resolver;
use crate::analysis::deferred::Deferred;
use crate::analysis::model::*;
use crate::error::{PlanError, PlanResult};

impl Resolver<'_, '_> {
    pub(super) fn array_function(
        &mut self,
        name: &str,
        values: &mut Vec<ExprNode>,
        names: &[String],
    ) -> PlanResult<Option<ExprNode>> {
        if name == "array_sort" && values.len() == 1 {
            let array = std::mem::take(values).one()?;
            let direction = self.literal(ScalarValue::Utf8(Some("ASC".into())));
            let nulls = self.literal(ScalarValue::Utf8(Some("NULLS LAST".into())));
            return self
                .scalar("array_sort", vec![array, direction, nulls])
                .map(Some);
        }
        if !matches!(
            name,
            "sequence"
                | "array_insert"
                | "array_repeat"
                | "array_compact"
                | "arrays_zip"
                | "slice"
                | "sort_array"
                | "array_append"
                | "array_prepend"
                | "get"
                | "array_contains"
                | "array_contains_all"
                | "array_size"
                | "array_remove"
                | "arrays_overlap"
                | "flatten"
        ) {
            return Ok(None);
        }
        let values = std::mem::take(values);
        let result = match name {
            "sequence" => self.sequence(values)?,
            "array_insert" => {
                let (array, position, value) = values.three()?;
                self.insert_array(array, position, value)?
            }
            "array_compact" => self.compact_array(values.one()?)?,
            "array_remove" => {
                let (a, b) = values.two()?;
                self.scalar("array_remove_all", vec![a, b])?
            }
            "arrays_zip" => {
                let fields = values
                    .iter()
                    .zip(names)
                    .enumerate()
                    .map(|(i, (v, n))| match v.kind {
                        Deferred::Resolved(
                            ExprKind::Attribute(_)
                            | ExprKind::OuterAttribute { .. }
                            | ExprKind::WithMetadata(_),
                        ) => n.clone(),
                        _ => i.to_string(),
                    })
                    .collect();
                self.scalar_udf(Arc::new(ScalarUDF::from(ArraysZip::new(fields))), values)?
            }
            "array_repeat" => {
                let (element, count) = values.two()?;
                let count = self.cast(count, DataType::Int64)?;
                let element_copy = self.copy_expression(&element)?;
                let count_copy = self.copy_expression(&count)?;
                let prototype = self.scalar("array_repeat", vec![element_copy, count_copy])?;
                let target = crate::analysis::typing::collection::nullable_array_type(
                    &prototype
                        .result_type
                        .resolved("array_repeat result")?
                        .data_type,
                );
                self.repeat_array(element, count, target)?
            }
            "slice" => {
                let (array, start, length) = values.three()?;
                let output_type = array.result_type.resolved("slice input")?.data_type.clone();
                let array = self.list_nullability(array, true)?;
                let start = self.cast(start, DataType::Int64)?;
                let length = self.cast(length, DataType::Int64)?;
                let start_copy = self.copy_expression(&start)?;
                let end = self.binary(start_copy, Operator::Plus, length)?;
                let one = self.literal(ScalarValue::Int64(Some(1)));
                let end = self.binary(end, Operator::Minus, one)?;
                let result = self.scalar("array_slice", vec![array, start, end])?;
                self.cast(result, output_type)?
            }
            "sort_array" => {
                let (array, ascending) = match values.len() {
                    1 => (values.one()?, true),
                    2 => {
                        let (array, order) = values.two()?;
                        let ScalarValue::Boolean(Some(order)) =
                            crate::analysis::lowering::evaluate_constant(&order)?
                        else {
                            return Err(PlanError::invalid("Invalid asc value for sort_array"));
                        };
                        (array, order)
                    }
                    n => {
                        return Err(PlanError::invalid(format!(
                            "sort_array requires 1 or 2 arguments, got {n}"
                        )));
                    }
                };
                let direction = self.literal(ScalarValue::Utf8(Some(
                    if ascending { "ASC" } else { "DESC" }.into(),
                )));
                let nulls = self.literal(ScalarValue::Utf8(Some(
                    if ascending {
                        "NULLS FIRST"
                    } else {
                        "NULLS LAST"
                    }
                    .into(),
                )));
                self.scalar("array_sort", vec![array, direction, nulls])?
            }
            "array_append" | "array_prepend" => {
                let (array, element) = values.two()?;
                let copy = self.copy_expression(&array)?;
                let condition = self.is_not_null(copy);
                let array = self.list_nullability(array, true)?;
                let value = if name == "array_append" {
                    self.scalar(name, vec![array, element])?
                } else {
                    self.scalar(name, vec![element, array])?
                };
                let value = self.list_nullability(value, true)?;
                self.case(vec![(condition, value)], None)?
            }
            "get" => {
                let (array, element) = values.two()?;
                let one = self.literal(ScalarValue::Int64(Some(1)));
                let element = self.binary(element, Operator::Plus, one)?;
                self.scalar("array_element", vec![array, element])?
            }
            "array_contains" => {
                let (array, value) = values.two()?;
                let copy = self.copy_expression(&array)?;
                let result = self.scalar("array_has", vec![copy, value])?;
                let present = self.is_not_null(array);
                let no = self.literal(ScalarValue::Boolean(Some(false)));
                let fallback = self.case(vec![(present, no)], None)?;
                self.scalar("coalesce", vec![result, fallback])?
            }
            "array_contains_all" => {
                let (array, value) = values.two()?;
                let result = self.scalar("array_has_all", vec![array, value])?;
                let no = self.literal(ScalarValue::Boolean(Some(false)));
                self.scalar("coalesce", vec![result, no])?
            }
            "array_size" => {
                let result = self.scalar("array_length", vec![values.one()?])?;
                self.cast(result, DataType::Int32)?
            }
            "flatten" => {
                let array = values.one()?;
                let target = crate::analysis::typing::collection::nullable_array_type(
                    &array.result_type.resolved("flatten input")?.data_type,
                );
                let array = self.cast(array, target.clone())?;
                let null = self.literal(ScalarValue::Null);
                let singleton = self.scalar("make_array", vec![null])?;
                let singleton = self.cast(singleton, target)?;
                let copy = self.copy_expression(&array)?;
                let has_null = self.scalar("array_has_any", vec![copy, singleton])?;
                let no_null = self.not(has_null)?;
                let flattened = self.scalar("flatten", vec![array])?;
                self.case(vec![(no_null, flattened)], None)?
            }
            "arrays_overlap" => {
                let (left, right) = values.two()?;
                let target = crate::analysis::typing::collection::list_nullability(
                    &left.result_type.resolved("array overlap left")?.data_type,
                    true,
                );
                let left = self.cast(left, target.clone())?;
                let right = self.cast(right, target.clone())?;
                let null = self.literal(ScalarValue::Null);
                let singleton = self.scalar("make_array", vec![null])?;
                let singleton = self.cast(singleton, target)?;
                let singleton_copy = self.copy_expression(&singleton)?;
                let copy = self.copy_expression(&left)?;
                let left_nulls = self.scalar("array_has_any", vec![copy, singleton_copy])?;
                let copy = self.copy_expression(&right)?;
                let right_nulls = self.scalar("array_has_any", vec![copy, singleton])?;
                let has_nulls = self.binary(left_nulls, Operator::Or, right_nulls)?;
                let copy = self.copy_expression(&left)?;
                let left_null = self.is_null(copy);
                let copy = self.copy_expression(&right)?;
                let right_null = self.is_null(copy);
                let either_null = self.binary(left_null, Operator::Or, right_null)?;
                let copy = self.copy_expression(&left)?;
                let compact_left = self.compact_array(copy)?;
                let copy = self.copy_expression(&right)?;
                let compact_right = self.compact_array(copy)?;
                let overlap = self.scalar("array_has_any", vec![compact_left, compact_right])?;
                let null = self.literal(ScalarValue::Null);
                let null_overlap = self.binary(overlap, Operator::Or, null)?;
                let overlap = self.scalar("array_has_any", vec![left, right])?;
                let null = self.literal(ScalarValue::Null);
                let yes = self.literal(ScalarValue::Boolean(Some(true)));
                self.case(
                    vec![
                        (either_null, null),
                        (has_nulls, null_overlap),
                        (yes, overlap),
                    ],
                    None,
                )?
            }
            _ => return Err(PlanError::internal("unknown array function")),
        };
        Ok(Some(result))
    }

    fn compact_array(&mut self, value: ExprNode) -> PlanResult<ExprNode> {
        self.scalar_udf(
            Arc::new(ScalarUDF::from(SparkArrayCompact::new())),
            vec![value],
        )
    }

    pub(super) fn repeat_array(
        &mut self,
        element: ExprNode,
        count: ExprNode,
        target: DataType,
    ) -> PlanResult<ExprNode> {
        let element_type = crate::analysis::typing::collection::nullable_array_type(
            &element
                .result_type
                .resolved("array repeat element")?
                .data_type,
        );
        let element = self.cast(element, element_type)?;
        let yes = self.literal(ScalarValue::Boolean(Some(true)));
        let element = self.case(vec![(yes, element)], None)?;
        let result = self.scalar("array_repeat", vec![element, count])?;
        self.cast(result, target)
    }
}

impl Resolver<'_, '_> {
    fn insert_array(
        &mut self,
        array: ExprNode,
        position: ExprNode,
        value: ExprNode,
    ) -> PlanResult<ExprNode> {
        let copy = self.copy_expression(&array)?;
        let array_input = self.list_nullability(copy, true)?;
        let copy = self.copy_expression(&array_input)?;
        let value_copy = self.copy_expression(&value)?;
        let prototype = self.scalar("array_append", vec![copy, value_copy])?;
        let target = crate::analysis::typing::collection::list_nullability(
            &prototype
                .result_type
                .resolved("array insert result")?
                .data_type,
            true,
        );
        let array_input = self.cast(array_input, target.clone())?;
        let copy = self.copy_expression(&array)?;
        let length = self.scalar("array_length", vec![copy])?;
        let length = self.cast(length, DataType::Int64)?;
        let copy = self.copy_expression(&position)?;
        let zero = self.literal(ScalarValue::Int32(Some(0)));
        let positive = self.binary(copy, Operator::Gt, zero)?;
        let copy = self.copy_expression(&position)?;
        let one = self.literal(ScalarValue::Int32(Some(1)));
        let positive_index = self.binary(copy, Operator::Minus, one)?;
        let copy = self.copy_expression(&position)?;
        let zero = self.literal(ScalarValue::Int32(Some(0)));
        let negative = self.binary(copy, Operator::Lt, zero)?;
        let copy = self.copy_expression(&length)?;
        let negative_index = self.binary(copy, Operator::Plus, position)?;
        let one = self.literal(ScalarValue::Int32(Some(1)));
        let negative_index = self.binary(negative_index, Operator::Plus, one)?;
        let index = self.case(
            vec![(positive, positive_index), (negative, negative_index)],
            None,
        )?;
        let copy = self.copy_expression(&array)?;
        let is_null = self.is_null(copy);
        let null_array = self.cast(array, target.clone())?;
        let copy = self.copy_expression(&index)?;
        let invalid = self.is_null(copy);
        let error = self.raise_error("array_insert: the index 0 is invalid. An index shall be either < 0 or > 0 (the first element has index 1)".into())?;
        let error = self.cast(error, target.clone())?;
        let mut branches = vec![(is_null, null_array), (invalid, error)];

        let copy = self.copy_expression(&index)?;
        let zero = self.literal(ScalarValue::Int32(Some(0)));
        let before = self.binary(copy, Operator::Lt, zero)?;
        let copy = self.copy_expression(&value)?;
        let one = self.literal(ScalarValue::Int64(Some(1)));
        let first = self.repeat_array(copy, one, target.clone())?;
        let copy = self.copy_expression(&index)?;
        let zero = self.literal(ScalarValue::Int64(Some(0)));
        let padding_length = self.binary(zero, Operator::Minus, copy)?;
        let null = self.literal(ScalarValue::Null);
        let padding = self.repeat_array(null, padding_length, target.clone())?;
        let copy = self.copy_expression(&array_input)?;
        let result = self.scalar("array_concat", vec![first, padding, copy])?;
        branches.push((before, result));

        let copy = self.copy_expression(&index)?;
        let zero = self.literal(ScalarValue::Int32(Some(0)));
        let at_start = self.binary(copy, Operator::Eq, zero)?;
        let copy = self.copy_expression(&array_input)?;
        let value_copy = self.copy_expression(&value)?;
        let result = self.scalar("array_prepend", vec![value_copy, copy])?;
        branches.push((at_start, result));

        let copy = self.copy_expression(&index)?;
        let one = self.literal(ScalarValue::Int64(Some(1)));
        let lower = self.binary(copy, Operator::GtEq, one)?;
        let copy = self.copy_expression(&length)?;
        let one = self.literal(ScalarValue::Int64(Some(1)));
        let upper_limit = self.binary(copy, Operator::Minus, one)?;
        let copy = self.copy_expression(&index)?;
        let upper = self.binary(copy, Operator::LtEq, upper_limit)?;
        let middle = self.binary(lower, Operator::And, upper)?;
        let copy = self.copy_expression(&array_input)?;
        let end = self.copy_expression(&index)?;
        let one = self.literal(ScalarValue::Int64(Some(1)));
        let prefix = self.scalar("array_slice", vec![copy, one, end])?;
        let copy = self.copy_expression(&value)?;
        let one = self.literal(ScalarValue::Int64(Some(1)));
        let inserted = self.repeat_array(copy, one, target.clone())?;
        let copy = self.copy_expression(&index)?;
        let one = self.literal(ScalarValue::Int64(Some(1)));
        let start = self.binary(copy, Operator::Plus, one)?;
        let copy = self.copy_expression(&array_input)?;
        let end = self.copy_expression(&length)?;
        let suffix = self.scalar("array_slice", vec![copy, start, end])?;
        let result = self.scalar("array_concat", vec![prefix, inserted, suffix])?;
        branches.push((middle, result));

        let copy = self.copy_expression(&index)?;
        let end = self.copy_expression(&length)?;
        let at_end = self.binary(copy, Operator::Eq, end)?;
        let copy = self.copy_expression(&array_input)?;
        let value_copy = self.copy_expression(&value)?;
        let result = self.scalar("array_append", vec![copy, value_copy])?;
        branches.push((at_end, result));

        let copy = self.copy_expression(&index)?;
        let end = self.copy_expression(&length)?;
        let beyond = self.binary(copy, Operator::Gt, end)?;
        let padding_length = self.binary(index, Operator::Minus, length)?;
        let null = self.literal(ScalarValue::Null);
        let padding = self.repeat_array(null, padding_length, target.clone())?;
        let one = self.literal(ScalarValue::Int64(Some(1)));
        let last = self.repeat_array(value, one, target)?;
        let result = self.scalar("array_concat", vec![array_input, padding, last])?;
        branches.push((beyond, result));
        self.case(branches, None)
    }
}
