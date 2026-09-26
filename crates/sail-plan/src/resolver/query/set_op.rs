use std::collections::HashMap;
use std::sync::{Arc, LazyLock};

use datafusion::arrow::datatypes::{DataType, FieldRef, TimeUnit};
use datafusion::functions_window::row_number::row_number_udwf;
use datafusion::logical_expr::expr::NullTreatment;
use datafusion::optimizer::analyzer::type_coercion::coerce_union_schema;
use datafusion_common::{Column, DFSchema, JoinType, NullEquality, ScalarValue};
use datafusion_expr::builder::project;
use datafusion_expr::expr::WindowFunctionParams;
use datafusion_expr::type_coercion::binary::type_union_coercion;
use datafusion_expr::{
    Expr, ExprSchemable, LogicalPlan, LogicalPlanBuilder, ScalarUDF, Union, WindowFrame,
    WindowFunctionDefinition, expr,
};
use regex::Regex;
use sail_common::spec;
use sail_function::scalar::conditional::SparkConditionalCast;

use crate::config::PlanConfig;
use crate::error::{PlanError, PlanResult};
use crate::function::{ansi_string_numeric_type, wider_numeric_type};
use crate::resolver::PlanResolver;
use crate::resolver::state::PlanResolverState;

impl PlanResolver<'_> {
    pub(super) async fn resolve_query_set_operation(
        &self,
        op: spec::SetOperation,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        use spec::SetOpType;

        let spec::SetOperation {
            left,
            right,
            set_op_type,
            is_all,
            by_name,
            allow_missing_columns,
        } = op;
        let left = self.resolve_query_plan(*left, state).await?;
        let right = self.resolve_query_plan(*right, state).await?;
        match set_op_type {
            SetOpType::Intersect => Ok(LogicalPlanBuilder::intersect(left, right, is_all)?),
            SetOpType::Union => {
                let (left, right) = if by_name {
                    let left_names = Self::get_field_names(left.schema(), state)?;
                    let right_names = Self::get_field_names(right.schema(), state)?;
                    let (mut left_reordered_columns, mut right_reordered_columns): (
                        Vec<Expr>,
                        Vec<Expr>,
                    ) = left_names
                        .iter()
                        .enumerate()
                        .map(|(left_idx, left_name)| {
                            match right_names
                                .iter()
                                .position(|right_name| left_name.eq_ignore_ascii_case(right_name))
                            {
                                Some(right_idx) => Ok((
                                    Expr::Column(Column::from(
                                        left.schema().qualified_field(left_idx),
                                    )),
                                    Expr::Column(Column::from(
                                        right.schema().qualified_field(right_idx),
                                    )),
                                )),
                                None if allow_missing_columns => Ok((
                                    Expr::Column(Column::from(
                                        left.schema().qualified_field(left_idx),
                                    )),
                                    Expr::Literal(ScalarValue::Null, None)
                                        .alias(state.register_field_name(left_name)),
                                )),
                                None => Err(PlanError::invalid(format!(
                                    "right column not found: {left_name}"
                                ))),
                            }
                        })
                        .collect::<PlanResult<Vec<(Expr, Expr)>>>()?
                        .into_iter()
                        .unzip();
                    if allow_missing_columns {
                        let (left_extra_columns, right_extra_columns): (Vec<Expr>, Vec<Expr>) =
                            right_names
                                .into_iter()
                                .enumerate()
                                .filter(|(_, right_name)| {
                                    !left_names
                                        .iter()
                                        .any(|left_name| left_name.eq_ignore_ascii_case(right_name))
                                })
                                .map(|(right_idx, right_name)| {
                                    (
                                        Expr::Literal(ScalarValue::Null, None)
                                            .alias(state.register_field_name(right_name)),
                                        Expr::Column(Column::from(
                                            right.schema().qualified_field(right_idx),
                                        )),
                                    )
                                })
                                .collect::<Vec<(Expr, Expr)>>()
                                .into_iter()
                                .unzip();
                        right_reordered_columns.extend(right_extra_columns);
                        left_reordered_columns.extend(left_extra_columns);
                        (
                            project(left, left_reordered_columns)?,
                            project(right, right_reordered_columns)?,
                        )
                    } else {
                        (left, project(right, right_reordered_columns)?)
                    }
                } else {
                    (left, right)
                };
                let ansi_mode = self
                    .config
                    .view_conditional_ansi_mode
                    .unwrap_or(self.config.ansi_mode);
                // Cast Spark's numeric common types, including ANSI STRING/numeric pairs,
                // before exposing the UNION type:
                // conditional consumers must not cache a narrower type than its actual values.
                let left_schema = Arc::clone(left.schema());
                let left = promote_union_numeric_input(
                    left,
                    right.schema(),
                    ansi_mode,
                    &self.config,
                    by_name,
                )?;
                let right = promote_union_numeric_input(
                    right,
                    &left_schema,
                    ansi_mode,
                    &self.config,
                    by_name,
                )?;
                let mut union =
                    Union::try_new_with_loose_types(vec![Arc::new(left), Arc::new(right)])?;
                // Conditional coercion needs the common UNION schema.
                let coerced = coerce_union_schema(&union.inputs)?;
                // Take only types and nullability from the coerced schema, since DataFusion lets
                // the last input's field metadata (such as a Spark interval qualifier) win.
                // Columns keep the loose type where DataFusion's common type differs from Spark:
                // DATE or STRING with TIMESTAMP becomes a nanosecond TIMESTAMP.
                // TODO: Widen these columns and interval qualifiers like Spark.
                // TODO: Coerce TIMESTAMP/STRING UNION columns to microseconds; raw UNION
                //  output currently retains unsupported nanosecond units, and STRING-first
                //  inputs also break UTC conversion consumers.
                let fields = union
                    .schema
                    .iter()
                    .zip(coerced.fields())
                    .map(|((qualifier, field), coerced_field)| {
                        let field = Arc::new(
                            field
                                .as_ref()
                                .clone()
                                .with_data_type(repair_union_type(
                                    field.data_type(),
                                    coerced_field.data_type(),
                                    ansi_mode,
                                ))
                                .with_nullable(coerced_field.is_nullable()),
                        );
                        (qualifier.cloned(), field)
                    })
                    .collect();
                union.schema = Arc::new(DFSchema::new_with_metadata(
                    fields,
                    union.schema.metadata().clone(),
                )?);
                let plan = LogicalPlan::Union(union);
                if is_all {
                    Ok(plan)
                } else {
                    Ok(LogicalPlanBuilder::new(plan).distinct()?.build()?)
                }
            }
            SetOpType::Except => {
                let left_len = left.schema().fields().len();
                let right_len = right.schema().fields().len();

                if left_len != right_len {
                    return Err(PlanError::invalid(format!(
                        "`EXCEPT ALL` must have the same number of columns. Left has {left_len} columns, right has {right_len} columns."
                    )));
                }

                let mut join_keys = left
                    .schema()
                    .fields()
                    .iter()
                    .zip(right.schema().fields().iter())
                    .map(|(left_field, right_field)| {
                        (
                            Column::from_name(left_field.name()),
                            Column::from_name(right_field.name()),
                        )
                    })
                    .collect::<Vec<_>>();

                let plan = if is_all {
                    let left_row_number_alias = state.register_field_name("row_num");
                    let right_row_number_alias = state.register_field_name("row_num");
                    let left_row_number_window =
                        Expr::WindowFunction(Box::new(expr::WindowFunction {
                            fun: WindowFunctionDefinition::WindowUDF(row_number_udwf()),
                            params: WindowFunctionParams {
                                args: vec![],
                                partition_by: left
                                    .schema()
                                    .fields()
                                    .iter()
                                    .map(|field| Expr::Column(Column::from_name(field.name())))
                                    .collect::<Vec<_>>(),
                                order_by: vec![],
                                window_frame: WindowFrame::new(None),
                                filter: None,
                                null_treatment: Some(NullTreatment::RespectNulls),
                                distinct: false,
                            },
                        }))
                        .alias(left_row_number_alias.as_str());
                    let right_row_number_window =
                        Expr::WindowFunction(Box::new(expr::WindowFunction {
                            fun: WindowFunctionDefinition::WindowUDF(row_number_udwf()),
                            params: WindowFunctionParams {
                                args: vec![],
                                partition_by: right
                                    .schema()
                                    .fields()
                                    .iter()
                                    .map(|field| Expr::Column(Column::from_name(field.name())))
                                    .collect::<Vec<_>>(),
                                order_by: vec![],
                                window_frame: WindowFrame::new(None),
                                filter: None,
                                null_treatment: Some(NullTreatment::RespectNulls),
                                distinct: false,
                            },
                        }))
                        .alias(right_row_number_alias.as_str());
                    let left = LogicalPlanBuilder::from(left)
                        .window(vec![left_row_number_window])?
                        .build()?;
                    let right = LogicalPlanBuilder::from(right)
                        .window(vec![right_row_number_window])?
                        .build()?;
                    let left_join_columns = join_keys
                        .iter()
                        .map(|(left_col, _)| left_col.clone())
                        .collect::<Vec<_>>();
                    join_keys.push((
                        Column::from_name(left_row_number_alias),
                        Column::from_name(right_row_number_alias),
                    ));
                    LogicalPlanBuilder::from(left)
                        .join_detailed(
                            right,
                            JoinType::LeftAnti,
                            join_keys.into_iter().unzip(),
                            None,
                            NullEquality::NullEqualsNull,
                        )?
                        .project(left_join_columns)?
                        .build()
                } else {
                    LogicalPlanBuilder::from(left)
                        .distinct()?
                        .join_detailed(
                            right,
                            JoinType::LeftAnti,
                            join_keys.into_iter().unzip(),
                            None,
                            NullEquality::NullEqualsNull,
                        )?
                        .build()
                }?;
                Ok(plan)
            }
        }
    }
}

fn promote_union_numeric_input(
    input: LogicalPlan,
    other_schema: &DFSchema,
    ansi_mode: bool,
    config: &PlanConfig,
    by_name: bool,
) -> PlanResult<LogicalPlan> {
    // Leave column-count validation to the UNION constructor.
    if input.schema().fields().len() != other_schema.fields().len() {
        return Ok(input);
    }
    let mut changed = false;
    let expressions = input
        .schema()
        .fields()
        .iter()
        .zip(other_schema.fields())
        .enumerate()
        .map(|(index, (field, other))| {
            let data_type = promote_union_numeric_type(
                field.data_type(),
                other.data_type(),
                ansi_mode,
                config,
                by_name,
                true,
            );
            let column = Expr::Column(Column::from(input.schema().qualified_field(index)));
            if data_type == *field.data_type() {
                Ok(column)
            } else {
                changed = true;
                let column = if has_string_numeric_coercion(field.data_type(), &data_type) {
                    ScalarUDF::from(SparkConditionalCast::new(data_type)).call(vec![column])
                } else {
                    column.cast_to(&data_type, input.schema())?
                };
                Ok(column.alias_with_metadata(field.name(), Some(field.metadata().clone().into())))
            }
        })
        .collect::<PlanResult<Vec<_>>>()?;
    if changed {
        Ok(project(input, expressions)?)
    } else {
        Ok(input)
    }
}

// Promote numeric combinations whose Spark common type differs from DataFusion,
// preserving unrelated leaves and metadata. ANSI STRING/numeric coercion is
// disabled inside map keys: Spark disallows key casts that can introduce NULL.
fn promote_union_numeric_type(
    data_type: &DataType,
    other_type: &DataType,
    ansi_mode: bool,
    config: &PlanConfig,
    by_name: bool,
    allow_string_numeric: bool,
) -> DataType {
    if ansi_mode
        && allow_string_numeric
        && ((data_type.is_string() && other_type.is_numeric())
            || (data_type.is_numeric() && other_type.is_string()))
    {
        return ansi_string_numeric_type(
            &[data_type.clone(), other_type.clone()],
            &DataType::Utf8,
            config,
        );
    }
    if wider_numeric_type(
        data_type,
        other_type,
        ansi_mode,
        config.legacy_decimal_retain_fraction_digits,
    ) == Some(DataType::Float64)
        && type_union_coercion(data_type, other_type) != Some(DataType::Float64)
    {
        return DataType::Float64;
    }
    let promote_field = |field: &FieldRef, other: &FieldRef, by_name, allow_string_numeric| {
        let data_type = promote_union_numeric_type(
            field.data_type(),
            other.data_type(),
            ansi_mode,
            config,
            by_name,
            allow_string_numeric,
        );
        let nullable =
            field.is_nullable() || (field.data_type().is_string() && data_type.is_numeric());
        Arc::new(
            field
                .as_ref()
                .clone()
                .with_data_type(data_type)
                .with_nullable(nullable),
        )
    };
    match (data_type, other_type) {
        (
            DataType::List(field) | DataType::LargeList(field) | DataType::FixedSizeList(field, _),
            DataType::List(other) | DataType::LargeList(other) | DataType::FixedSizeList(other, _),
        ) => {
            let field = promote_field(field, other, by_name, allow_string_numeric);
            match data_type {
                DataType::List(_) => DataType::List(field),
                DataType::LargeList(_) => DataType::LargeList(field),
                DataType::FixedSizeList(_, size) => DataType::FixedSizeList(field, *size),
                _ => unreachable!(),
            }
        }
        (DataType::Map(field, sorted), DataType::Map(other, _)) => {
            // Spark's ResolveUnion does not reorder structs nested inside maps.
            // TODO: Reject ANSI UNIONs with numeric/STRING map keys like Spark;
            // their required STRING casts can introduce NULL map keys.
            let (DataType::Struct(entries), DataType::Struct(other_entries)) =
                (field.data_type(), other.data_type())
            else {
                return data_type.clone();
            };
            if entries.len() != 2 || other_entries.len() != 2 {
                return data_type.clone();
            }
            DataType::Map(
                Arc::new(
                    field.as_ref().clone().with_data_type(DataType::Struct(
                        vec![
                            promote_field(&entries[0], &other_entries[0], false, false),
                            promote_field(
                                &entries[1],
                                &other_entries[1],
                                false,
                                allow_string_numeric,
                            ),
                        ]
                        .into(),
                    )),
                ),
                *sorted,
            )
        }
        (DataType::Struct(fields), DataType::Struct(others)) if fields.len() == others.len() => {
            // Match the name alignment used by the later UNION coercion before promoting
            // numeric leaves. Positional pairs can otherwise widen unrelated fields and
            // lose exact DECIMAL values when unionByName reorders a nested struct.
            // Spark keeps positional pairs when their names match its configured resolver.
            let reordered = by_name
                && fields.iter().zip(others).any(|(field, other)| {
                    if config.case_sensitive {
                        field.name() != other.name()
                    } else {
                        !union_field_names_equal_ignore_case(field.name(), other.name())
                    }
                });
            let others_by_name = reordered
                .then(|| {
                    others
                        .iter()
                        .map(|field| (field.name(), field))
                        .collect::<HashMap<_, _>>()
                })
                .filter(|others| fields.iter().all(|field| others.contains_key(field.name())));
            DataType::Struct(
                fields
                    .iter()
                    .zip(others)
                    .map(|(field, other)| {
                        let other = others_by_name
                            .as_ref()
                            .and_then(|others| others.get(field.name()).copied())
                            .unwrap_or(other);
                        promote_field(field, other, by_name, allow_string_numeric)
                    })
                    .collect(),
            )
        }
        _ => data_type.clone(),
    }
}

fn union_field_names_equal_ignore_case(left: &str, right: &str) -> bool {
    if left == right || (left.is_ascii() && right.is_ascii()) {
        return left.eq_ignore_ascii_case(right);
    }
    // Spark's resolver uses Java equalsIgnoreCase. Like the existing Delta resolver,
    // retain identity mappings for characters unassigned in OpenJDK 17's Unicode 13.
    #[expect(clippy::expect_used)]
    static ASSIGNED_CHARACTER: LazyLock<Regex> = LazyLock::new(|| {
        Regex::new(r"^\p{Age:13.0}$").expect("JDK 17 Unicode age pattern should be valid")
    });
    let fold = |character: char| {
        if character.is_ascii() {
            return character.to_ascii_lowercase();
        }
        let mut buffer = [0; 4];
        if !ASSIGNED_CHARACTER.is_match(character.encode_utf8(&mut buffer)) {
            return character;
        }
        let mut uppercase = character.to_uppercase();
        let first = uppercase.next().unwrap_or(character);
        // Java uses single-character mappings: full expansions such as ß -> SS must
        // not equate unrelated fields. Lowercasing also handles Greek titlecase pairs.
        let uppercase = if uppercase.next().is_some() {
            character
        } else {
            first
        };
        uppercase.to_lowercase().next().unwrap_or(uppercase)
    };
    left.chars().map(fold).eq(right.chars().map(fold))
}

// Keep the pre-analyzer type only at leaves where exposing DataFusion's common
// type would change existing consumers. Preserve widened types in sibling fields.
// TODO: Expose the LTZ common type of mixed NTZ/LTZ UNION inputs after timestamp
//  consumers preserve nullability and apply timezone conversions to that common type.
fn repair_union_type(data_type: &DataType, coerced_type: &DataType, ansi_mode: bool) -> DataType {
    // Preserve existing types where DataFusion matches ambiguous nested names
    // differently from Spark's resolver. Supported STRING/numeric pairs are cast above.
    if (ansi_mode && data_type.is_numeric() && coerced_type.is_string())
        || matches!(
            (data_type, coerced_type),
            (
                DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, _),
                DataType::Timestamp(TimeUnit::Nanosecond, _),
            ) | (
                DataType::Timestamp(_, None),
                DataType::Timestamp(_, Some(_))
            )
        )
    {
        return data_type.clone();
    }
    let repair_field = |field: &FieldRef, coerced_field: &FieldRef| {
        Arc::new(
            coerced_field
                .as_ref()
                .clone()
                .with_metadata(field.metadata().clone())
                .with_data_type(repair_union_type(
                    field.data_type(),
                    coerced_field.data_type(),
                    ansi_mode,
                )),
        )
    };
    match (data_type, coerced_type) {
        (
            DataType::List(field) | DataType::LargeList(field) | DataType::FixedSizeList(field, _),
            coerced_type,
        ) => match coerced_type {
            DataType::List(coerced_field) => DataType::List(repair_field(field, coerced_field)),
            DataType::LargeList(coerced_field) => {
                DataType::LargeList(repair_field(field, coerced_field))
            }
            DataType::FixedSizeList(coerced_field, size) => {
                DataType::FixedSizeList(repair_field(field, coerced_field), *size)
            }
            _ => coerced_type.clone(),
        },
        (DataType::Map(field, _), DataType::Map(coerced_field, sorted)) => {
            DataType::Map(repair_field(field, coerced_field), *sorted)
        }
        (DataType::Struct(fields), DataType::Struct(coerced_fields))
            if fields.len() == coerced_fields.len() =>
        {
            DataType::Struct(
                fields
                    .iter()
                    .zip(coerced_fields)
                    .map(|(field, coerced_field)| repair_field(field, coerced_field))
                    .collect(),
            )
        }
        _ => coerced_type.clone(),
    }
}

fn has_string_numeric_coercion(source: &DataType, target: &DataType) -> bool {
    if source.is_string() && target.is_numeric() {
        return true;
    }
    match (source, target) {
        (
            DataType::List(source)
            | DataType::LargeList(source)
            | DataType::FixedSizeList(source, _),
            DataType::List(target)
            | DataType::LargeList(target)
            | DataType::FixedSizeList(target, _),
        )
        | (DataType::Map(source, _), DataType::Map(target, _)) => {
            has_string_numeric_coercion(source.data_type(), target.data_type())
        }
        (DataType::Struct(source), DataType::Struct(target)) => {
            source.iter().zip(target).any(|(source, target)| {
                has_string_numeric_coercion(source.data_type(), target.data_type())
            })
        }
        _ => false,
    }
}
