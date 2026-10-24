use std::collections::HashMap;
use std::sync::Arc;

use datafusion_expr::expr::Expr;
use sail_common_datafusion::catalog::FunctionStatus;
mod metadata;

// Prepare asynchronous functions for physical planning.
pub fn prepare_async_functions(
    plan: datafusion_expr::LogicalPlan,
) -> datafusion_common::Result<datafusion_expr::LogicalPlan> {
    use datafusion_common::Column;
    use datafusion_common::tree_node::{
        Transformed, TransformedResult, TreeNode, TreeNodeRecursion,
    };
    use datafusion_expr::expr_rewriter::NamePreserver;
    use datafusion_expr::utils::{conjunction, split_conjunction_owned};
    use datafusion_expr::{Aggregate, Filter, JoinType, LogicalPlan, Projection};
    use sail_common_datafusion::utils::items::ItemTaker;
    use sail_logical_plan::sort::{RequiredSortNode, SortWithinPartitionsNode};

    let contains_async = |expr: &Expr| {
        expr.exists(|expr| {
            Ok(matches!(expr, Expr::ScalarFunction(function)
                if function.func.as_async().is_some()))
        })
    };
    // This scan avoids plan transformation for queries without async calls,
    // at the cost of another scan when they are present.
    let has_async = plan.apply_with_subqueries(|node| {
        node.apply_expressions(|expr| {
            Ok(if contains_async(expr)? {
                TreeNodeRecursion::Stop
            } else {
                TreeNodeRecursion::Continue
            })
        })
    })? == TreeNodeRecursion::Stop;
    if !has_async {
        return Ok(plan);
    }
    plan.transform_up_with_subqueries(|plan| {
        // Check each node before rewriting its expressions. Extracting an inner
        // async call first could hide unsupported nesting from this check.
        plan.apply_expressions(|expr| {
            expr.apply(|expr| {
                if let Expr::ScalarFunction(outer) = expr
                    && outer.func.as_async().is_some()
                {
                    for argument in &outer.args {
                        argument.apply(|nested| {
                            if let Expr::ScalarFunction(inner) = nested
                                && inner.func.as_async().is_some()
                            {
                                return datafusion_common::plan_err!(
                                    "Async calls cannot be nested inside another async call; materialize the inner result in a table before evaluating the outer call"
                                );
                            }
                            Ok(TreeNodeRecursion::Continue)
                        })?;
                    }
                }
                Ok(TreeNodeRecursion::Continue)
            })
        })?;
        match plan {
            LogicalPlan::Join(mut join) if join.join_type == JoinType::Inner => {
                let Some(predicate) = join.filter.take() else {
                    return Ok(Transformed::no(LogicalPlan::Join(join)));
                };
                if !contains_async(&predicate)? {
                    join.filter = Some(predicate);
                    return Ok(Transformed::no(LogicalPlan::Join(join)));
                }
                let mut asynchronous = Vec::new();
                let mut synchronous = Vec::new();
                for predicate in split_conjunction_owned(predicate) {
                    if contains_async(&predicate)? {
                        asynchronous.push(predicate);
                    } else {
                        synchronous.push(predicate);
                    }
                }
                join.filter = conjunction(synchronous);
                let input = Arc::new(LogicalPlan::Join(join));
                let Some(predicate) = conjunction(asynchronous) else {
                    return Ok(Transformed::no(Arc::unwrap_or_clone(input)));
                };
                Ok(Transformed::yes(LogicalPlan::Filter(Filter::try_new(
                    predicate, input,
                )?)))
            }
            plan if matches!(&plan, LogicalPlan::Sort(_))
                || matches!(&plan, LogicalPlan::Extension(extension)
                    if extension.node.as_any().is::<SortWithinPartitionsNode>()
                        || extension.node.as_any().is::<RequiredSortNode>()) =>
            {
                let input = Arc::new(plan.inputs().one()?.clone());
                let output = input
                    .schema()
                    .columns()
                    .into_iter()
                    .map(Expr::Column)
                    .collect::<Vec<_>>();
                let mut projection = output.clone();
                let mut expressions = plan.expressions();
                let mut next_alias = 0usize;
                for expression in &mut expressions {
                    if !contains_async(expression)? {
                        continue;
                    }
                    let name = loop {
                        let name = format!("__sail_async_sort_{next_alias}");
                        next_alias += 1;
                        if !input.schema().has_column_with_unqualified_name(&name) {
                            break name;
                        }
                    };
                    projection.push(expression.clone().alias(name.clone()));
                    *expression = Expr::Column(Column::from_name(name));
                }
                if projection.len() == output.len() {
                    return Ok(Transformed::no(plan));
                }
                let input = LogicalPlan::Projection(Projection::try_new(projection, input)?);
                let sort = plan.with_new_exprs(expressions, vec![input])?;
                Ok(Transformed::yes(LogicalPlan::Projection(Projection::try_new(
                    output,
                    Arc::new(sort),
                )?)))
            }
            LogicalPlan::Aggregate(mut aggregate) => {
                // DataFusion 55.1.0 assigns the same async result-column offset to
                // different aggregates. Project async calls first so each aggregate
                // reads its own result column.
                let mut projection = aggregate
                    .input
                    .schema()
                    .columns()
                    .into_iter()
                    .map(Expr::Column)
                    .collect::<Vec<_>>();
                let input_columns = projection.len();
                let mut next_alias = 0usize;
                let mut group_columns: HashMap<Expr, String> = HashMap::new();
                let mut project_group = |expr: Expr| -> datafusion_common::Result<Expr> {
                    if !contains_async(&expr)? {
                        return Ok(expr);
                    }
                    let saved_name = NamePreserver::new_for_projection().save(&expr);
                    let name = if let Some(name) = group_columns.get(&expr) {
                        name.clone()
                    } else {
                        let name = loop {
                            let name = format!("__sail_async_group_{next_alias}");
                            next_alias += 1;
                            if !aggregate.input.schema().has_column_with_unqualified_name(&name) {
                                break name;
                            }
                        };
                        projection.push(expr.clone().alias(name.clone()));
                        group_columns.insert(expr, name.clone());
                        name
                    };
                    Ok(saved_name.restore(Expr::Column(Column::from_name(name))))
                };
                aggregate.group_expr = std::mem::take(&mut aggregate.group_expr)
                    .into_iter()
                    .map(|expr| {
                        if matches!(&expr, Expr::GroupingSet(_)) {
                            expr.map_children(|expr| project_group(expr).map(Transformed::yes))
                                .data()
                        } else {
                            project_group(expr)
                        }
                    })
                    .collect::<datafusion_common::Result<Vec<_>>>()?;
                let expressions = std::mem::take(&mut aggregate.aggr_expr)
                    .into_iter()
                    .map(|expr| {
                        let name = NamePreserver::new_for_projection().save(&expr);
                        let rewritten = expr
                            .transform_up(|expr| {
                                if let Expr::ScalarFunction(function) = &expr
                                    && function.func.as_async().is_some()
                                {
                                    let name = loop {
                                        let name = format!("__sail_async_aggregate_{next_alias}");
                                        next_alias += 1;
                                        if !aggregate
                                            .input
                                            .schema()
                                            .has_column_with_unqualified_name(&name)
                                        {
                                            break name;
                                        }
                                    };
                                    projection.push(expr.alias(name.clone()));
                                    return Ok(Transformed::yes(Expr::Column(Column::from_name(name))));
                                }
                                Ok(Transformed::no(expr))
                            })
                            .data()?;
                        Ok(name.restore(rewritten))
                    })
                    .collect::<datafusion_common::Result<Vec<_>>>()?;
                aggregate.aggr_expr = expressions;
                if projection.len() == input_columns {
                    return Ok(Transformed::no(LogicalPlan::Aggregate(aggregate)));
                }
                let input = Arc::new(LogicalPlan::Projection(Projection::try_new(
                    projection,
                    aggregate.input,
                )?));
                Ok(Transformed::yes(LogicalPlan::Aggregate(
                    Aggregate::try_new_with_schema(
                        input,
                        aggregate.group_expr,
                        aggregate.aggr_expr,
                        aggregate.schema,
                    )?,
                )))
            }
            plan => Ok(Transformed::no(plan)),
        }
    })
    .data()
}

const BUILT_IN_FUNCTION_NAMES: &[&str] = &[
    "!",
    "!=",
    "%",
    "&",
    "*",
    "+",
    "-",
    "/",
    "<",
    "<<",
    "<=",
    "<=>",
    "<>",
    "=",
    "==",
    ">",
    ">=",
    ">>",
    ">>>",
    "^",
    "abs",
    "acos",
    "acosh",
    "add_days",
    "add_months",
    "add_years",
    "aes_decrypt",
    "aes_encrypt",
    "aggregate",
    "and",
    "any",
    "any_value",
    "approx_count_distinct",
    "approx_percentile",
    "array",
    "array_agg",
    "array_append",
    "array_compact",
    "array_concat",
    "array_contains",
    "array_contains_all",
    "array_distinct",
    "array_except",
    "array_insert",
    "array_intersect",
    "array_join",
    "array_max",
    "array_min",
    "array_position",
    "array_prepend",
    "array_remove",
    "array_repeat",
    "array_size",
    "array_sort",
    "array_union",
    "arrays_overlap",
    "arrays_zip",
    "ascii",
    "asin",
    "asinh",
    "assert_true",
    "atan",
    "atan2",
    "atanh",
    "avg",
    "base64",
    "between",
    "bigint",
    "bin",
    "binary",
    "bit_and",
    "bit_count",
    "bit_get",
    "bit_length",
    "bit_or",
    "bit_xor",
    "bitmap_and_agg",
    "bitmap_bit_position",
    "bitmap_bucket_number",
    "bitmap_construct_agg",
    "bitmap_count",
    "bitmap_or_agg",
    "bitwise_not",
    "bool_and",
    "bool_or",
    "boolean",
    "bround",
    "btrim",
    "cardinality",
    "case",
    "cast",
    "cbrt",
    "ceil",
    "ceiling",
    "char",
    "char_length",
    "character_length",
    "chr",
    "coalesce",
    "collate",
    "collation",
    "collect_list",
    "collect_set",
    "concat",
    "concat_ws",
    "contains",
    "conv",
    "convert_timezone",
    "corr",
    "cos",
    "cosh",
    "cot",
    "count",
    "count_if",
    "count_min_sketch",
    "covar_pop",
    "covar_samp",
    "crc32",
    "csc",
    "cume_dist",
    "curdate",
    "current_catalog",
    "current_database",
    "current_date",
    "current_path",
    "current_schema",
    "current_time",
    "current_timestamp",
    "current_timezone",
    "current_user",
    "date",
    "date_add",
    "date_diff",
    "date_format",
    "date_from_unix_date",
    "date_part",
    "date_sub",
    "date_trunc",
    "dateadd",
    "datediff",
    "datepart",
    "day",
    "dayname",
    "dayofmonth",
    "dayofweek",
    "dayofyear",
    "decimal",
    "decode",
    "deep_size",
    "degrees",
    "dense_rank",
    "div",
    "double",
    "e",
    "element_at",
    "elt",
    "encode",
    "endswith",
    "equal_null",
    "every",
    "exists",
    "exp",
    "explode",
    "explode_outer",
    "expm1",
    "extract",
    "factorial",
    "filter",
    "find_in_set",
    "first",
    "first_value",
    "flatten",
    "float",
    "floor",
    "forall",
    "format_number",
    "format_string",
    "from_avro",
    "from_csv",
    "from_json",
    "from_protobuf",
    "from_unixtime",
    "from_utc_timestamp",
    "from_xml",
    "get",
    "get_json_object",
    "getbit",
    "greatest",
    "grouping",
    "grouping_id",
    "hash",
    "hex",
    "histogram_numeric",
    "hll_sketch_agg",
    "hll_sketch_estimate",
    "hll_union",
    "hll_union_agg",
    "hour",
    "hypot",
    "if",
    "ifnull",
    "ilike",
    "in",
    "initcap",
    "inline",
    "inline_outer",
    "input_file_block_length",
    "input_file_block_start",
    "input_file_name",
    "instr",
    "int",
    "is_valid_utf8",
    "is_valid_variant",
    "is_variant_null",
    "isnan",
    "isnotnull",
    "isnull",
    "java_method",
    "json_array_length",
    "json_object_keys",
    "json_tuple",
    "kll_merge_agg_bigint",
    "kll_merge_agg_double",
    "kll_merge_agg_float",
    "kurtosis",
    "lag",
    "last",
    "last_day",
    "last_value",
    "lcase",
    "lead",
    "least",
    "left",
    "len",
    "length",
    "levenshtein",
    "like",
    "listagg",
    "ln",
    "localtimestamp",
    "locate",
    "log",
    "log10",
    "log1p",
    "log2",
    "lower",
    "lpad",
    "ltrim",
    "luhn_check",
    "make_date",
    "make_dt_interval",
    "make_interval",
    "make_time",
    "make_timestamp",
    "make_timestamp_ltz",
    "make_timestamp_ntz",
    "make_valid_utf8",
    "make_ym_interval",
    "map",
    "map_concat",
    "map_contains_key",
    "map_entries",
    "map_filter",
    "map_from_arrays",
    "map_from_entries",
    "map_keys",
    "map_values",
    "map_zip_with",
    "mask",
    "max",
    "max_by",
    "md5",
    "mean",
    "measure",
    "median",
    "min",
    "min_by",
    "minute",
    "mod",
    "mode",
    "monotonically_increasing_id",
    "month",
    "monthname",
    "months_between",
    "named_struct",
    "nanvl",
    "negative",
    "next_day",
    "not",
    "now",
    "nth_value",
    "ntile",
    "nullif",
    "nullifzero",
    "nvl",
    "nvl2",
    "octet_length",
    "or",
    "overlay",
    "parse_json",
    "parse_url",
    "percent_rank",
    "percentile",
    "percentile_approx",
    "percentile_cont",
    "percentile_disc",
    "pi",
    "pmod",
    "posexplode",
    "posexplode_outer",
    "position",
    "positive",
    "pow",
    "power",
    "printf",
    "product",
    "quarter",
    "quote",
    "radians",
    "raise_error",
    "rand",
    "randn",
    "random",
    "random_poisson",
    "randstr",
    "range",
    "rank",
    "reduce",
    "reflect",
    "regexp",
    "regexp_count",
    "regexp_extract",
    "regexp_extract_all",
    "regexp_instr",
    "regexp_like",
    "regexp_replace",
    "regexp_substr",
    "regr_avgx",
    "regr_avgy",
    "regr_count",
    "regr_intercept",
    "regr_r2",
    "regr_slope",
    "regr_sxx",
    "regr_sxy",
    "regr_syy",
    "repeat",
    "replace",
    "reverse",
    "right",
    "rint",
    "rlike",
    "round",
    "row_number",
    "rpad",
    "rtrim",
    "schema_of_avro",
    "schema_of_csv",
    "schema_of_json",
    "schema_of_variant",
    "schema_of_variant_agg",
    "schema_of_xml",
    "sec",
    "second",
    "sentences",
    "sequence",
    "session_user",
    "session_window",
    "sha",
    "sha1",
    "sha2",
    "shiftleft",
    "shiftright",
    "shiftrightunsigned",
    "shuffle",
    "sign",
    "signum",
    "sin",
    "sinh",
    "size",
    "skewness",
    "slice",
    "smallint",
    "some",
    "sort_array",
    "soundex",
    "space",
    "spark_partition_id",
    "split",
    "split_part",
    "sqrt",
    "st_asbinary",
    "st_geogfromwkb",
    "st_geomfromwkb",
    "st_setsrid",
    "st_srid",
    "stack",
    "startswith",
    "std",
    "stddev",
    "stddev_pop",
    "stddev_samp",
    "str_to_map",
    "string",
    "string_agg",
    "strpos",
    "struct",
    "substr",
    "substring",
    "substring_index",
    "sum",
    "tan",
    "tanh",
    "theta_difference",
    "theta_intersection",
    "theta_intersection_agg",
    "theta_sketch_agg",
    "theta_sketch_estimate",
    "theta_union",
    "theta_union_agg",
    "time",
    "time_bucket",
    "time_diff",
    "time_from_micros",
    "time_from_millis",
    "time_from_seconds",
    "time_to_micros",
    "time_to_millis",
    "time_to_seconds",
    "time_trunc",
    "timestamp",
    "timestamp_add",
    "timestamp_diff",
    "timestamp_micros",
    "timestamp_millis",
    "timestamp_seconds",
    "timestampadd",
    "timestampdiff",
    "tinyint",
    "to_avro",
    "to_binary",
    "to_char",
    "to_csv",
    "to_date",
    "to_json",
    "to_number",
    "to_protobuf",
    "to_time",
    "to_timestamp",
    "to_timestamp_ltz",
    "to_timestamp_ntz",
    "to_unix_timestamp",
    "to_utc_timestamp",
    "to_varchar",
    "to_variant_object",
    "to_xml",
    "transform",
    "transform_keys",
    "transform_values",
    "translate",
    "trim",
    "trunc",
    "try_add",
    "try_aes_decrypt",
    "try_aes_encrypt",
    "try_avg",
    "try_divide",
    "try_element_at",
    "try_make_interval",
    "try_make_timestamp",
    "try_make_timestamp_ltz",
    "try_make_timestamp_ntz",
    "try_mod",
    "try_multiply",
    "try_parse_json",
    "try_parse_url",
    "try_reflect",
    "try_subtract",
    "try_sum",
    "try_to_binary",
    "try_to_number",
    "try_to_time",
    "try_to_timestamp",
    "try_url_decode",
    "try_validate_utf8",
    "try_variant_get",
    "tuple_difference_double",
    "tuple_difference_integer",
    "tuple_difference_theta_double",
    "tuple_difference_theta_integer",
    "tuple_intersection_agg_double",
    "tuple_intersection_agg_integer",
    "tuple_intersection_double",
    "tuple_intersection_integer",
    "tuple_intersection_theta_double",
    "tuple_intersection_theta_integer",
    "tuple_sketch_agg_double",
    "tuple_sketch_agg_integer",
    "tuple_sketch_estimate_double",
    "tuple_sketch_estimate_integer",
    "tuple_sketch_summary_double",
    "tuple_sketch_summary_integer",
    "tuple_sketch_theta_double",
    "tuple_sketch_theta_integer",
    "tuple_union_agg_double",
    "tuple_union_agg_integer",
    "tuple_union_double",
    "tuple_union_integer",
    "tuple_union_theta_double",
    "tuple_union_theta_integer",
    "typeof",
    "ucase",
    "unbase64",
    "unhex",
    "uniform",
    "unix_date",
    "unix_micros",
    "unix_millis",
    "unix_seconds",
    "unix_timestamp",
    "upper",
    "url_decode",
    "url_encode",
    "user",
    "uuid",
    "validate_utf8",
    "var_pop",
    "var_samp",
    "variance",
    "variant_explode",
    "variant_explode_outer",
    "variant_get",
    "variant_to_json",
    "vector_avg",
    "vector_cosine_similarity",
    "vector_inner_product",
    "vector_l2_distance",
    "vector_norm",
    "vector_normalize",
    "vector_sum",
    "version",
    "weekday",
    "weekofyear",
    "when",
    "width_bucket",
    "window",
    "window_time",
    "xpath",
    "xpath_boolean",
    "xpath_double",
    "xpath_float",
    "xpath_int",
    "xpath_long",
    "xpath_number",
    "xpath_short",
    "xpath_string",
    "xxhash64",
    "year",
    "years",
    "zeroifnull",
    "zip_with",
    "|",
    "||",
    "~",
];
pub(crate) fn list_built_in_function_statuses() -> Vec<FunctionStatus> {
    BUILT_IN_FUNCTION_NAMES
        .iter()
        .filter_map(|name| metadata::built_in_public_function_status(name))
        .collect()
}
pub use crate::analysis::typing::temporal::is_spark_compatible_arrow_fixed_offset;
#[cfg(test)]
mod tests {
    use super::is_spark_compatible_arrow_fixed_offset;

    #[test]
    fn spark_compatible_arrow_fixed_offset_matrix() {
        for timezone in [
            "+00", "+01", "+0130", "+01:30", "-00:00", "-01", "-0130", "-01:30", "+18", "+1800",
            "+18:00", "-18", "-1800", "-18:00",
        ] {
            assert!(
                is_spark_compatible_arrow_fixed_offset(timezone),
                "{timezone}"
            );
        }

        for timezone in [
            "",
            "Z",
            "UTC",
            "UTC+01:30",
            "GMT+01:30",
            "+1:30",
            "+01:3",
            "+01:30:15",
            "+0160",
            "+18:01",
            "+1801",
            "+23:59",
            "-18:01",
            "America/Los_Angeles",
        ] {
            assert!(
                !is_spark_compatible_arrow_fixed_offset(timezone),
                "{timezone}"
            );
        }
    }
}
