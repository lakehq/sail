//! These tests exercise the analysis and lowering boundary without executing a query.

use std::sync::Arc;

use arrow_schema::DataType;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::SessionContext;
use sail_common::spec;
use sail_common_datafusion::catalog::display::DefaultCatalogDisplay;
use sail_common_datafusion::session::plan::PlanService;

use super::ResolveQuery;
use crate::analysis::deferred::Deferred;
use crate::analysis::ir::{ExpressionNode, IdGenerator, LogicalQuery, PlanNode};
use crate::analysis::{AnalysisContext, AnalysisPipeline};
use crate::catalog::SparkCatalogObjectDisplay;
use crate::config::PlanConfig;
use crate::error::{PlanError, PlanResult};
use crate::formatter::SparkPlanFormatter;

fn session() -> SessionContext {
    let mut state = SessionStateBuilder::new().with_default_features().build();
    state.config_mut().set_extension(Arc::new(PlanService::new(
        Box::new(DefaultCatalogDisplay::<SparkCatalogObjectDisplay>::default()),
        Box::new(SparkPlanFormatter),
    )));
    SessionContext::new_with_state(state)
}

async fn analyze(sql: &str) -> PlanResult<LogicalQuery> {
    let plan = sail_sql_analyzer::statement::from_ast_statement(
        sail_sql_analyzer::parser::parse_one_statement(sql)?,
    )?;
    let spec::Plan::Query(query) = plan else {
        return Err(PlanError::internal("expected query"));
    };
    analyze_request(query).await
}

async fn analyze_request(query: spec::QueryPlan) -> PlanResult<LogicalQuery> {
    let session = session();
    AnalysisPipeline::new(ResolveQuery)
        .analyze(
            query.into(),
            &mut AnalysisContext {
                session: &session,
                config: Arc::new(PlanConfig::new()?),
                ids: IdGenerator::default(),
            },
        )
        .await
}

#[tokio::test]
async fn native_resolution_inserts_casts_and_preserves_output_types() -> PlanResult<()> {
    let query =
        analyze("SELECT abs(value) AS magnitude, array(1, CAST(2 AS BIGINT)) AS items FROM VALUES ('-2') t(value)").await?;
    let schema = query.plan.schema.resolved("test output")?;
    assert_eq!(
        schema.attributes[0]
            .result_type
            .resolved("magnitude type")?
            .data_type,
        DataType::Float64
    );
    assert!(
        !schema.attributes[1]
            .result_type
            .resolved("array type")?
            .nullable
    );
    let Deferred::Resolved(PlanNode::Project { expressions, .. }) = &query.plan.node else {
        return Err(PlanError::internal("expected projection"));
    };
    let Deferred::Resolved(ExpressionNode::WithMetadata(value)) = &expressions[0].expression.node
    else {
        return Err(PlanError::internal("expected alias"));
    };
    let Deferred::Resolved(ExpressionNode::ScalarFunction { arguments, .. }) = &value.node else {
        return Err(PlanError::internal("expected scalar call"));
    };
    assert!(matches!(
        arguments[0].node,
        Deferred::Resolved(ExpressionNode::Cast { .. })
    ));
    assert_eq!(
        query.into_named_plan()?.fields,
        Some(vec!["magnitude".into(), "items".into()])
    );
    Ok(())
}

#[tokio::test]
async fn native_attributes_survive_aliases_joins_and_duplicate_output_names() -> PlanResult<()> {
    let query = analyze("SELECT a.x AS same, b.x AS same FROM (VALUES (1)) a(x) JOIN (VALUES (1)) b(x) ON a.x = b.x").await?;
    let fields = &query.plan.schema.resolved("test output")?.attributes;
    assert_ne!(fields[0].id, fields[1].id);
    assert_ne!(fields[0].origin, fields[1].origin);
    let plan = query.into_named_plan()?;
    assert_eq!(plan.fields, Some(vec!["same".into(), "same".into()]));
    assert_ne!(
        plan.plan.schema().field(0).name(),
        plan.plan.schema().field(1).name()
    );
    Ok(())
}

#[tokio::test]
async fn native_aggregate_extracts_calls_before_projecting() -> PlanResult<()> {
    let query =
        analyze("SELECT x, sum(y) + 1 AS total FROM (VALUES (1, 2), (1, 3)) t(x, y) GROUP BY x")
            .await?;
    let Deferred::Resolved(PlanNode::Project { input, .. }) = &query.plan.node else {
        return Err(PlanError::internal("expected project"));
    };
    assert!(matches!(
        input.node,
        Deferred::Resolved(PlanNode::Aggregate { .. })
    ));
    query.into_named_plan()?;
    assert!(
        analyze("SELECT x, sum(y) FROM (VALUES (1, 2)) t(x, y)")
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn native_correlated_subquery_binds_outer_attribute_ids() -> PlanResult<()> {
    let query = analyze("SELECT x, (SELECT max(b.y) FROM (VALUES (1), (2)) b(y) WHERE b.y = a.x) AS m FROM (VALUES (1), (2)) a(x)").await?;
    query.into_named_plan()?;
    Ok(())
}

#[tokio::test]
async fn native_union_and_case_resolve_common_types() -> PlanResult<()> {
    let query = analyze("SELECT CASE WHEN x IS NOT NULL THEN CAST(x AS BIGINT) ELSE 0 END AS value FROM (VALUES (1), (NULL)) t(x) UNION ALL SELECT CAST(2 AS BIGINT)").await?;
    let schema = query.plan.schema.resolved("test output")?;
    assert_eq!(
        schema.attributes[0]
            .result_type
            .resolved("union type")?
            .data_type,
        DataType::Int64
    );
    query.into_named_plan()?;
    Ok(())
}

#[tokio::test]
async fn native_lambda_bodies_are_typed_before_lowering() -> PlanResult<()> {
    for sql in [
        "SELECT transform(array('-2', '-1'), x -> abs(x)) AS items",
        "SELECT transform(array(10, 20), (x, i) -> i + 1) AS positions",
        "SELECT transform(array(1, 2), x -> transform(array(3), y -> x + y)) AS nested",
        "SELECT aggregate(array(1, 2), 0, (acc, x) -> acc + x)",
        "SELECT array_sort(array(2, 1), (x, y) -> x - y)",
        "SELECT transform(array(struct(1 AS a)), x -> x.a)",
        "SELECT map_filter(map(1, 2), true)",
        "SELECT map_filter(NULL, NULL)",
        "SELECT transform_keys(map(1, 2), 'key')",
        "SELECT transform_values(map(1, 2), 10)",
        "SELECT array_sort(array(2, 1), (x, y) -> y)",
        "SELECT aggregate(array(1, 2), 0, (acc, x) -> x)",
    ] {
        analyze(sql).await?.into_named_plan()?;
    }
    Ok(())
}

#[tokio::test]
async fn native_using_join_keeps_qualified_key_bindings() -> PlanResult<()> {
    for kind in ["JOIN", "LEFT JOIN", "FULL JOIN"] {
        let query = analyze(&format!("SELECT x, a.x AS left_key, b.x AS right_key FROM (VALUES (1)) a(x) {kind} (VALUES (CAST(1 AS BIGINT))) b(x) USING (x)")).await?;
        query.into_named_plan()?;
    }
    Ok(())
}

#[tokio::test]
async fn native_window_calls_are_materialized_before_projection() -> PlanResult<()> {
    let query =
        analyze("SELECT x, row_number() OVER (ORDER BY x) AS n FROM (VALUES (1), (2)) t(x)")
            .await?;
    query.into_named_plan()?;
    Ok(())
}

#[tokio::test]
async fn native_predicates_and_scalar_subqueries_have_resolved_types() -> PlanResult<()> {
    for sql in [
        "SELECT 1 BETWEEN 0 AND CAST(2 AS BIGINT)",
        "SELECT NULL IS DISTINCT FROM 1, NULL <=> NULL",
        "SELECT (SELECT 1) AS value",
        "SELECT x IN (SELECT CAST(y AS BIGINT) FROM (VALUES (1), (NULL)) b(y)) FROM (VALUES (1)) a(x)",
        "SELECT -x FROM (VALUES (1)) a(x)",
    ] {
        analyze(sql).await?.into_named_plan()?;
    }
    Ok(())
}

#[tokio::test]
async fn native_ctes_capture_declaration_scope_and_fresh_instances() -> PlanResult<()> {
    for sql in [
        "WITH a AS (SELECT 1 AS x), b AS (SELECT * FROM a) SELECT * FROM (WITH a AS (SELECT 2 AS y) SELECT * FROM b)",
        "WITH a AS (SELECT 1 AS x) SELECT l.x, r.x FROM a l JOIN a r ON l.x = r.x",
    ] {
        analyze(sql).await?.into_named_plan()?;
    }
    assert!(
        analyze("WITH a AS (SELECT * FROM b), b AS (SELECT 1 AS x) SELECT * FROM a")
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn native_set_operations_resolve_each_input_before_combining() -> PlanResult<()> {
    for op in ["INTERSECT", "INTERSECT ALL", "EXCEPT", "EXCEPT ALL"] {
        let sql = format!(
            "SELECT x FROM (VALUES (1), (NULL)) a(x) {op} SELECT CAST(y AS BIGINT) FROM (VALUES (1), (1)) b(y)"
        );
        analyze(&sql).await?.into_named_plan()?;
    }
    Ok(())
}

#[tokio::test]
async fn native_range_window_offsets_match_ordering_type() -> PlanResult<()> {
    let query = analyze("SELECT sum(x) OVER (ORDER BY x RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) FROM (VALUES (1), (2)) a(x)").await?;
    query.into_named_plan()?;
    assert!(analyze("SELECT sum(x) OVER (ORDER BY x, y RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) FROM (VALUES (1, 2)) a(x,y)").await.is_err());
    Ok(())
}

#[tokio::test]
async fn native_struct_fields_and_wildcards_preserve_bindings() -> PlanResult<()> {
    for sql in [
        "SELECT s.x FROM (SELECT struct(1 AS x, 2 AS y) AS s)",
        "SELECT a.* FROM (VALUES (1, 2)) a(x,y) JOIN (VALUES (1, 3)) b(x,z) USING (x)",
        "SELECT t.* FROM (SELECT * FROM (VALUES (1)) a(x) JOIN (VALUES (1)) b(x) USING (x)) t",
        "SELECT struct(*) FROM (VALUES (1, 2)) a(x,y)",
        "SELECT explode(array(1, 2)) AS x",
        "SELECT * FROM (SELECT * FROM VALUES ('a', 1), ('b', 2) AS t(k, v)) PIVOT (sum(v) FOR (k) IN ('a', 'b'))",
        "SELECT * FROM (SELECT * FROM VALUES (1, 2) AS t(a, b)) UNPIVOT (v FOR k IN (a, b))",
        "SELECT * FROM VALUES (1), (2) AS t(x), LATERAL (SELECT t.x + 1 AS y)",
        "SELECT posexplode_outer(array(1, 2)) AS (p, x)",
        "SELECT inline(array(struct(1 AS x, 2 AS y)))",
        "SELECT /*+ COALESCE(1) */ x FROM (VALUES (1)) t(x)",
        "SELECT /*+ REPARTITION(2, x) */ x FROM (VALUES (1)) t(x)",
        "SELECT * FROM explode(array(1, 2))",
        "SELECT * FROM (VALUES (1)) t(x) LATERAL VIEW explode(array(x, 2)) e AS y",
    ] {
        analyze(sql).await?.into_named_plan()?;
    }
    let query =
        analyze("SELECT a.* FROM (VALUES (1, 2)) a(x,y) JOIN (VALUES (1, 3)) b(x,z) USING (x)")
            .await?;
    assert_eq!(
        query.into_named_plan()?.fields,
        Some(vec!["x".into(), "y".into()])
    );
    Ok(())
}

#[tokio::test]
async fn native_instances_refresh_ids_across_correlations_and_lambdas() -> PlanResult<()> {
    let query = analyze("SELECT x, transform(array(1), y -> x + y), (SELECT max(z) FROM (VALUES (2)) b(z) WHERE z = a.x) FROM (VALUES (1)) a(x)").await?;
    let mut ids = IdGenerator::default();
    let first = query.plan.instantiate(&mut ids)?;
    let second = query.plan.instantiate(&mut ids)?;
    let a = &first.schema.resolved("first instance")?.attributes;
    let b = &second.schema.resolved("second instance")?.attributes;
    assert!(
        a.iter()
            .all(|a| b.iter().all(|b| a.id != b.id && a.origin != b.origin))
    );
    LogicalQuery { plan: first }.into_named_plan()?;
    LogicalQuery { plan: second }.into_named_plan()?;
    Ok(())
}

#[tokio::test]
async fn native_compound_functions_lower_with_resolved_fields() -> PlanResult<()> {
    for sql in [
        "SELECT id, sum(id), grouping(id), grouping_id() FROM range(3) GROUP BY ROLLUP(id)",
        "SELECT id + 1, count(*) FROM range(3) GROUP BY CUBE(id)",
        "SELECT id, grouping_id(id) FROM range(3) GROUP BY GROUPING SETS ((id), (), (id))",
        "SELECT id + 1 AS value, count(*) AS n FROM range(3) GROUP BY value HAVING n > 0 ORDER BY 1",
        "SELECT id + 1 AS value, count(*) FROM range(3) GROUP BY 1",
        "SELECT 1 FROM (SELECT id, id + 1 AS other FROM range(3)) ORDER BY other",
        "SELECT id FROM range(3) GROUP BY id ORDER BY sum(id)",
        "SELECT window, count(*) FROM (SELECT TIMESTAMP '2024-01-01' AS ts) GROUP BY window(ts, '10 seconds')",
        "SELECT window(TIMESTAMP '2024-01-01', '10 seconds'), json_tuple('{\"a\":1}', 'a')",
        "SELECT sum('1'), avg(1), first(1, true), last(1), collect_list(1), collect_set(1), count(1, 2), count_if(true), median(1), percentile(1, 0.5), listagg('a', ',')",
        "SELECT kurtosis(1), skewness(1), product(1), min_by(1, 2), max_by(1, 2), mode(1), hll_sketch_agg(1), theta_sketch_agg(1), try_avg(1), try_sum(1)",
        "SELECT row_number() OVER (), first(1, true) OVER (), count_if(true) OVER (), collect_set(1) OVER (), sum('1') OVER (), median(1) OVER (), listagg('a') OVER ()",
        "SELECT coalesce(NULL, 1), if(true, 1, 2), nvl(NULL, 1), nullifzero(1), zeroifnull(NULL)",
        "SELECT ascii('a'), length('a'), substring('abc', 1, 2), locate('a', 'abc'), regexp_instr('abc', 'a'), regexp_substr('abc', 'a')",
        "SELECT current_timezone(), current_timestamp(), localtimestamp(), date_format(DATE '2024-01-01', 'yyyy'), trunc(DATE '2024-01-01', 'MM')",
        "SELECT to_date('2024-01-01'), to_timestamp('2024-01-01'), unix_timestamp(), from_unixtime(0), dayofweek(DATE '2024-01-01')",
        "SELECT add_months(DATE '2024-01-01', 1), date_add(DATE '2024-01-01', 2), timestampadd('DAY', 1, TIMESTAMP '2024-01-01')",
        "SELECT datediff(DATE '2024-01-02', DATE '2024-01-01'), timestampdiff('MONTH', TIMESTAMP '2024-01-01', TIMESTAMP '2024-02-01'), months_between(DATE '2024-02-01', DATE '2024-01-01')",
        "SELECT convert_timezone('UTC', 'Asia/Shanghai', TIMESTAMP_NTZ '2024-01-01'), from_utc_timestamp(TIMESTAMP '2024-01-01', 'Asia/Shanghai'), make_timestamp(2024, 1, 1, 0, 0, 0)",
        "SELECT DATE '2024-02-29', TIME '12:34:56.123456789', TIMESTAMP '2024-01-01 00:00:00+08:00', TIMESTAMP_NTZ '2024-01-01 00:00:00+08:00'",
        "SELECT 4 / 2, 4 div 2, mod(4, 3), -1, negative('2'), ceil(1.23, -1), floor(1.23)",
        "SELECT ln(0), log(10, 100), log1p(0), hypot(3, 4), rint(1.2), round('1.2'), sqrt(4), csc(1), sec(1)",
        "SELECT size(array(1, 2)), element_at(array(1, 2), 1), try_element_at(array(1), 3)",
        "SELECT map(1, 2), map_from_arrays(array(1), array(2)), map_from_entries(array(struct(1, 2)))",
        "SELECT map_values(map(1, 2)), map_entries(map(1, 2)), map_contains_key(map(1, 2), 1), str_to_map('a:1')",
        "SELECT map_concat(map(1, 2), map(3, 4))",
        "SELECT sequence(1, 3), array_insert(array(1), 2, 3)",
        "SELECT 'abc' LIKE 'a%', 'abc' ILIKE 'A%', 'abc' RLIKE 'a.*'",
        "SELECT from_json('{\"a\":1}', 'a INT'), to_json(struct(1)), get_json_object('{\"a\":1}', '$.a')",
        "SELECT from_csv('1', 'a INT'), to_csv(struct(1)), xpath_int('<a>1</a>', 'a')",
        "SELECT CAST(1 AS TIMESTAMP), CAST('2024-01-01' AS DATE), CAST(array(1) AS STRING), CAST(struct(1) AS STRUCT<a: INT>)",
        "SELECT CAST(1 AS VARIANT), CAST(parse_json('1') AS INT)",
        "SELECT array_repeat(1, 3), arrays_zip(array(1), array(2)), slice(array(1, 2), 1, 1), sort_array(array(2, 1))",
        "SELECT array_append(array(1), 2), array_prepend(array(1), 2), get(array(1), 0), array_contains(array(1), 1), array_contains_all(array(1), array(1))",
        "SELECT flatten(array(array(1))), arrays_overlap(array(1), array(1)), array_size(array(1)), array_remove(array(1), 1)",
    ] {
        analyze(sql)
            .await
            .map_err(|e| PlanError::internal(format!("{sql}: {e}")))?
            .into_named_plan()
            .map_err(|e| PlanError::internal(format!("{sql}: {e}")))?;
    }
    Ok(())
}

#[tokio::test]
async fn native_parameters_are_values_before_function_analysis() -> PlanResult<()> {
    let plan = sail_sql_analyzer::statement::from_ast_statement(
        sail_sql_analyzer::parser::parse_one_statement("SELECT round(:value)")?,
    )?;
    let spec::Plan::Query(input) = plan else {
        return Err(PlanError::internal("expected query"));
    };
    let query = spec::QueryPlan {
        plan_id: None,
        node: spec::QueryNode::WithParameters {
            input: Box::new(input.clone()),
            positional_arguments: vec![],
            named_arguments: vec![(
                "value".into(),
                spec::Expr::Literal(spec::Literal::Utf8 {
                    value: Some("1.2".into()),
                }),
            )],
        },
    };
    let query = analyze_request(query).await?;
    assert_eq!(
        query.plan.schema.resolved("parameter result")?.attributes[0]
            .result_type
            .resolved("parameter result type")?
            .data_type,
        DataType::Float64
    );
    query.into_named_plan()?;
    let query = spec::QueryPlan {
        plan_id: None,
        node: spec::QueryNode::WithParameters {
            input: Box::new(input),
            positional_arguments: vec![],
            named_arguments: vec![(
                "value".into(),
                spec::Expr::UnresolvedAttribute {
                    name: spec::ObjectName::bare("value"),
                    plan_id: None,
                    is_metadata_column: false,
                },
            )],
        },
    };
    assert!(analyze_request(query).await.is_err());
    Ok(())
}

#[tokio::test]
async fn native_row_identities_are_materialized_in_their_row_scope() -> PlanResult<()> {
    for sql in [
        "SELECT monotonically_increasing_id(), spark_partition_id() FROM range(3)",
        "SELECT monotonically_increasing_id() + monotonically_increasing_id() FROM range(3)",
        "SELECT sum(monotonically_increasing_id()) FROM range(3)",
        "SELECT spark_partition_id(), count(*) FROM range(3) GROUP BY spark_partition_id()",
        "SELECT id FROM range(3) WHERE spark_partition_id() = 0",
        "SELECT id FROM range(3) ORDER BY monotonically_increasing_id()",
        "SELECT row_number() OVER (PARTITION BY spark_partition_id() ORDER BY id) FROM range(3)",
    ] {
        let query = analyze(sql).await?;
        let plan = query.into_named_plan()?.plan;
        assert!(
            plan.display_indent().to_string().contains("Id"),
            "{sql}: {plan}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn native_scalar_iterators_materialize_nested_calls_and_join_predicates() -> PlanResult<()> {
    use datafusion_expr::ScalarUDF;
    use sail_python_udf::config::PySparkUdfConfig;
    use sail_python_udf::udf::pyspark_udf::{PySparkUDF, PySparkUdfKind};
    let session = session();
    session.register_udf(ScalarUDF::new_from_impl(PySparkUDF::new(
        PySparkUdfKind::ScalarPandasIter,
        "identity_iter".into(),
        vec![],
        true,
        vec![DataType::Int64],
        DataType::Int64,
        Arc::new(PySparkUdfConfig::default()),
    )));
    for sql in [
        "SELECT identity_iter(id) FROM range(3)",
        "SELECT identity_iter(identity_iter(id)) FROM range(3)",
        "SELECT identity_iter(id), identity_iter(id) FROM range(3)",
        "SELECT id FROM range(3) WHERE identity_iter(id) > 0",
        "SELECT id FROM range(3) ORDER BY identity_iter(id)",
        "SELECT a.id FROM range(3) a JOIN range(3) b ON identity_iter(a.id) = b.id",
        "SELECT a.id FROM range(3) a LEFT JOIN range(3) b ON identity_iter(b.id) = a.id",
        "SELECT a.id FROM range(3) a JOIN range(3) b ON identity_iter(a.id + b.id) > 0",
        "SELECT sum(identity_iter(id)) FROM range(3)",
    ] {
        let spec::Plan::Query(request) = sail_sql_analyzer::statement::from_ast_statement(
            sail_sql_analyzer::parser::parse_one_statement(sql)?,
        )?
        else {
            return Err(PlanError::internal("expected query"));
        };
        let query = AnalysisPipeline::new(ResolveQuery)
            .analyze(
                request.into(),
                &mut AnalysisContext {
                    session: &session,
                    config: Arc::new(PlanConfig::new()?),
                    ids: IdGenerator::default(),
                },
            )
            .await?;
        let plan = query.into_named_plan()?.plan;
        assert!(
            plan.display_indent().to_string().contains("MapPartitions"),
            "{sql}: {plan}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn native_commands_keep_inputs_native_until_lowering() -> PlanResult<()> {
    use crate::analysis::command::{AnalyzedPlan, LogicalCommand};
    let session = session();
    for sql in [
        "CREATE VIEW v AS SELECT abs('-2') AS x",
        "CREATE TABLE t (x BIGINT, y STRING) USING parquet LOCATION '/tmp/sail-native-analysis-only'",
        "INSERT OVERWRITE DIRECTORY '/tmp/sail-native-analysis-only' USING parquet SELECT abs('-2') AS x",
        "SET TIMEZONE = 'UTC'",
        "RESET TIMEZONE",
    ] {
        let request = sail_sql_analyzer::statement::from_ast_statement(
            sail_sql_analyzer::parser::parse_one_statement(sql)?,
        )?;
        let mut context = AnalysisContext {
            session: &session,
            config: Arc::new(PlanConfig::new()?),
            ids: IdGenerator::default(),
        };
        let plan = super::analyze_plan(request, &mut context).await?;
        match plan {
            AnalyzedPlan::Command(LogicalCommand::Write(write)) => {
                LogicalQuery { plan: write.input }.into_named_plan()?;
            }
            AnalyzedPlan::Command(_) => {}
            _ => return Err(PlanError::internal("expected native command")),
        }
    }
    let spec::Plan::Query(input) = sail_sql_analyzer::statement::from_ast_statement(
        sail_sql_analyzer::parser::parse_one_statement("SELECT 1 AS value")?,
    )?
    else {
        return Err(PlanError::internal("expected query"));
    };
    let request = spec::Plan::Command(spec::CommandPlan::new(spec::CommandNode::ShowString(
        spec::ShowString {
            input: Box::new(input),
            num_rows: 10,
            truncate: 20,
            vertical: false,
        },
    )));
    let mut context = AnalysisContext {
        session: &session,
        config: Arc::new(PlanConfig::new()?),
        ids: IdGenerator::default(),
    };
    let plan = super::analyze_plan(request, &mut context)
        .await?
        .lower(&session)
        .await?;
    assert!(plan.fields.is_none());
    Ok(())
}

#[tokio::test]
async fn native_write_columns_resolve_defaults_generation_and_identity() -> PlanResult<()> {
    use sail_common_datafusion::catalog::{CatalogTableColumnIdentity, TableColumnStatus};

    use super::write::{TableInfo, WriteColumnMatch};
    let session = session();
    let mut context = AnalysisContext {
        session: &session,
        config: Arc::new(PlanConfig::new()?),
        ids: IdGenerator::default(),
    };
    let mut resolver = super::Resolver::new(&mut context);
    let column = |name: &str| TableColumnStatus {
        name: name.into(),
        data_type: DataType::Int64,
        nullable: true,
        comment: None,
        default: None,
        generated_always_as: None,
        identity: None,
        is_partition: false,
        is_bucket: false,
        is_cluster: false,
    };
    let mut generated = column("computed");
    generated.generated_always_as = Some("value + 1".into());
    let mut default = column("fallback");
    default.default = Some("7".into());
    let mut identity = column("identity");
    identity.identity = Some(CatalogTableColumnIdentity {
        start: 100,
        step: 2,
        allow_explicit_insert: false,
        high_water_mark: None,
    });
    let info = TableInfo {
        lakehouse_table: None,
        columns: vec![column("value"), generated, default, identity],
        default_values: Default::default(),
        location: None,
        format: "delta".into(),
        partition_by: vec![],
        sort_by: vec![],
        bucket_by: None,
        properties: vec![],
    };
    for sql in ["SELECT 2 AS value", "SELECT 2 AS value, 3 AS computed"] {
        let spec::Plan::Query(request) = sail_sql_analyzer::statement::from_ast_statement(
            sail_sql_analyzer::parser::parse_one_statement(sql)?,
        )?
        else {
            return Err(PlanError::internal("expected query"));
        };
        let input = resolver.query(request).await?;
        let plan = resolver
            .rewrite_write_input(input, WriteColumnMatch::ByName, &info)
            .await?;
        let schema = plan.schema.resolved("write output")?;
        assert_eq!(schema.attributes.len(), 4);
        assert!(schema.attributes.iter().all(|a| {
            a.result_type
                .resolved("write type")
                .is_ok_and(|t| t.data_type == DataType::Int64)
        }));
        LogicalQuery { plan }.into_named_plan()?;
    }
    Ok(())
}

#[tokio::test]
async fn native_insert_defaults_are_bound_before_values_type_inference_finishes() -> PlanResult<()>
{
    use sail_common_datafusion::catalog::TableColumnStatus;

    use super::write::{TableInfo, WriteColumnMatch};
    let session = session();
    let mut context = AnalysisContext {
        session: &session,
        config: Arc::new(PlanConfig::new()?),
        ids: IdGenerator::default(),
    };
    let mut resolver = super::Resolver::new(&mut context);
    let info = TableInfo {
        lakehouse_table: None,
        columns: vec![TableColumnStatus {
            name: "value".into(),
            data_type: DataType::Int64,
            nullable: true,
            comment: None,
            default: Some("5000000000".into()),
            generated_always_as: None,
            identity: None,
            is_partition: false,
            is_bucket: false,
            is_cluster: false,
        }],
        default_values: Default::default(),
        location: None,
        format: "parquet".into(),
        partition_by: vec![],
        sort_by: vec![],
        bucket_by: None,
        properties: vec![],
    };
    for sql in [
        "VALUES (DEFAULT), (123)",
        "SELECT * FROM VALUES (DEFAULT), (123) AS t(value)",
    ] {
        let spec::Plan::Query(request) = sail_sql_analyzer::statement::from_ast_statement(
            sail_sql_analyzer::parser::parse_one_statement(sql)?,
        )?
        else {
            return Err(PlanError::internal("expected query"));
        };
        let input = resolver.query(request).await?;
        assert!(super::Resolver::plan_has_default_column_value(&input)?);
        let plan = resolver
            .rewrite_write_input(input, WriteColumnMatch::ByPosition, &info)
            .await?;
        assert!(!super::Resolver::plan_has_default_column_value(&plan)?);
        LogicalQuery { plan }.into_named_plan()?;
    }
    Ok(())
}

#[tokio::test]
async fn deferred_native_fields_are_inferred_without_changing_identities() -> PlanResult<()> {
    use sail_query::rewrite::{QueryRewriter, walk_expression, walk_plan};

    use crate::analysis::ir::Infer;
    struct Defer;
    impl QueryRewriter for Defer {
        type Error = PlanError;
        fn rewrite_attribute(
            &mut self,
            mut attribute: crate::analysis::ir::Attribute,
        ) -> PlanResult<crate::analysis::ir::Attribute> {
            attribute.result_type = Deferred::unresolved(Infer);
            Ok(attribute)
        }
        fn rewrite_expression(
            &mut self,
            expression: crate::analysis::ir::LogicalExpression,
        ) -> PlanResult<crate::analysis::ir::LogicalExpression> {
            let mut expression = walk_expression(self, expression)?;
            expression.result_type = Deferred::unresolved(Infer);
            Ok(expression)
        }
        fn rewrite_plan(
            &mut self,
            plan: crate::analysis::ir::LogicalPlan,
        ) -> PlanResult<crate::analysis::ir::LogicalPlan> {
            walk_plan(self, plan)
        }
    }
    for sql in [
        "SELECT id + 1 AS x, abs(id) AS y FROM range(2)",
        "SELECT transform(array(1, 2), x -> x + id) AS a FROM range(2)",
        "SELECT aggregate(array(1, 2), 0, (acc, x) -> acc + x, acc -> acc * 2) AS a",
        "SELECT a.id, EXISTS(SELECT * FROM range(1) b WHERE a.id = b.id) AS found FROM range(2) a",
    ] {
        let original = analyze(sql).await?;
        let ids = original
            .plan
            .schema
            .resolved("original")?
            .attributes
            .iter()
            .map(|a| a.id)
            .collect::<Vec<_>>();
        let plan = Defer.rewrite_plan(original.plan)?;
        let session = session();
        let query = AnalysisPipeline::new(ResolveQuery)
            .analyze(
                LogicalQuery { plan },
                &mut AnalysisContext {
                    session: &session,
                    config: Arc::new(PlanConfig::new()?),
                    ids: IdGenerator::default(),
                },
            )
            .await?;
        assert_eq!(
            query
                .plan
                .schema
                .resolved("resolved")?
                .attributes
                .iter()
                .map(|a| a.id)
                .collect::<Vec<_>>(),
            ids
        );
        query.into_named_plan()?;
    }
    Ok(())
}

#[tokio::test]
async fn grouped_subquery_outputs_lower_without_backend_resolution() -> PlanResult<()> {
    for sql in [
        "SELECT CAST(v AS BIGINT) + (SELECT MIN(CAST(x AS BIGINT)) FROM VALUES (10), (20) AS s(x)) AS g, COUNT(*) AS n FROM VALUES (1), (2), (1) AS t(v) GROUP BY g",
        "SELECT candidate.id, COUNT(*) AS row_count, EXISTS(SELECT * FROM VALUES (1) AS lookup(id) WHERE lookup.id = candidate.id) AS present FROM VALUES (1), (1), (2) AS candidate(id) GROUP BY candidate.id ORDER BY candidate.id DESC LIMIT 1",
    ] {
        let query = analyze(sql).await?;
        query.into_named_plan()?;
    }
    Ok(())
}

#[tokio::test]
async fn additional_layers_receive_resolved_ir_and_cannot_bypass_lowering() -> PlanResult<()> {
    struct Observe {
        name: &'static str,
        visits: Arc<std::sync::Mutex<Vec<&'static str>>>,
        invalidate: bool,
    }
    #[async_trait::async_trait]
    impl crate::analysis::AnalysisLayer for Observe {
        fn name(&self) -> &str {
            self.name
        }
        async fn transform(
            &self,
            mut query: LogicalQuery,
            _: &mut AnalysisContext<'_>,
        ) -> PlanResult<LogicalQuery> {
            let schema = query.plan.schema.resolved("layer input")?;
            assert_eq!(
                schema.attributes[0]
                    .result_type
                    .resolved("layer input field")?
                    .data_type,
                DataType::Int64
            );
            self.visits
                .lock()
                .map_err(|_| PlanError::internal("poisoned test state"))?
                .push(self.name);
            if self.invalidate {
                query.plan.schema = Deferred::unresolved(crate::analysis::ir::Infer);
            }
            Ok(query)
        }
    }
    let visits = Arc::new(std::sync::Mutex::new(vec![]));
    let session = session();
    let request = spec::QueryPlan::new(spec::QueryNode::Range(spec::Range {
        start: Some(0),
        end: 1,
        step: 1,
        num_partitions: Some(1),
    }));
    let query = AnalysisPipeline::new(ResolveQuery)
        .with_layer(Observe {
            name: "first",
            visits: Arc::clone(&visits),
            invalidate: false,
        })
        .with_layer(Observe {
            name: "second",
            visits: Arc::clone(&visits),
            invalidate: true,
        })
        .analyze(
            request.into(),
            &mut AnalysisContext {
                session: &session,
                config: Arc::new(PlanConfig::new()?),
                ids: IdGenerator::default(),
            },
        )
        .await?;
    assert_eq!(
        *visits
            .lock()
            .map_err(|_| PlanError::internal("poisoned test state"))?,
        ["first", "second"]
    );
    assert!(query.into_named_plan().is_err());
    Ok(())
}

#[tokio::test]
async fn collection_and_string_kernels_receive_normalized_arguments() -> PlanResult<()> {
    for sql in [
        "SELECT concat(1, 'x', TIMESTAMP '2024-01-02 03:04:05')",
        "SELECT concat(array(), array(1, 2))",
        "SELECT levenshtein('kitten', 'sitting', CAST(2 AS BIGINT))",
        "SELECT posexplode_outer(explode(array(array(1, 2), NULL)))",
    ] {
        analyze(sql).await?.into_named_plan()?;
    }
    Ok(())
}

#[tokio::test]
async fn resolving_native_literals_preserves_semantic_metadata() -> PlanResult<()> {
    use crate::analysis::ir::*;
    let mut query = analyze("SELECT 1").await?;
    let Deferred::Resolved(PlanNode::Project { expressions, .. }) = &mut query.plan.node else {
        return Err(PlanError::internal("expected projection"));
    };
    let mut field = expressions[0]
        .expression
        .result_type
        .resolved("literal field")?
        .clone();
    field
        .metadata
        .insert("source".into(), "native-layer".into());
    expressions[0].expression.result_type = Deferred::Resolved(field);
    let session = session();
    let query = AnalysisPipeline::new(ResolveQuery)
        .analyze(
            query,
            &mut AnalysisContext {
                session: &session,
                config: Arc::new(PlanConfig::new()?),
                ids: IdGenerator::default(),
            },
        )
        .await?;
    assert_eq!(
        query.plan.schema.resolved("output")?.attributes[0]
            .result_type
            .resolved("field")?
            .metadata
            .get("source")
            .map(String::as_str),
        Some("native-layer")
    );
    query.into_named_plan()?;
    Ok(())
}
