use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, DictionaryArray, RecordBatch};
use datafusion::arrow::datatypes::{Field, Int64Type, Int8Type, Schema};
use datafusion::arrow::{array::StringDictionaryBuilder, datatypes::DataType};
use datafusion::assert_batches_eq;
use datafusion::common::ScalarValue;
use datafusion::config::ConfigOptions;
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs};
use datafusion::prelude::SessionContext;
use datafusion_functions_json::udfs::json_get_str_udf;
use utils::{create_context, display_val, logical_plan, run_query, run_query_params};

use crate::utils::{for_all_json_datatypes, run_query_datatype};

mod utils;

#[tokio::test]
async fn test_json_contains() {
    let expected = [
        "+------------------+-------------------------------------------+",
        "| name             | json_contains(test.json_data,Utf8(\"foo\")) |",
        "+------------------+-------------------------------------------+",
        "| object_foo       | true                                      |",
        "| object_foo_array | true                                      |",
        "| object_foo_obj   | true                                      |",
        "| object_foo_null  | true                                      |",
        "| object_bar       | false                                     |",
        "| list_foo         | false                                     |",
        "| invalid_json     | false                                     |",
        "+------------------+-------------------------------------------+",
    ];

    for_all_json_datatypes(async |dt| {
        let batches = run_query_datatype("select name, json_contains(json_data, 'foo') from test", dt)
            .await
            .unwrap();
        assert_batches_eq!(expected, &batches);
    })
    .await;
}

#[tokio::test]
async fn test_json_contains_array() {
    let sql = "select json_contains('[1, 2, 3]', 2)";
    let batches = run_query(sql).await.unwrap();
    assert_eq!(display_val(batches).await, (DataType::Boolean, "true".to_string()));

    let sql = "select json_contains('[1, 2, 3]', 3)";
    let batches = run_query(sql).await.unwrap();
    assert_eq!(display_val(batches).await, (DataType::Boolean, "false".to_string()));
}

#[tokio::test]
async fn test_json_contains_nested() {
    let sql = r#"select json_contains('[1, 2, {"foo": null}]', 2)"#;
    let batches = run_query(sql).await.unwrap();
    assert_eq!(display_val(batches).await, (DataType::Boolean, "true".to_string()));

    let sql = r#"select json_contains('[1, 2, {"foo": null}]', 2, 'foo')"#;
    let batches = run_query(sql).await.unwrap();
    assert_eq!(display_val(batches).await, (DataType::Boolean, "true".to_string()));

    let sql = r#"select json_contains('[1, 2, {"foo": null}]', 2, 'bar')"#;
    let batches = run_query(sql).await.unwrap();
    assert_eq!(display_val(batches).await, (DataType::Boolean, "false".to_string()));
}

#[tokio::test]
async fn test_json_get_union() {
    let batches = run_query("select name, json_get(json_data, 'foo') from test")
        .await
        .unwrap();

    let expected = [
        "+------------------+--------------------------------------+",
        "| name             | json_get(test.json_data,Utf8(\"foo\")) |",
        "+------------------+--------------------------------------+",
        "| object_foo       | {str=abc}                            |",
        "| object_foo_array | {array=[1]}                          |",
        "| object_foo_obj   | {object={}}                          |",
        "| object_foo_null  | {null=}                              |",
        "| object_bar       | {null=}                              |",
        "| list_foo         | {null=}                              |",
        "| invalid_json     | {null=}                              |",
        "+------------------+--------------------------------------+",
    ];
    assert_batches_eq!(expected, &batches);
}

#[tokio::test]
async fn test_json_get_array_elem() {
    let sql = "select json_get('[1, 2, 3]', 2)";
    let batches = run_query(sql).await.unwrap();
    let (value_type, value_repr) = display_val(batches).await;
    assert!(matches!(value_type, DataType::Union(_, _)));
    assert_eq!(value_repr, "{int=3}");
}

#[tokio::test]
async fn test_json_get_array_basic_numbers() {
    let sql = "select json_get_array('[1, 2, 3]')";
    let batches = run_query(sql).await.unwrap();
    let (value_type, value_repr) = display_val(batches).await;
    assert!(matches!(value_type, DataType::List(_)));
    assert_eq!(value_repr, "[1, 2, 3]");
}

#[tokio::test]
async fn test_json_get_array_mixed_types() {
    let sql = r#"select json_get_array('["hello", 42, true, null, 3.14]')"#;
    let batches = run_query(sql).await.unwrap();
    let (value_type, value_repr) = display_val(batches).await;
    assert!(matches!(value_type, DataType::List(_)));
    assert_eq!(value_repr, r#"["hello", 42, true, null, 3.14]"#);
}

#[tokio::test]
async fn test_json_get_array_nested_objects() {
    let sql = r#"select json_get_array('[{"name": "John"}, {"age": 30}]')"#;
    let batches = run_query(sql).await.unwrap();
    let (value_type, value_repr) = display_val(batches).await;
    assert!(matches!(value_type, DataType::List(_)));
    assert_eq!(value_repr, r#"[{"name": "John"}, {"age": 30}]"#);
}

#[tokio::test]
async fn test_json_get_array_nested_arrays() {
    let sql = r"select json_get_array('[[1, 2], [3, 4]]')";
    let batches = run_query(sql).await.unwrap();
    let (value_type, value_repr) = display_val(batches).await;
    assert!(matches!(value_type, DataType::List(_)));
    assert_eq!(value_repr, "[[1, 2], [3, 4]]");
}

#[tokio::test]
async fn test_json_get_array_empty() {
    let sql = "select json_get_array('[]')";
    let batches = run_query(sql).await.unwrap();
    let (value_type, value_repr) = display_val(batches).await;
    assert!(matches!(value_type, DataType::List(_)));
    assert_eq!(value_repr, "[]");
}

#[tokio::test]
async fn test_json_get_array_invalid_json() {
    let sql = "select json_get_array('')";
    let batches = run_query(sql).await.unwrap();
    let (value_type, value_repr) = display_val(batches).await;
    assert!(matches!(value_type, DataType::List(_)));
    assert_eq!(value_repr, "");
}

#[tokio::test]
async fn test_json_get_array_with_path() {
    let sql = r#"select json_get_array('{"items": [1, 2, 3]}', 'items')"#;
    let batches = run_query(sql).await.unwrap();
    let (value_type, value_repr) = display_val(batches).await;
    assert!(matches!(value_type, DataType::List(_)));
    assert_eq!(value_repr, "[1, 2, 3]");
}

#[tokio::test]
async fn test_json_get_equals() {
    let e = run_query(r"select name, json_get(json_data, 'foo')='abc' from test")
        .await
        .unwrap_err();

    // see https://github.com/apache/datafusion/issues/10180
    assert!(e
        .to_string()
        .starts_with("Error during planning: Cannot infer common argument type for comparison operation Union"));
}

#[tokio::test]
async fn test_json_get_cast_equals() {
    let batches = run_query(r"select name, json_get(json_data, 'foo')::string='abc' from test")
        .await
        .unwrap();

    let expected = [
        "+------------------+----------------------------------------------------+",
        "| name             | json_get(test.json_data,Utf8(\"foo\")) = Utf8(\"abc\") |",
        "+------------------+----------------------------------------------------+",
        "| object_foo       | true                                               |",
        "| object_foo_array |                                                    |",
        "| object_foo_obj   |                                                    |",
        "| object_foo_null  |                                                    |",
        "| object_bar       |                                                    |",
        "| list_foo         |                                                    |",
        "| invalid_json     |                                                    |",
        "+------------------+----------------------------------------------------+",
    ];
    assert_batches_eq!(expected, &batches);
}

#[tokio::test]
async fn test_json_get_str() {
    let batches = run_query("select name, json_get_str(json_data, 'foo') from test")
        .await
        .unwrap();

    let expected = [
        "+------------------+------------------------------------------+",
        "| name             | json_get_str(test.json_data,Utf8(\"foo\")) |",
        "+------------------+------------------------------------------+",
        "| object_foo       | abc                                      |",
        "| object_foo_array |                                          |",
        "| object_foo_obj   |                                          |",
        "| object_foo_null  |                                          |",
        "| object_bar       |                                          |",
        "| list_foo         |                                          |",
        "| invalid_json     |                                          |",
        "+------------------+------------------------------------------+",
    ];
    assert_batches_eq!(expected, &batches);
}

#[tokio::test]
async fn test_json_get_str_equals() {
    let sql = "select name, json_get_str(json_data, 'foo')='abc' from test";
    let batches = run_query(sql).await.unwrap();

    let expected = [
        "+------------------+--------------------------------------------------------+",
        "| name             | json_get_str(test.json_data,Utf8(\"foo\")) = Utf8(\"abc\") |",
        "+------------------+--------------------------------------------------------+",
        "| object_foo       | true                                                   |",
        "| object_foo_array |                                                        |",
        "| object_foo_obj   |                                                        |",
        "| object_foo_null  |                                                        |",
        "| object_bar       |                                                        |",
        "| list_foo         |                                                        |",
        "| invalid_json     |                                                        |",
        "+------------------+--------------------------------------------------------+",
    ];
    assert_batches_eq!(expected, &batches);
}

#[tokio::test]
async fn test_json_get_str_int() {
    let sql = r#"select json_get_str('["a", "b", "c"]', 1)"#;
    let batches = run_query(sql).await.unwrap();
    assert_eq!(display_val(batches).await, (DataType::Utf8, "b".to_string()));

    let sql = r#"select json_get_str('["a", "b", "c"]', 3)"#;
    let batches = run_query(sql).await.unwrap();
    assert_eq!(display_val(batches).await, (DataType::Utf8, String::new()));
}

#[tokio::test]
async fn test_json_get_str_path() {
    let sql = r#"select json_get_str('{"a": {"aa": "x", "ab: "y"}, "b": []}', 'a', 'aa')"#;
    let batches = run_query(sql).await.unwrap();
    assert_eq!(display_val(batches).await, (DataType::Utf8, "x".to_string()));
}

#[tokio::test]
async fn test_json_get_str_null() {
    let e = run_query(r"select json_get_str('{}', null)").await.unwrap_err();

    assert_eq!(
        e.to_string(),
        "Error during planning: Unexpected argument type to 'json_get_str' at position 2, expected string or int, got Null."
    );
}

#[tokio::test]
async fn test_json_get_no_path() {
    let batches = run_query(r#"select json_get('"foo"')::string"#).await.unwrap();
    assert_eq!(display_val(batches).await, (DataType::Utf8, "foo".to_string()));

    let batches = run_query(r"select json_get('123')::int").await.unwrap();
    assert_eq!(display_val(batches).await, (DataType::Int64, "123".to_string()));

    let batches = run_query(r"select json_get('true')::int").await.unwrap();
    assert_eq!(display_val(batches).await, (DataType::Int64, String::new()));
}

#[tokio::test]
async fn test_json_get_int() {
    let batches = run_query(r"select json_get_int('[1, 2, 3]', 1)").await.unwrap();
    assert_eq!(display_val(batches).await, (DataType::Int64, "2".to_string()));
}

#[tokio::test]
async fn test_json_get_path() {
    let batches = run_query(r#"select json_get('{"i": 19}', 'i')::int<20"#).await.unwrap();
    assert_eq!(display_val(batches).await, (DataType::Boolean, "true".to_string()));
}

#[tokio::test]
async fn test_json_get_cast_int() {
    let sql = r#"select json_get('{"foo": 42}', 'foo')::int"#;
    let batches = run_query(sql).await.unwrap();
    assert_eq!(display_val(batches).await, (DataType::Int64, "42".to_string()));

    // floats not allowed
    let sql = r#"select json_get('{"foo": 4.2}', 'foo')::int"#;
    let batches = run_query(sql).await.unwrap();
    assert_eq!(display_val(batches).await, (DataType::Int64, String::new()));
}

#[tokio::test]
async fn test_json_get_cast_int_path() {
    let sql = r#"select json_get('{"foo": [null, {"x": false, "bar": 73}}', 'foo', 1, 'bar')::int"#;
    let batches = run_query(sql).await.unwrap();
    assert_eq!(display_val(batches).await, (DataType::Int64, "73".to_string()));
}

#[tokio::test]
async fn test_json_get_int_lookup() {
    let sql = "select str_key, json_data from other where json_get_int(json_data, str_key) is not null";
    let batches = run_query(sql).await.unwrap();
    let expected = [
        "+---------+---------------+",
        "| str_key | json_data     |",
        "+---------+---------------+",
        "| foo     |  {\"foo\": 42}  |",
        "+---------+---------------+",
    ];
    assert_batches_eq!(expected, &batches);

    // lookup by int
    let sql = "select int_key, json_data from other where json_get_int(json_data, int_key) is not null";
    let batches = run_query(sql).await.unwrap();
    let expected = [
        "+---------+-----------+",
        "| int_key | json_data |",
        "+---------+-----------+",
        "| 0       |  [42]     |",
        "+---------+-----------+",
    ];
    assert_batches_eq!(expected, &batches);
}

#[tokio::test]
async fn test_json_get_float() {
    let batches = run_query("select json_get_float('[1.5]', 0)").await.unwrap();
    assert_eq!(display_val(batches).await, (DataType::Float64, "1.5".to_string()));

    // coerce int to float
    let batches = run_query("select json_get_float('[1]', 0)").await.unwrap();
    assert_eq!(display_val(batches).await, (DataType::Float64, "1.0".to_string()));
}

#[tokio::test]
async fn test_json_get_cast_float() {
    let sql = r#"select json_get('{"foo": 4.2e2}', 'foo')::float"#;
    let batches = run_query(sql).await.unwrap();
    assert_eq!(display_val(batches).await, (DataType::Float64, "420.0".to_string()));
}

#[tokio::test]
async fn test_json_get_cast_numeric() {
    let sql = r#"select json_get('{"foo": 4.2e2}', 'foo')::numeric"#;
    let batches = run_query(sql).await.unwrap();
    assert_eq!(display_val(batches).await, (DataType::Float64, "420.0".to_string()));
}

#[tokio::test]
async fn test_json_get_cast_numeric_equals() {
    let sql = r#"select json_get('{"foo": 420}', 'foo')::numeric = 420"#;
    let batches = run_query(sql).await.unwrap();
    assert_eq!(display_val(batches).await, (DataType::Boolean, "true".to_string()));
}

#[tokio::test]
async fn test_json_get_bool() {
    let batches = run_query("select json_get_bool('[true]', 0)").await.unwrap();
    assert_eq!(display_val(batches).await, (DataType::Boolean, "true".to_string()));

    let batches = run_query(r#"select json_get_bool('{"": false}', '')"#).await.unwrap();
    assert_eq!(display_val(batches).await, (DataType::Boolean, "false".to_string()));
}

#[tokio::test]
async fn test_json_get_cast_bool() {
    let sql = r#"select json_get('{"foo": true}', 'foo')::bool"#;
    let batches = run_query(sql).await.unwrap();
    assert_eq!(display_val(batches).await, (DataType::Boolean, "true".to_string()));
}

#[tokio::test]
async fn test_json_get_json() {
    let batches = run_query("select name, json_get_json(json_data, 'foo') from test")
        .await
        .unwrap();

    let expected = [
        "+------------------+-------------------------------------------+",
        "| name             | json_get_json(test.json_data,Utf8(\"foo\")) |",
        "+------------------+-------------------------------------------+",
        "| object_foo       | \"abc\"                                     |",
        "| object_foo_array | [1]                                       |",
        "| object_foo_obj   | {}                                        |",
        "| object_foo_null  | null                                      |",
        "| object_bar       |                                           |",
        "| list_foo         |                                           |",
        "| invalid_json     |                                           |",
        "+------------------+-------------------------------------------+",
    ];
    assert_batches_eq!(expected, &batches);
}

#[tokio::test]
async fn test_json_get_json_float() {
    let sql = r#"select json_get_json('{"x": 4.2e-1}', 'x')"#;
    let batches = run_query(sql).await.unwrap();
    assert_eq!(display_val(batches).await, (DataType::Utf8, "4.2e-1".to_string()));
}

#[tokio::test]
async fn test_json_length_array() {
    let sql = "select json_length('[1, 2, 3]')";
    let batches = run_query(sql).await.unwrap();
    assert_eq!(display_val(batches).await, (DataType::UInt64, "3".to_string()));
}

#[tokio::test]
async fn test_json_length_object() {
    let sql = r#"select json_length('{"a": 1, "b": 2, "c": 3}')"#;
    let batches = run_query(sql).await.unwrap();
    assert_eq!(display_val(batches).await, (DataType::UInt64, "3".to_string()));

    let sql = r"select json_length('{}')";
    let batches = run_query(sql).await.unwrap();
    assert_eq!(display_val(batches).await, (DataType::UInt64, "0".to_string()));
}

#[tokio::test]
async fn test_json_length_string() {
    let sql = r#"select json_length('"foobar"')"#;
    let batches = run_query(sql).await.unwrap();
    assert_eq!(display_val(batches).await, (DataType::UInt64, String::new()));
}

#[tokio::test]
async fn test_json_length_object_nested() {
    let sql = r#"select json_length('{"a": 1, "b": 2, "c": [1, 2]}', 'c')"#;
    let batches = run_query(sql).await.unwrap();
    assert_eq!(display_val(batches).await, (DataType::UInt64, "2".to_string()));

    let sql = r#"select json_length('{"a": 1, "b": 2, "c": []}', 'b')"#;
    let batches = run_query(sql).await.unwrap();
    assert_eq!(display_val(batches).await, (DataType::UInt64, String::new()));
}

#[tokio::test]
async fn test_json_contains_large() {
    let expected = [
        "+----------+",
        "| count(*) |",
        "+----------+",
        "| 4        |",
        "+----------+",
    ];

    let batches = run_query_datatype(
        "select count(*) from test where json_contains(json_data, 'foo')",
        &DataType::LargeUtf8,
    )
    .await
    .unwrap();
    assert_batches_eq!(expected, &batches);
}

#[tokio::test]
async fn test_json_contains_large_vec() {
    let expected = [
        "+----------+",
        "| count(*) |",
        "+----------+",
        "| 0        |",
        "+----------+",
    ];

    let batches = run_query_datatype(
        "select count(*) from test where json_contains(json_data, name)",
        &DataType::LargeUtf8,
    )
    .await
    .unwrap();
    assert_batches_eq!(expected, &batches);
}

#[tokio::test]
async fn test_json_contains_large_both() {
    let expected = [
        "+----------+",
        "| count(*) |",
        "+----------+",
        "| 0        |",
        "+----------+",
    ];

    let batches = run_query_datatype(
        "select count(*) from test where json_contains(json_data, json_data)",
        &DataType::LargeUtf8,
    )
    .await
    .unwrap();
    assert_batches_eq!(expected, &batches);
}

#[tokio::test]
async fn test_json_contains_large_params() {
    let expected = [
        "+----------+",
        "| count(*) |",
        "+----------+",
        "| 4        |",
        "+----------+",
    ];

    let sql = "select count(*) from test where json_contains(json_data, 'foo')";
    let params = vec![ScalarValue::LargeUtf8(Some("foo".to_string()))];

    for_all_json_datatypes(async |dt| {
        let batches = run_query_params(sql, dt, params.clone()).await.unwrap();
        assert_batches_eq!(expected, &batches);
    })
    .await;
}

#[tokio::test]
async fn test_json_contains_large_both_params() {
    let expected = [
        "+----------+",
        "| count(*) |",
        "+----------+",
        "| 4        |",
        "+----------+",
    ];

    let sql = "select count(*) from test where json_contains(json_data, 'foo')";
    let params = vec![ScalarValue::LargeUtf8(Some("foo".to_string()))];

    for_all_json_datatypes(async |dt| {
        let batches = run_query_params(sql, dt, params.clone()).await.unwrap();
        assert_batches_eq!(expected, &batches);
    })
    .await;
}

#[tokio::test]
async fn test_json_length_vec() {
    let sql = r"select name, json_len(json_data) as len from test";

    let expected = [
        "+------------------+-----+",
        "| name             | len |",
        "+------------------+-----+",
        "| object_foo       | 1   |",
        "| object_foo_array | 1   |",
        "| object_foo_obj   | 1   |",
        "| object_foo_null  | 1   |",
        "| object_bar       | 1   |",
        "| list_foo         | 1   |",
        "| invalid_json     |     |",
        "+------------------+-----+",
    ];

    for_all_json_datatypes(async |dt| {
        let batches = run_query_datatype(sql, dt).await.unwrap();
        assert_batches_eq!(expected, &batches);
    })
    .await;
}

#[tokio::test]
async fn test_no_args() {
    let err = run_query(r"select json_len()").await.unwrap_err();
    assert!(err
        .to_string()
        .contains("No function matches the given name and argument types 'json_length()'."));
}

#[test]
fn test_json_get_utf8() {
    let json_get_str = json_get_str_udf();
    let args = &[
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(
            r#"{"a": {"aa": "x", "ab: "y"}, "b": []}"#.to_string(),
        ))),
        ColumnarValue::Scalar(ScalarValue::Utf8(Some("a".to_string()))),
        ColumnarValue::Scalar(ScalarValue::Utf8(Some("aa".to_string()))),
    ];

    let ColumnarValue::Scalar(sv) = json_get_str
        .invoke_with_args(ScalarFunctionArgs {
            args: args.to_vec(),
            arg_fields: vec![
                Arc::new(Field::new("arg_1", DataType::LargeUtf8, false)),
                Arc::new(Field::new("arg_2", DataType::LargeUtf8, false)),
                Arc::new(Field::new("arg_3", DataType::LargeUtf8, false)),
            ],
            number_rows: 1,
            return_field: Arc::new(
                Field::new("ret_field", DataType::Utf8, false)
                    .with_metadata(HashMap::from_iter(vec![("is_json".to_string(), "true".to_string())])),
            ),
            config_options: Arc::new(ConfigOptions::default()),
        })
        .unwrap()
    else {
        panic!("expected scalar")
    };

    assert_eq!(sv, ScalarValue::Utf8(Some("x".to_string())));
}

#[test]
fn test_json_get_large_utf8() {
    let json_get_str = json_get_str_udf();
    let args = &[
        ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(
            r#"{"a": {"aa": "x", "ab: "y"}, "b": []}"#.to_string(),
        ))),
        ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some("a".to_string()))),
        ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some("aa".to_string()))),
    ];

    let ColumnarValue::Scalar(sv) = json_get_str
        .invoke_with_args(ScalarFunctionArgs {
            args: args.to_vec(),
            arg_fields: vec![
                Arc::new(Field::new("arg_1", DataType::LargeUtf8, false)),
                Arc::new(Field::new("arg_2", DataType::LargeUtf8, false)),
                Arc::new(Field::new("arg_3", DataType::LargeUtf8, false)),
            ],
            number_rows: 1,
            return_field: Arc::new(
                Field::new("ret_field", DataType::Utf8, false)
                    .with_metadata(HashMap::from_iter(vec![("is_json".to_string(), "true".to_string())])),
            ),
            config_options: Arc::new(ConfigOptions::default()),
        })
        .unwrap()
    else {
        panic!("expected scalar")
    };

    assert_eq!(sv, ScalarValue::Utf8(Some("x".to_string())));
}

#[tokio::test]
async fn test_json_get_union_scalar() {
    let sql = r#"select json_get(json_get('{"x": {"y": 1}}', 'x'), 'y') as v"#;
    let expected = [
        "+---------+",
        "| v       |",
        "+---------+",
        "| {int=1} |",
        "+---------+",
    ];

    for_all_json_datatypes(async |dt| {
        let batches = run_query_datatype(sql, dt).await.unwrap();
        assert_batches_eq!(expected, &batches);
    })
    .await;
}

#[tokio::test]
async fn test_json_get_nested_collapsed() {
    let sql = "select name, json_get(json_get(json_data, 'foo'), 0) as v from test";
    let expected = [
        "+------------------+---------+",
        "| name             | v       |",
        "+------------------+---------+",
        "| object_foo       | {null=} |",
        "| object_foo_array | {int=1} |",
        "| object_foo_obj   | {null=} |",
        "| object_foo_null  | {null=} |",
        "| object_bar       | {null=} |",
        "| list_foo         | {null=} |",
        "| invalid_json     | {null=} |",
        "+------------------+---------+",
    ];

    let expected_dict = [
        "+------------------+---------+",
        "| name             | v       |",
        "+------------------+---------+",
        "| object_foo       |         |",
        "| object_foo_array | {int=1} |",
        "| object_foo_obj   |         |",
        "| object_foo_null  |         |",
        "| object_bar       |         |",
        "| list_foo         |         |",
        "| invalid_json     |         |",
        "+------------------+---------+",
    ];

    for_all_json_datatypes(async |dt| {
        let batches = run_query_datatype(sql, dt).await.unwrap();

        if matches!(dt, DataType::Dictionary(_, _)) {
            assert_batches_eq!(expected_dict, &batches);
        } else {
            assert_batches_eq!(expected, &batches);
        }
    })
    .await;
}

#[tokio::test]
async fn test_json_get_cte() {
    // avoid auto-un-nesting with a CTE
    let sql = r"
        with t as (select name, json_get(json_data, 'foo') j from test)
        select name, json_get(j, 0) v from t
    ";
    let expected = [
        "+------------------+---------+",
        "| name             | v       |",
        "+------------------+---------+",
        "| object_foo       | {null=} |",
        "| object_foo_array | {int=1} |",
        "| object_foo_obj   | {null=} |",
        "| object_foo_null  | {null=} |",
        "| object_bar       | {null=} |",
        "| list_foo         | {null=} |",
        "| invalid_json     | {null=} |",
        "+------------------+---------+",
    ];

    let expected_dict = [
        "+------------------+---------+",
        "| name             | v       |",
        "+------------------+---------+",
        "| object_foo       |         |",
        "| object_foo_array | {int=1} |",
        "| object_foo_obj   |         |",
        "| object_foo_null  |         |",
        "| object_bar       |         |",
        "| list_foo         |         |",
        "| invalid_json     |         |",
        "+------------------+---------+",
    ];

    for_all_json_datatypes(async |dt| {
        let batches = run_query_datatype(sql, dt).await.unwrap();

        if matches!(dt, DataType::Dictionary(_, _)) {
            assert_batches_eq!(expected_dict, &batches);
        } else {
            assert_batches_eq!(expected, &batches);
        }
    })
    .await;
}

#[tokio::test]
async fn test_plan_json_get_cte() {
    // avoid auto-unnesting with a CTE
    let sql = r"
        explain
        with t as (select name, json_get(json_data, 'foo') j from test)
        select name, json_get(j, 0) v from t
    ";
    let expected = [
        "Projection: t.name, json_get(t.j, Int64(0)) AS v",
        "  SubqueryAlias: t",
        "    Projection: test.name, json_get(test.json_data, Utf8(\"foo\")) AS j",
        "      TableScan: test projection=[name, json_data]",
    ];

    let plan_lines = logical_plan(sql).await;
    assert_eq!(plan_lines, expected);
}

#[tokio::test]
async fn test_json_get_unnest() {
    let sql = "select name, json_get(json_get(json_data, 'foo'), 0) v from test";

    let expected = [
        "+------------------+---------+",
        "| name             | v       |",
        "+------------------+---------+",
        "| object_foo       | {null=} |",
        "| object_foo_array | {int=1} |",
        "| object_foo_obj   | {null=} |",
        "| object_foo_null  | {null=} |",
        "| object_bar       | {null=} |",
        "| list_foo         | {null=} |",
        "| invalid_json     | {null=} |",
        "+------------------+---------+",
    ];

    let expected_dict = [
        "+------------------+---------+",
        "| name             | v       |",
        "+------------------+---------+",
        "| object_foo       |         |",
        "| object_foo_array | {int=1} |",
        "| object_foo_obj   |         |",
        "| object_foo_null  |         |",
        "| object_bar       |         |",
        "| list_foo         |         |",
        "| invalid_json     |         |",
        "+------------------+---------+",
    ];

    for_all_json_datatypes(async |dt| {
        let batches = run_query_datatype(sql, dt).await.unwrap();

        if matches!(dt, DataType::Dictionary(_, _)) {
            assert_batches_eq!(expected_dict, &batches);
        } else {
            assert_batches_eq!(expected, &batches);
        }
    })
    .await;
}

#[tokio::test]
async fn test_plan_json_get_unnest() {
    let sql = "explain select json_get(json_get(json_data, 'foo'), 0) v from test";
    let expected = [
        "Projection: json_get(test.json_data, Utf8(\"foo\"), Int64(0)) AS v",
        "  TableScan: test projection=[json_data]",
    ];

    let plan_lines = logical_plan(sql).await;
    assert_eq!(plan_lines, expected);
}

#[tokio::test]
async fn test_json_get_int_unnest() {
    let sql = "select name, json_get(json_get(json_data, 'foo'), 0)::int v from test";

    let expected = [
        "+------------------+---+",
        "| name             | v |",
        "+------------------+---+",
        "| object_foo       |   |",
        "| object_foo_array | 1 |",
        "| object_foo_obj   |   |",
        "| object_foo_null  |   |",
        "| object_bar       |   |",
        "| list_foo         |   |",
        "| invalid_json     |   |",
        "+------------------+---+",
    ];

    for_all_json_datatypes(async |dt| {
        let batches = run_query_datatype(sql, dt).await.unwrap();
        assert_batches_eq!(expected, &batches);
    })
    .await;
}

#[tokio::test]
async fn test_plan_json_get_int_unnest() {
    let sql = "explain select json_get(json_get(json_data, 'foo'), 0)::int v from test";
    let expected = [
        "Projection: json_get_int(test.json_data, Utf8(\"foo\"), Int64(0)) AS v",
        "  TableScan: test projection=[json_data]",
    ];

    let plan_lines = logical_plan(sql).await;
    assert_eq!(plan_lines, expected);
}

#[tokio::test]
async fn test_multiple_lookup_arrays() {
    let sql = "select json_get(json_data, str_key1, str_key2) v from more_nested";
    let err = run_query(sql).await.unwrap_err();
    assert_eq!(
        err.to_string(),
        "Execution error: More than 1 path element is not supported when querying JSON using an array."
    );
}

#[tokio::test]
async fn test_json_get_union_array_nested() {
    let sql = "select json_get(json_get(json_data, str_key1), str_key2) v from more_nested";
    let expected = [
        "+-------------+",
        "| v           |",
        "+-------------+",
        "| {array=[0]} |",
        "| {null=}     |",
        "| {null=}     |",
        "+-------------+",
    ];

    for_all_json_datatypes(async |dt| {
        let batches = run_query_datatype(sql, dt).await.unwrap();
        assert_batches_eq!(expected, &batches);
    })
    .await;
}

#[tokio::test]
async fn test_plan_json_get_union_array_nested() {
    let sql = "explain select json_get(json_get(json_data, str_key1), str_key2) v from more_nested";
    // json_get is not un-nested because lookup types are not literals
    let expected = [
        "Projection: json_get(json_get(more_nested.json_data, more_nested.str_key1), more_nested.str_key2) AS v",
        "  TableScan: more_nested projection=[json_data, str_key1, str_key2]",
    ];

    let plan_lines = logical_plan(sql).await;
    assert_eq!(plan_lines, expected);
}

#[tokio::test]
async fn test_json_get_union_array_skip_double_nested() {
    let sql =
        "select json_data, json_get_int(json_get(json_get(json_data, str_key1), str_key2), int_key) v from more_nested";
    let expected = [
        "+--------------------------+---+",
        "| json_data                | v |",
        "+--------------------------+---+",
        "|  {\"foo\": {\"bar\": [0]}}   | 0 |",
        "|  {\"foo\": {\"bar\": [1]}}   |   |",
        "|  {\"foo\": {\"bar\": null}}  |   |",
        "+--------------------------+---+",
    ];

    for_all_json_datatypes(async |dt| {
        let batches = run_query_datatype(sql, dt).await.unwrap();
        assert_batches_eq!(expected, &batches);
    })
    .await;
}

#[tokio::test]
async fn test_arrow() {
    let sql = "select name, json_data->'foo' from test";

    let expected = [
        "+------------------+-------------------------+",
        "| name             | test.json_data -> 'foo' |",
        "+------------------+-------------------------+",
        "| object_foo       | {str=abc}               |",
        "| object_foo_array | {array=[1]}             |",
        "| object_foo_obj   | {object={}}             |",
        "| object_foo_null  | {null=}                 |",
        "| object_bar       | {null=}                 |",
        "| list_foo         | {null=}                 |",
        "| invalid_json     | {null=}                 |",
        "+------------------+-------------------------+",
    ];

    let expected_dict = [
        "+------------------+-------------------------+",
        "| name             | test.json_data -> 'foo' |",
        "+------------------+-------------------------+",
        "| object_foo       | {str=abc}               |",
        "| object_foo_array | {array=[1]}             |",
        "| object_foo_obj   | {object={}}             |",
        "| object_foo_null  |                         |",
        "| object_bar       |                         |",
        "| list_foo         |                         |",
        "| invalid_json     |                         |",
        "+------------------+-------------------------+",
    ];

    for_all_json_datatypes(async |dt| {
        let batches = run_query_datatype(sql, dt).await.unwrap();
        if matches!(dt, DataType::Dictionary(_, _)) {
            assert_batches_eq!(expected_dict, &batches);
        } else {
            assert_batches_eq!(expected, &batches);
        }
    })
    .await;
}

#[tokio::test]
async fn test_plan_arrow() {
    let lines = logical_plan(r"explain select json_data->'foo' from test").await;

    let expected = [
        "Projection: json_get(test.json_data, Utf8(\"foo\")) AS test.json_data -> 'foo'",
        "  TableScan: test projection=[json_data]",
    ];

    assert_eq!(lines, expected);
}

#[tokio::test]
async fn test_long_arrow() {
    let sql = "select name, json_data->>'foo' from test";

    let expected = [
        "+------------------+--------------------------+",
        "| name             | test.json_data ->> 'foo' |",
        "+------------------+--------------------------+",
        "| object_foo       | abc                      |",
        "| object_foo_array | [1]                      |",
        "| object_foo_obj   | {}                       |",
        "| object_foo_null  |                          |",
        "| object_bar       |                          |",
        "| list_foo         |                          |",
        "| invalid_json     |                          |",
        "+------------------+--------------------------+",
    ];

    for_all_json_datatypes(async |dt| {
        let batches = run_query_datatype(sql, dt).await.unwrap();
        assert_batches_eq!(expected, &batches);
    })
    .await;
}

#[tokio::test]
async fn test_plan_long_arrow() {
    let lines = logical_plan(r"explain select json_data->>'foo' from test").await;

    let expected = [
        "Projection: json_as_text(test.json_data, Utf8(\"foo\")) AS test.json_data ->> 'foo'",
        "  TableScan: test projection=[json_data]",
    ];

    assert_eq!(lines, expected);
}

#[tokio::test]
async fn test_long_arrow_eq_str() {
    let sql = r"select name, (json_data->>'foo')='abc' from test";

    let expected = [
        "+------------------+----------------------------------------+",
        "| name             | test.json_data ->> 'foo' = Utf8(\"abc\") |",
        "+------------------+----------------------------------------+",
        "| object_foo       | true                                   |",
        "| object_foo_array | false                                  |",
        "| object_foo_obj   | false                                  |",
        "| object_foo_null  |                                        |",
        "| object_bar       |                                        |",
        "| list_foo         |                                        |",
        "| invalid_json     |                                        |",
        "+------------------+----------------------------------------+",
    ];

    for_all_json_datatypes(async |dt| {
        let batches = run_query_datatype(sql, dt).await.unwrap();
        assert_batches_eq!(expected, &batches);
    })
    .await;
}

/// Test column name / alias creation with a cast in the needle / key
#[tokio::test]
async fn test_arrow_cast_key_text() {
    let sql = r#"select ('{"foo": 42}'->>('foo'::text))"#;

    let expected = [
        "+-------------------------+",
        "| '{\"foo\": 42}' ->> 'foo' |",
        "+-------------------------+",
        "| 42                      |",
        "+-------------------------+",
    ];

    for_all_json_datatypes(async |dt| {
        let batches = run_query_datatype(sql, dt).await.unwrap();
        assert_batches_eq!(expected, &batches);
    })
    .await;
}

#[tokio::test]
async fn test_arrow_cast_int() {
    let sql = r#"select ('{"foo": 42}'->'foo')::int"#;

    let expected = [
        "+------------------------+",
        "| '{\"foo\": 42}' -> 'foo' |",
        "+------------------------+",
        "| 42                     |",
        "+------------------------+",
    ];

    for_all_json_datatypes(async |dt| {
        let batches = run_query_datatype(sql, dt).await.unwrap();
        assert_batches_eq!(expected, &batches);
        assert_eq!(display_val(batches).await, (DataType::Int64, "42".to_string()));
    })
    .await;
}

#[tokio::test]
async fn test_plan_arrow_cast_int() {
    let lines = logical_plan(r"explain select (json_data->'foo')::int from test").await;

    let expected = [
        "Projection: json_get_int(test.json_data, Utf8(\"foo\")) AS test.json_data -> 'foo'",
        "  TableScan: test projection=[json_data]",
    ];

    assert_eq!(lines, expected);
}

#[tokio::test]
async fn test_arrow_double_nested() {
    let sql = "select name, json_data->'foo'->0 from test";

    let expected = [
        "+------------------+------------------------------+",
        "| name             | test.json_data -> 'foo' -> 0 |",
        "+------------------+------------------------------+",
        "| object_foo       | {null=}                      |",
        "| object_foo_array | {int=1}                      |",
        "| object_foo_obj   | {null=}                      |",
        "| object_foo_null  | {null=}                      |",
        "| object_bar       | {null=}                      |",
        "| list_foo         | {null=}                      |",
        "| invalid_json     | {null=}                      |",
        "+------------------+------------------------------+",
    ];

    let expected_dict = [
        "+------------------+------------------------------+",
        "| name             | test.json_data -> 'foo' -> 0 |",
        "+------------------+------------------------------+",
        "| object_foo       |                              |",
        "| object_foo_array | {int=1}                      |",
        "| object_foo_obj   |                              |",
        "| object_foo_null  |                              |",
        "| object_bar       |                              |",
        "| list_foo         |                              |",
        "| invalid_json     |                              |",
        "+------------------+------------------------------+",
    ];

    for_all_json_datatypes(async |dt| {
        let batches = run_query_datatype(sql, dt).await.unwrap();
        if matches!(dt, DataType::Dictionary(_, _)) {
            assert_batches_eq!(expected_dict, &batches);
        } else {
            assert_batches_eq!(expected, &batches);
        }
    })
    .await;
}

#[tokio::test]
async fn test_plan_arrow_double_nested() {
    let lines = logical_plan(r"explain select json_data->'foo'->0 from test").await;

    let expected = [
        "Projection: json_get(test.json_data, Utf8(\"foo\"), Int64(0)) AS test.json_data -> 'foo' -> 0",
        "  TableScan: test projection=[json_data]",
    ];

    assert_eq!(lines, expected);
}

#[tokio::test]
async fn test_double_arrow_double_nested() {
    let sql = "select name, json_data->>'foo'->>0 from test";
    let expected = [
        "+------------------+--------------------------------+",
        "| name             | test.json_data ->> 'foo' ->> 0 |",
        "+------------------+--------------------------------+",
        "| object_foo       |                                |",
        "| object_foo_array | 1                              |",
        "| object_foo_obj   |                                |",
        "| object_foo_null  |                                |",
        "| object_bar       |                                |",
        "| list_foo         |                                |",
        "| invalid_json     |                                |",
        "+------------------+--------------------------------+",
    ];

    for_all_json_datatypes(async |dt| {
        let batches = run_query_datatype(sql, dt).await.unwrap();
        assert_batches_eq!(expected, &batches);
    })
    .await;
}

#[tokio::test]
async fn test_plan_double_arrow_double_nested() {
    let lines = logical_plan(r"explain select json_data->>'foo'->>0 from test").await;

    let expected = [
        "Projection: json_as_text(test.json_data, Utf8(\"foo\"), Int64(0)) AS test.json_data ->> 'foo' ->> 0",
        "  TableScan: test projection=[json_data]",
    ];

    assert_eq!(lines, expected);
}

#[tokio::test]
async fn test_arrow_double_nested_cast() {
    let sql = "select name, (json_data->'foo'->0)::int from test";
    let expected = [
        "+------------------+------------------------------+",
        "| name             | test.json_data -> 'foo' -> 0 |",
        "+------------------+------------------------------+",
        "| object_foo       |                              |",
        "| object_foo_array | 1                            |",
        "| object_foo_obj   |                              |",
        "| object_foo_null  |                              |",
        "| object_bar       |                              |",
        "| list_foo         |                              |",
        "| invalid_json     |                              |",
        "+------------------+------------------------------+",
    ];

    for_all_json_datatypes(async |dt| {
        let batches = run_query_datatype(sql, dt).await.unwrap();
        assert_batches_eq!(expected, &batches);
    })
    .await;
}

#[tokio::test]
async fn test_plan_arrow_double_nested_cast() {
    let lines = logical_plan(r"explain select (json_data->'foo'->0)::int from test").await;

    let expected = [
        "Projection: json_get_int(test.json_data, Utf8(\"foo\"), Int64(0)) AS test.json_data -> 'foo' -> 0",
        "  TableScan: test projection=[json_data]",
    ];

    assert_eq!(lines, expected);
}

#[tokio::test]
async fn test_double_arrow_double_nested_cast() {
    let sql = "select name, (json_data->>'foo'->>0)::int from test";
    let expected = [
        "+------------------+--------------------------------+",
        "| name             | test.json_data ->> 'foo' ->> 0 |",
        "+------------------+--------------------------------+",
        "| object_foo       |                                |",
        "| object_foo_array | 1                              |",
        "| object_foo_obj   |                                |",
        "| object_foo_null  |                                |",
        "| object_bar       |                                |",
        "| list_foo         |                                |",
        "| invalid_json     |                                |",
        "+------------------+--------------------------------+",
    ];

    for_all_json_datatypes(async |dt| {
        let batches = run_query_datatype(sql, dt).await.unwrap();
        assert_batches_eq!(expected, &batches);
    })
    .await;
}

#[tokio::test]
async fn test_plan_double_arrow_double_nested_cast() {
    let lines = logical_plan(r"explain select (json_data->>'foo'->>0)::int from test").await;

    // NB: json_as_text(..)::int is NOT the same as `json_get_int(..)`, hence the cast is not rewritten
    let expected = [
        "Projection: CAST(json_as_text(test.json_data, Utf8(\"foo\"), Int64(0)) AS test.json_data ->> 'foo' ->> 0 AS Int32)",
        "  TableScan: test projection=[json_data]",
    ];

    assert_eq!(lines, expected);
}

#[tokio::test]
async fn test_arrow_nested_columns() {
    let sql = "select json_data->str_key1->str_key2 v from more_nested";
    let expected = [
        "+-------------+",
        "| v           |",
        "+-------------+",
        "| {array=[0]} |",
        "| {null=}     |",
        "| {null=}     |",
        "+-------------+",
    ];

    for_all_json_datatypes(async |dt| {
        let batches = run_query_datatype(sql, dt).await.unwrap();
        assert_batches_eq!(expected, &batches);
    })
    .await;
}

#[tokio::test]
async fn test_arrow_nested_double_columns() {
    let sql = "select json_data->str_key1->str_key2->int_key v from more_nested";
    let expected = [
        "+---------+",
        "| v       |",
        "+---------+",
        "| {int=0} |",
        "| {null=} |",
        "| {null=} |",
        "+---------+",
    ];

    for_all_json_datatypes(async |dt| {
        let batches = run_query_datatype(sql, dt).await.unwrap();
        assert_batches_eq!(expected, &batches);
    })
    .await;
}

#[tokio::test]
async fn test_lexical_precedence_correct() {
    #[rustfmt::skip]
    let expected = [
        "+------+",
        "| v    |",
        "+------+",
        "| true |",
        "+------+",
    ];
    let sql = r#"select '{"a": "b"}'->>'a'='b' as v"#;
    let batches = run_query(sql).await.unwrap();
    assert_batches_eq!(expected, &batches);
}

#[tokio::test]
async fn test_question_mark_contains() {
    let sql = "select name, json_data ? 'foo' from test";
    let expected = [
        "+------------------+------------------------+",
        "| name             | test.json_data ? 'foo' |",
        "+------------------+------------------------+",
        "| object_foo       | true                   |",
        "| object_foo_array | true                   |",
        "| object_foo_obj   | true                   |",
        "| object_foo_null  | true                   |",
        "| object_bar       | false                  |",
        "| list_foo         | false                  |",
        "| invalid_json     | false                  |",
        "+------------------+------------------------+",
    ];

    for_all_json_datatypes(async |dt| {
        let batches = run_query_datatype(sql, dt).await.unwrap();
        assert_batches_eq!(expected, &batches);
    })
    .await;
}

#[tokio::test]
async fn test_arrow_filter() {
    let sql = "select name from test where (json_data->>'foo') = 'abc'";

    let expected = [
        "+------------+",
        "| name       |",
        "+------------+",
        "| object_foo |",
        "+------------+",
    ];

    for_all_json_datatypes(async |dt| {
        let batches = run_query_datatype(sql, dt).await.unwrap();
        assert_batches_eq!(expected, &batches);
    })
    .await;
}

#[tokio::test]
async fn test_question_filter() {
    let sql = "select name from test where json_data ? 'foo'";
    let expected = [
        "+------------------+",
        "| name             |",
        "+------------------+",
        "| object_foo       |",
        "| object_foo_array |",
        "| object_foo_obj   |",
        "| object_foo_null  |",
        "+------------------+",
    ];

    for_all_json_datatypes(async |dt| {
        let batches = run_query_datatype(sql, dt).await.unwrap();
        assert_batches_eq!(expected, &batches);
    })
    .await;
}

#[tokio::test]
async fn test_json_get_union_is_null() {
    let sql = "select name, json_get(json_data, 'foo') is null from test";

    let expected = [
        "+------------------+----------------------------------------------+",
        "| name             | json_get(test.json_data,Utf8(\"foo\")) IS NULL |",
        "+------------------+----------------------------------------------+",
        "| object_foo       | false                                        |",
        "| object_foo_array | false                                        |",
        "| object_foo_obj   | false                                        |",
        "| object_foo_null  | true                                         |",
        "| object_bar       | true                                         |",
        "| list_foo         | true                                         |",
        "| invalid_json     | true                                         |",
        "+------------------+----------------------------------------------+",
    ];

    for_all_json_datatypes(async |dt| {
        let batches = run_query_datatype(sql, dt).await.unwrap();
        assert_batches_eq!(expected, &batches);
    })
    .await;
}

#[tokio::test]
async fn test_json_get_union_is_not_null() {
    let sql = "select name, json_get(json_data, 'foo') is not null from test";

    let expected = [
        "+------------------+--------------------------------------------------+",
        "| name             | json_get(test.json_data,Utf8(\"foo\")) IS NOT NULL |",
        "+------------------+--------------------------------------------------+",
        "| object_foo       | true                                             |",
        "| object_foo_array | true                                             |",
        "| object_foo_obj   | true                                             |",
        "| object_foo_null  | false                                            |",
        "| object_bar       | false                                            |",
        "| list_foo         | false                                            |",
        "| invalid_json     | false                                            |",
        "+------------------+--------------------------------------------------+",
    ];

    for_all_json_datatypes(async |dt| {
        let batches = run_query_datatype(sql, dt).await.unwrap();
        assert_batches_eq!(expected, &batches);
    })
    .await;
}

#[tokio::test]
async fn test_arrow_union_is_null() {
    let sql = "select name, (json_data->'foo') is null from test";
    let expected = [
        "+------------------+---------------------------------+",
        "| name             | test.json_data -> 'foo' IS NULL |",
        "+------------------+---------------------------------+",
        "| object_foo       | false                           |",
        "| object_foo_array | false                           |",
        "| object_foo_obj   | false                           |",
        "| object_foo_null  | true                            |",
        "| object_bar       | true                            |",
        "| list_foo         | true                            |",
        "| invalid_json     | true                            |",
        "+------------------+---------------------------------+",
    ];

    for_all_json_datatypes(async |dt| {
        let batches = run_query_datatype(sql, dt).await.unwrap();
        assert_batches_eq!(expected, &batches);
    })
    .await;
}

#[tokio::test]
async fn test_arrow_union_is_not_null() {
    let sql = "select name, (json_data->'foo') is not null from test";
    let expected = [
        "+------------------+-------------------------------------+",
        "| name             | test.json_data -> 'foo' IS NOT NULL |",
        "+------------------+-------------------------------------+",
        "| object_foo       | true                                |",
        "| object_foo_array | true                                |",
        "| object_foo_obj   | true                                |",
        "| object_foo_null  | false                               |",
        "| object_bar       | false                               |",
        "| list_foo         | false                               |",
        "| invalid_json     | false                               |",
        "+------------------+-------------------------------------+",
    ];

    for_all_json_datatypes(async |dt| {
        let batches = run_query_datatype(sql, dt).await.unwrap();
        assert_batches_eq!(expected, &batches);
    })
    .await;
}

#[tokio::test]
async fn test_arrow_scalar_union_is_null() {
    let sql = r#"
        select ('{"x": 1}'->'foo') is null as not_contains,
               ('{"foo": 1}'->'foo') is null as contains_num,
               ('{"foo": null}'->'foo') is null as contains_null"#;

    let expected = [
        "+--------------+--------------+---------------+",
        "| not_contains | contains_num | contains_null |",
        "+--------------+--------------+---------------+",
        "| true         | false        | true          |",
        "+--------------+--------------+---------------+",
    ];

    for_all_json_datatypes(async |dt| {
        let batches = run_query_datatype(sql, dt).await.unwrap();
        assert_batches_eq!(expected, &batches);
    })
    .await;
}

#[tokio::test]
async fn test_long_arrow_cast() {
    let sql = "select (json_data->>'foo')::int from other";

    let expected = [
        "+---------------------------+",
        "| other.json_data ->> 'foo' |",
        "+---------------------------+",
        "| 42                        |",
        "| 42                        |",
        "|                           |",
        "|                           |",
        "+---------------------------+",
    ];

    for_all_json_datatypes(async |dt| {
        let batches = run_query_datatype(sql, dt).await.unwrap();
        assert_batches_eq!(expected, &batches);
    })
    .await;
}

#[tokio::test]
async fn test_arrow_cast_numeric() {
    let sql = r#"select ('{"foo": 420}'->'foo')::numeric = 420"#;
    let batches = run_query(sql).await.unwrap();
    assert_eq!(display_val(batches).await, (DataType::Boolean, "true".to_string()));
}

#[tokio::test]
async fn test_dict_haystack() {
    let sql = "select json_get(json_data, 'foo') v from dicts";
    let expected = [
        "+-----------------------+",
        "| v                     |",
        "+-----------------------+",
        "| {object={\"bar\": [0]}} |",
        "|                       |",
        "|                       |",
        "|                       |",
        "+-----------------------+",
    ];

    for_all_json_datatypes(async |dt| {
        let batches = run_query_datatype(sql, dt).await.unwrap();
        assert_batches_eq!(expected, &batches);
    })
    .await;
}

fn check_for_null_dictionary_values(array: &dyn Array) {
    let array = array.as_any().downcast_ref::<DictionaryArray<Int64Type>>().unwrap();
    let keys_array = array.keys();
    let keys = keys_array
        .iter()
        .filter_map(|x| x.map(|v| usize::try_from(v).unwrap()))
        .collect::<Vec<_>>();
    let values_array = array.values();
    // no non-null keys should point to a null value
    for i in 0..values_array.len() {
        if values_array.is_null(i) {
            // keys should not contain
            if keys.contains(&i) {
                #[allow(clippy::print_stdout)]
                {
                    println!("keys: {keys:?}");
                    println!("values: {values_array:?}");
                    panic!("keys should not contain null values");
                }
            }
        }
    }
}

/// Test that we don't output nulls in dictionary values.
#[allow(clippy::doc_markdown)]
/// This can cause issues with arrow-rs and DataFusion; they expect nulls to be in keys.
#[tokio::test]
async fn test_dict_get_no_null_values() {
    let ctx = build_dict_schema().await;

    let sql = "select json_get(x, 'baz') v from data";
    let expected = [
        "+------------+",
        "| v          |",
        "+------------+",
        "|            |",
        "| {str=fizz} |",
        "|            |",
        "| {str=abcd} |",
        "|            |",
        "| {str=fizz} |",
        "| {str=fizz} |",
        "| {str=fizz} |",
        "| {str=fizz} |",
        "|            |",
        "+------------+",
    ];
    let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
    assert_batches_eq!(expected, &batches);
    for batch in batches {
        check_for_null_dictionary_values(batch.column(0).as_ref());
    }

    let sql = "select json_get_str(x, 'baz') v from data";
    let expected = [
        "+------+", "| v    |", "+------+", "|      |", "| fizz |", "|      |", "| abcd |", "|      |", "| fizz |",
        "| fizz |", "| fizz |", "| fizz |", "|      |", "+------+",
    ];
    let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
    assert_batches_eq!(expected, &batches);
    for batch in batches {
        check_for_null_dictionary_values(batch.column(0).as_ref());
    }
}

#[tokio::test]
async fn test_dict_haystack_filter() {
    let sql = "select json_data v from dicts where json_get(json_data, 'foo') is not null";
    let expected = [
        "+-------------------------+",
        "| v                       |",
        "+-------------------------+",
        "|  {\"foo\": {\"bar\": [0]}}  |",
        "+-------------------------+",
    ];

    for_all_json_datatypes(async |dt| {
        let batches = run_query_datatype(sql, dt).await.unwrap();
        assert_batches_eq!(expected, &batches);
    })
    .await;
}

#[tokio::test]
async fn test_dict_haystack_needle() {
    let sql = "select json_get(json_get(json_data, str_key1), str_key2) v from dicts";
    let expected = [
        "+-------------+",
        "| v           |",
        "+-------------+",
        "| {array=[0]} |",
        "|             |",
        "|             |",
        "|             |",
        "+-------------+",
    ];

    for_all_json_datatypes(async |dt| {
        let batches = run_query_datatype(sql, dt).await.unwrap();
        assert_batches_eq!(expected, &batches);
    })
    .await;
}

#[tokio::test]
async fn test_dict_length() {
    let sql = "select json_length(json_data) v from dicts";
    #[rustfmt::skip]
    let expected = [
        "+---+",
        "| v |",
        "+---+",
        "| 1 |",
        "| 1 |",
        "| 2 |",
        "| 2 |",
        "+---+",
    ];

    for_all_json_datatypes(async |dt| {
        let batches = run_query_datatype(sql, dt).await.unwrap();
        assert_batches_eq!(expected, &batches);
    })
    .await;
}

#[tokio::test]
async fn test_dict_contains() {
    let sql = "select json_contains(json_data, str_key2) v from dicts";
    let expected = [
        "+-------+",
        "| v     |",
        "+-------+",
        "| false |",
        "| false |",
        "| true  |",
        "| true  |",
        "+-------+",
    ];

    for_all_json_datatypes(async |dt| {
        let batches = run_query_datatype(sql, dt).await.unwrap();
        assert_batches_eq!(expected, &batches);
    })
    .await;
}

#[tokio::test]
async fn test_dict_contains_where() {
    let sql = "select str_key2 from dicts where json_contains(json_data, str_key2)";
    let expected = [
        "+----------+",
        "| str_key2 |",
        "+----------+",
        "| spam     |",
        "| snap     |",
        "+----------+",
    ];

    for_all_json_datatypes(async |dt| {
        let batches = run_query_datatype(sql, dt).await.unwrap();
        assert_batches_eq!(expected, &batches);
    })
    .await;
}

#[tokio::test]
async fn test_dict_get_int() {
    let sql = "select json_get_int(json_data, str_key2) v from dicts";
    #[rustfmt::skip]
    let expected = [
        "+---+",
        "| v |",
        "+---+",
        "|   |",
        "|   |",
        "| 1 |",
        "| 2 |",
        "+---+",
    ];

    for_all_json_datatypes(async |dt| {
        let batches = run_query_datatype(sql, dt).await.unwrap();
        assert_batches_eq!(expected, &batches);
    })
    .await;
}

async fn build_dict_schema() -> SessionContext {
    let mut builder = StringDictionaryBuilder::<Int8Type>::new();
    builder.append(r#"{"foo": "bar"}"#).unwrap();
    builder.append(r#"{"baz": "fizz"}"#).unwrap();
    builder.append("nah").unwrap();
    builder.append(r#"{"baz": "abcd"}"#).unwrap();
    builder.append_null();
    builder.append(r#"{"baz": "fizz"}"#).unwrap();
    builder.append(r#"{"baz": "fizz"}"#).unwrap();
    builder.append(r#"{"baz": "fizz"}"#).unwrap();
    builder.append(r#"{"baz": "fizz"}"#).unwrap();
    builder.append_null();

    let dict = builder.finish();

    assert_eq!(dict.len(), 10);
    assert_eq!(dict.values().len(), 4);

    let array = Arc::new(dict) as ArrayRef;

    let schema = Arc::new(Schema::new(vec![Field::new(
        "x",
        DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
        true,
    )]));

    let data = RecordBatch::try_new(schema.clone(), vec![array]).unwrap();

    let ctx = create_context().await.unwrap();
    ctx.register_batch("data", data).unwrap();
    ctx
}

#[tokio::test]
async fn test_dict_filter() {
    let ctx = build_dict_schema().await;

    let sql = "select json_get(x, 'baz') v from data";
    let expected = [
        "+------------+",
        "| v          |",
        "+------------+",
        "|            |",
        "| {str=fizz} |",
        "|            |",
        "| {str=abcd} |",
        "|            |",
        "| {str=fizz} |",
        "| {str=fizz} |",
        "| {str=fizz} |",
        "| {str=fizz} |",
        "|            |",
        "+------------+",
    ];

    let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();

    assert_batches_eq!(expected, &batches);
}

#[tokio::test]
async fn test_dict_filter_is_not_null() {
    let ctx = build_dict_schema().await;
    let sql = "select x from data where json_get(x, 'baz') is not null";
    let expected = [
        "+-----------------+",
        "| x               |",
        "+-----------------+",
        "| {\"baz\": \"fizz\"} |",
        "| {\"baz\": \"abcd\"} |",
        "| {\"baz\": \"fizz\"} |",
        "| {\"baz\": \"fizz\"} |",
        "| {\"baz\": \"fizz\"} |",
        "| {\"baz\": \"fizz\"} |",
        "+-----------------+",
    ];

    let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();

    assert_batches_eq!(expected, &batches);
}

#[tokio::test]
async fn test_dict_filter_contains() {
    let ctx = build_dict_schema().await;
    let sql = "select x from data where json_contains(x, 'baz')";
    let expected = [
        "+-----------------+",
        "| x               |",
        "+-----------------+",
        "| {\"baz\": \"fizz\"} |",
        "| {\"baz\": \"abcd\"} |",
        "| {\"baz\": \"fizz\"} |",
        "| {\"baz\": \"fizz\"} |",
        "| {\"baz\": \"fizz\"} |",
        "| {\"baz\": \"fizz\"} |",
        "+-----------------+",
    ];

    let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();

    assert_batches_eq!(expected, &batches);

    // test with a boolean OR as well
    let batches = ctx
        .sql(&format!("{sql} or false"))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    assert_batches_eq!(expected, &batches);
}

#[tokio::test]
async fn test_json_object_keys() {
    let expected = [
        "+----------------------------------+",
        "| json_object_keys(test.json_data) |",
        "+----------------------------------+",
        "| [foo]                            |",
        "| [foo]                            |",
        "| [foo]                            |",
        "| [foo]                            |",
        "| [bar]                            |",
        "|                                  |",
        "|                                  |",
        "+----------------------------------+",
    ];

    let sql = "select json_object_keys(json_data) from test";
    for_all_json_datatypes(async |dt| {
        let batches = run_query_datatype(sql, dt).await.unwrap();
        assert_batches_eq!(expected, &batches);
    })
    .await;
}

#[tokio::test]
async fn test_json_object_keys_many() {
    let expected = [
        "+-----------------------+",
        "| v                     |",
        "+-----------------------+",
        "| [foo, bar, spam, ham] |",
        "+-----------------------+",
    ];

    let sql = r#"select json_object_keys('{"foo": 1, "bar": 2.2, "spam": true, "ham": []}') as v"#;
    let batches = run_query(sql).await.unwrap();
    assert_batches_eq!(expected, &batches);
}

#[tokio::test]
async fn test_json_object_keys_nested() {
    let json = r#"'{"foo": [{"bar": {"spam": true, "ham": []}}]}'"#;

    let sql = format!("select json_object_keys({json}) as v");
    let batches = run_query(&sql).await.unwrap();
    #[rustfmt::skip]
    let expected = [
        "+-------+",
        "| v     |",
        "+-------+",
        "| [foo] |",
        "+-------+",
    ];
    assert_batches_eq!(expected, &batches);

    let sql = format!("select json_object_keys({json}, 'foo') as v");
    let batches = run_query(&sql).await.unwrap();
    #[rustfmt::skip]
    let expected = [
        "+---+",
        "| v |",
        "+---+",
        "|   |",
        "+---+",
    ];
    assert_batches_eq!(expected, &batches);

    let sql = format!("select json_object_keys({json}, 'foo', 0) as v");
    let batches = run_query(&sql).await.unwrap();
    #[rustfmt::skip]
    let expected = [
        "+-------+",
        "| v     |",
        "+-------+",
        "| [bar] |",
        "+-------+",
    ];
    assert_batches_eq!(expected, &batches);

    let sql = format!("select json_object_keys({json}, 'foo', 0, 'bar') as v");
    let batches = run_query(&sql).await.unwrap();
    #[rustfmt::skip]
    let expected = [
        "+-------------+",
        "| v           |",
        "+-------------+",
        "| [spam, ham] |",
        "+-------------+",
    ];
    assert_batches_eq!(expected, &batches);
}

#[tokio::test]
async fn test_lookup_literal_column_matrix() {
    let sql = r#"
WITH json_columns AS (
    SELECT unnest(['{"a": 1}', '{"b": 2}']) as json_column
), attr_names AS (
    -- this is deliberately a different length to json_columns
    SELECT
        unnest(['a', 'b', 'c']) as attr_name,
        arrow_cast(unnest(['a', 'b', 'c']), 'Dictionary(Int32, Utf8)') as attr_name_dict
)
SELECT
    attr_name,
    json_column,
    'a' = attr_name,
    json_get('{"a": 1}', attr_name),       -- literal lookup with column
    json_get('{"a": 1}', attr_name_dict),  -- literal lookup with dict column
    json_get('{"a": 1}', 'a'),             -- literal lookup with literal
    json_get(json_column, attr_name),      -- column lookup with column
    json_get(json_column, attr_name_dict), -- column lookup with dict column
    json_get(json_column, 'a')             -- column lookup with literal
FROM json_columns, attr_names
"#;

    let expected = [
        "+-----------+-------------+----------------------------------+-------------------------------------------------+------------------------------------------------------+--------------------------------------+---------------------------------------------------------+--------------------------------------------------------------+----------------------------------------------+",
        "| attr_name | json_column | Utf8(\"a\") = attr_names.attr_name | json_get(Utf8(\"{\"a\": 1}\"),attr_names.attr_name) | json_get(Utf8(\"{\"a\": 1}\"),attr_names.attr_name_dict) | json_get(Utf8(\"{\"a\": 1}\"),Utf8(\"a\")) | json_get(json_columns.json_column,attr_names.attr_name) | json_get(json_columns.json_column,attr_names.attr_name_dict) | json_get(json_columns.json_column,Utf8(\"a\")) |",
        "+-----------+-------------+----------------------------------+-------------------------------------------------+------------------------------------------------------+--------------------------------------+---------------------------------------------------------+--------------------------------------------------------------+----------------------------------------------+",
        "| a         | {\"a\": 1}    | true                             | {int=1}                                         | {int=1}                                              | {int=1}                              | {int=1}                                                 | {int=1}                                                      | {int=1}                                      |",
        "| b         | {\"a\": 1}    | false                            | {null=}                                         | {null=}                                              | {int=1}                              | {null=}                                                 | {null=}                                                      | {int=1}                                      |",
        "| c         | {\"a\": 1}    | false                            | {null=}                                         | {null=}                                              | {int=1}                              | {null=}                                                 | {null=}                                                      | {int=1}                                      |",
        "| a         | {\"b\": 2}    | true                             | {int=1}                                         | {int=1}                                              | {int=1}                              | {null=}                                                 | {null=}                                                      | {null=}                                      |",
        "| b         | {\"b\": 2}    | false                            | {null=}                                         | {null=}                                              | {int=1}                              | {int=2}                                                 | {int=2}                                                      | {null=}                                      |",
        "| c         | {\"b\": 2}    | false                            | {null=}                                         | {null=}                                              | {int=1}                              | {null=}                                                 | {null=}                                                      | {null=}                                      |",
        "+-----------+-------------+----------------------------------+-------------------------------------------------+------------------------------------------------------+--------------------------------------+---------------------------------------------------------+--------------------------------------------------------------+----------------------------------------------+",
    ];

    let batches = run_query(sql).await.unwrap();
    assert_batches_eq!(expected, &batches);
}

#[tokio::test]
async fn test_lookup_literal_column_matrix_dictionaries() {
    let sql = r#"
WITH json_columns AS (
    SELECT arrow_cast(unnest(['{"a": 1}', '{"b": 2}']), 'Dictionary(Int32, Utf8)') as json_column
), attr_names AS (
    -- this is deliberately a different length to json_columns
    SELECT
        unnest(['a', 'b', 'c']) as attr_name,
        arrow_cast(unnest(['a', 'b', 'c']), 'Dictionary(Int32, Utf8)') as attr_name_dict
)
SELECT
    attr_name,
    json_column,
    'a' = attr_name,
    json_get('{"a": 1}', attr_name),       -- literal lookup with column
    json_get('{"a": 1}', attr_name_dict),  -- literal lookup with dict column
    json_get('{"a": 1}', 'a'),             -- literal lookup with literal
    json_get(json_column, attr_name),      -- column lookup with column
    json_get(json_column, attr_name_dict), -- column lookup with dict column
    json_get(json_column, 'a')             -- column lookup with literal
FROM json_columns, attr_names
"#;

    // NB as compared to the non-dictionary case, we null out the dictionary keys if the return
    // value is a dict, which is why we get true nulls instead of {null=}
    let expected = [
        "+-----------+-------------+----------------------------------+-------------------------------------------------+------------------------------------------------------+--------------------------------------+---------------------------------------------------------+--------------------------------------------------------------+----------------------------------------------+",
        "| attr_name | json_column | Utf8(\"a\") = attr_names.attr_name | json_get(Utf8(\"{\"a\": 1}\"),attr_names.attr_name) | json_get(Utf8(\"{\"a\": 1}\"),attr_names.attr_name_dict) | json_get(Utf8(\"{\"a\": 1}\"),Utf8(\"a\")) | json_get(json_columns.json_column,attr_names.attr_name) | json_get(json_columns.json_column,attr_names.attr_name_dict) | json_get(json_columns.json_column,Utf8(\"a\")) |",
        "+-----------+-------------+----------------------------------+-------------------------------------------------+------------------------------------------------------+--------------------------------------+---------------------------------------------------------+--------------------------------------------------------------+----------------------------------------------+",
        "| a         | {\"a\": 1}    | true                             | {int=1}                                         | {int=1}                                              | {int=1}                              | {int=1}                                                 | {int=1}                                                      | {int=1}                                      |",
        "| b         | {\"a\": 1}    | false                            | {null=}                                         | {null=}                                              | {int=1}                              |                                                         |                                                              | {int=1}                                      |",
        "| c         | {\"a\": 1}    | false                            | {null=}                                         | {null=}                                              | {int=1}                              |                                                         |                                                              | {int=1}                                      |",
        "| a         | {\"b\": 2}    | true                             | {int=1}                                         | {int=1}                                              | {int=1}                              |                                                         |                                                              |                                              |",
        "| b         | {\"b\": 2}    | false                            | {null=}                                         | {null=}                                              | {int=1}                              | {int=2}                                                 | {int=2}                                                      |                                              |",
        "| c         | {\"b\": 2}    | false                            | {null=}                                         | {null=}                                              | {int=1}                              |                                                         |                                                              |                                              |",
        "+-----------+-------------+----------------------------------+-------------------------------------------------+------------------------------------------------------+--------------------------------------+---------------------------------------------------------+--------------------------------------------------------------+----------------------------------------------+",
    ];

    let batches = run_query(sql).await.unwrap();
    assert_batches_eq!(expected, &batches);
}
