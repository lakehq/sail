#![allow(dead_code)]
use std::sync::{Arc, LazyLock};

use datafusion::arrow::array::{
    ArrayRef, DictionaryArray, Int32Array, Int64Array, StringViewArray, UInt32Array, UInt64Array, UInt8Array,
};
use datafusion::arrow::datatypes::{DataType, Field, Int32Type, Int64Type, Schema, UInt32Type, UInt8Type};
use datafusion::arrow::util::display::{ArrayFormatter, FormatOptions};
use datafusion::arrow::{array::LargeStringArray, array::StringArray, record_batch::RecordBatch};
use datafusion::common::ParamValues;
use datafusion::error::Result;
use datafusion::execution::context::SessionContext;
use datafusion::prelude::SessionConfig;
use datafusion_functions_json::register_all;

pub async fn create_context() -> Result<SessionContext> {
    let config = SessionConfig::new().set_str("datafusion.sql_parser.dialect", "postgres");
    let mut ctx = SessionContext::new_with_config(config);
    register_all(&mut ctx)?;
    Ok(ctx)
}

pub static DICT_TYPE: LazyLock<DataType> =
    LazyLock::new(|| DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)));
pub static LARGE_DICT_TYPE: LazyLock<DataType> =
    LazyLock::new(|| DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::LargeUtf8)));

#[expect(clippy::too_many_lines)]
async fn create_test_table(json_data_type: &DataType) -> Result<SessionContext> {
    let ctx = create_context().await?;

    let test_data = [
        ("object_foo", r#" {"foo": "abc"} "#),
        ("object_foo_array", r#" {"foo": [1]} "#),
        ("object_foo_obj", r#" {"foo": {}} "#),
        ("object_foo_null", r#" {"foo": null} "#),
        ("object_bar", r#" {"bar": true} "#),
        ("list_foo", r#" ["foo"] "#),
        ("invalid_json", "is not json"),
    ];
    let json_values = test_data.iter().map(|(_, json)| *json);

    let json_array = match json_data_type {
        DataType::Utf8 => Arc::new(StringArray::from_iter_values(json_values)) as ArrayRef,
        DataType::LargeUtf8 => Arc::new(LargeStringArray::from_iter_values(json_values)),
        DataType::Utf8View => Arc::new(StringViewArray::from_iter_values(json_values)),
        DataType::Dictionary(key_type, _) if key_type.as_ref() != &DataType::Int32 => {
            panic!("Only Int32 dictionary encoding is supported for JSON data in these tests")
        }
        DataType::Dictionary(key_type, child)
            if key_type.as_ref() == &DataType::Int32 && child.as_ref() == &DataType::Utf8 =>
        {
            Arc::new(DictionaryArray::<Int32Type>::new(
                Int32Array::from_iter_values(0..(i32::try_from(json_values.len()).expect("fits in a i32"))),
                Arc::new(StringArray::from_iter_values(json_values)),
            ))
        }
        DataType::Dictionary(key_type, child)
            if key_type.as_ref() == &DataType::Int32 && child.as_ref() == &DataType::LargeUtf8 =>
        {
            Arc::new(DictionaryArray::<Int32Type>::new(
                Int32Array::from_iter_values(0..(i32::try_from(json_values.len()).expect("fits in a i32"))),
                Arc::new(LargeStringArray::from_iter_values(json_values)),
            ))
        }
        _ => panic!("Unsupported JSON data type: {json_data_type}"),
    };

    let test_batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8View, false),
            Field::new("json_data", json_data_type.clone(), false),
        ])),
        vec![
            Arc::new(StringViewArray::from(
                test_data.iter().map(|(name, _)| *name).collect::<Vec<_>>(),
            )),
            json_array,
        ],
    )?;
    ctx.register_batch("test", test_batch)?;

    let other_data = [
        (r#" {"foo": 42} "#, "foo", 0),
        (r#" {"foo": 42} "#, "bar", 1),
        (r" [42] ", "foo", 0),
        (r" [42] ", "bar", 1),
    ];
    let other_batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("json_data", DataType::Utf8, false),
            Field::new("str_key", DataType::Utf8, false),
            Field::new("int_key", DataType::Int64, false),
        ])),
        vec![
            Arc::new(StringArray::from(
                other_data.iter().map(|(json, _, _)| *json).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                other_data.iter().map(|(_, str_key, _)| *str_key).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                other_data.iter().map(|(_, _, int_key)| *int_key).collect::<Vec<_>>(),
            )),
        ],
    )?;
    ctx.register_batch("other", other_batch)?;

    let more_nested = [
        (r#" {"foo": {"bar": [0]}} "#, "foo", "bar", 0),
        (r#" {"foo": {"bar": [1]}} "#, "foo", "spam", 0),
        (r#" {"foo": {"bar": null}} "#, "foo", "bar", 0),
    ];
    let more_nested_batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("json_data", DataType::Utf8, false),
            Field::new("str_key1", DataType::Utf8, false),
            Field::new(
                "str_key2",
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                false,
            ),
            Field::new("int_key", DataType::Int64, false),
        ])),
        vec![
            Arc::new(StringArray::from(
                more_nested.iter().map(|(json, _, _, _)| *json).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                more_nested
                    .iter()
                    .map(|(_, str_key1, _, _)| *str_key1)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(
                more_nested
                    .iter()
                    .map(|(_, _, str_key2, _)| *str_key2)
                    .collect::<DictionaryArray<Int32Type>>(),
            ),
            Arc::new(Int64Array::from(
                more_nested
                    .iter()
                    .map(|(_, _, _, int_key)| *int_key)
                    .collect::<Vec<_>>(),
            )),
        ],
    )?;
    ctx.register_batch("more_nested", more_nested_batch)?;

    let dict_data = [
        (r#" {"foo": {"bar": [0]}} "#, "foo", "bar", 0),
        (r#" {"bar": "snap"} "#, "foo", "spam", 0),
        (r#" {"spam": 1, "snap": 2} "#, "foo", "spam", 0),
        (r#" {"spam": 1, "snap": 2} "#, "foo", "snap", 0),
    ];
    let dict_batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new(
                "json_data",
                DataType::Dictionary(DataType::UInt32.into(), DataType::Utf8.into()),
                false,
            ),
            Field::new(
                "str_key1",
                DataType::Dictionary(DataType::UInt8.into(), DataType::LargeUtf8.into()),
                false,
            ),
            Field::new(
                "str_key2",
                DataType::Dictionary(DataType::UInt8.into(), DataType::Utf8View.into()),
                false,
            ),
            Field::new(
                "int_key",
                DataType::Dictionary(DataType::Int64.into(), DataType::UInt64.into()),
                false,
            ),
        ])),
        vec![
            Arc::new(DictionaryArray::<UInt32Type>::new(
                UInt32Array::from_iter_values(
                    dict_data
                        .iter()
                        .enumerate()
                        .map(|(id, _)| u32::try_from(id).expect("fits in a u32")),
                ),
                Arc::new(StringArray::from(
                    dict_data.iter().map(|(json, _, _, _)| *json).collect::<Vec<_>>(),
                )),
            )),
            Arc::new(DictionaryArray::<UInt8Type>::new(
                UInt8Array::from_iter_values(
                    dict_data
                        .iter()
                        .enumerate()
                        .map(|(id, _)| u8::try_from(id).expect("fits in a u8")),
                ),
                Arc::new(LargeStringArray::from(
                    dict_data
                        .iter()
                        .map(|(_, str_key1, _, _)| *str_key1)
                        .collect::<Vec<_>>(),
                )),
            )),
            Arc::new(DictionaryArray::<UInt8Type>::new(
                UInt8Array::from_iter_values(
                    dict_data
                        .iter()
                        .enumerate()
                        .map(|(id, _)| u8::try_from(id).expect("fits in a u8")),
                ),
                Arc::new(StringViewArray::from(
                    dict_data
                        .iter()
                        .map(|(_, _, str_key2, _)| *str_key2)
                        .collect::<Vec<_>>(),
                )),
            )),
            Arc::new(DictionaryArray::<Int64Type>::new(
                Int64Array::from_iter_values(
                    dict_data
                        .iter()
                        .enumerate()
                        .map(|(id, _)| i64::try_from(id).expect("fits in a i64")),
                ),
                Arc::new(UInt64Array::from_iter_values(
                    dict_data
                        .iter()
                        .map(|(_, _, _, int_key)| u64::try_from(*int_key).expect("not negative")),
                )),
            )),
        ],
    )?;
    ctx.register_batch("dicts", dict_batch)?;

    Ok(ctx)
}

pub async fn run_query(sql: &str) -> Result<Vec<RecordBatch>> {
    run_query_datatype(sql, &DataType::Utf8View).await
}

pub async fn run_query_datatype(sql: &str, json_data_type: &DataType) -> Result<Vec<RecordBatch>> {
    let ctx = create_test_table(json_data_type).await?;
    ctx.sql(sql).await?.collect().await
}

pub async fn run_query_params(
    sql: &str,
    json_data_type: &DataType,
    query_values: impl Into<ParamValues>,
) -> Result<Vec<RecordBatch>> {
    let ctx = create_test_table(json_data_type).await?;
    ctx.sql(sql).await?.with_param_values(query_values)?.collect().await
}

pub async fn for_all_json_datatypes(f: impl AsyncFn(&DataType)) {
    for dt in [
        &DataType::Utf8,
        &DataType::LargeUtf8,
        &DataType::Utf8View,
        &DICT_TYPE,
        &LARGE_DICT_TYPE,
    ] {
        f(dt).await;
    }
}

pub async fn display_val(batch: Vec<RecordBatch>) -> (DataType, String) {
    assert_eq!(batch.len(), 1);
    let batch = batch.first().unwrap();
    assert_eq!(batch.num_rows(), 1);
    let schema = batch.schema();
    let schema_col = schema.field(0);
    let c = batch.column(0);
    let options = FormatOptions::default().with_display_error(true);
    let f = ArrayFormatter::try_new(c.as_ref(), &options).unwrap();
    let repr = f.value(0).try_to_string().unwrap();
    (schema_col.data_type().clone(), repr)
}

pub async fn logical_plan(sql: &str) -> Vec<String> {
    let batches = run_query(sql).await.unwrap();
    let plan_col = batches[0].column(1).as_any().downcast_ref::<StringArray>().unwrap();
    let logical_plan = plan_col.value(0);
    logical_plan.split('\n').map(ToString::to_string).collect()
}
