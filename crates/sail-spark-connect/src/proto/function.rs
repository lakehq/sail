#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::RecordBatch;
    use arrow::error::ArrowError;
    use arrow_cast::display::{ArrayFormatter, FormatOptions};
    use sail_common::tests::test_gold_set;
    use sail_plan::object_store::ObjectStoreConfig;
    use serde::{Deserialize, Serialize};

    use crate::error::{SparkError, SparkResult};
    use crate::proto::data_type_json::JsonDataType;
    use crate::session::Session;

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub(crate) struct FunctionExample {
        query: String,
        result: Vec<String>,
        schema: JsonDataType,
    }

    #[allow(dead_code)]
    fn format_record_batches(batches: Vec<RecordBatch>) -> SparkResult<Vec<String>> {
        let options = FormatOptions::default().with_null("NULL");
        let mut output = vec![];
        for batch in batches {
            let formatters = batch
                .columns()
                .iter()
                .map(|column| ArrayFormatter::try_new(column, &options))
                .collect::<Result<Vec<_>, ArrowError>>()?;
            for row in 0..batch.num_rows() {
                let line = formatters
                    .iter()
                    .map(|formatter| formatter.value(row).try_to_string())
                    .collect::<Result<Vec<_>, ArrowError>>()?
                    .join("\t");
                output.push(line);
            }
        }
        Ok(output)
    }

    #[test]
    fn test_sql_function() -> Result<(), Box<dyn std::error::Error>> {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let session = Session::try_new(
            None,
            "test".to_string(),
            Arc::new(ObjectStoreConfig::default()),
        )?;
        Ok(test_gold_set(
            "tests/gold_data/function/*.json",
            |example: FunctionExample| -> SparkResult<String> {
                let result =
                    rt.block_on(async { session.execute_query(example.query.as_str()).await });
                // TODO: validate the result against the expected output
                // TODO: handle non-deterministic results and error messages
                match result {
                    // FIXME: the output can be non-deterministic
                    Ok(_) => Ok("ok".to_string()),
                    Err(x) => Err(x),
                }
            },
            SparkError::internal,
        )?)
    }
}
