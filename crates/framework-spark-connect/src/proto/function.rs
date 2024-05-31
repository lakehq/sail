#[cfg(test)]
mod tests {
    use crate::error::{SparkError, SparkResult};
    use crate::executor::execute_query;
    use crate::proto::data_type_json::JsonDataType;
    use crate::session::Session;
    use framework_common::tests::test_gold_set;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub(crate) struct FunctionExample {
        query: String,
        result: Vec<String>,
        schema: JsonDataType,
    }

    #[test]
    fn test_sql_function() -> Result<(), Box<dyn std::error::Error>> {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let session = Session::new(None, "test".to_string());
        Ok(test_gold_set(
            "tests/gold_data/function/*.json",
            |example: FunctionExample| -> SparkResult<String> {
                let ctx = session.context();
                let result =
                    rt.block_on(async { execute_query(ctx, example.query.as_str()).await });
                // TODO: validate the result against the expected output
                // TODO: handle non-deterministic results and error messages
                match result {
                    Ok(_) => Ok("ok".to_string()),
                    Err(_) => Err(SparkError::internal("error".to_string())),
                }
            },
            |e| SparkError::internal(e),
        )?)
    }
}
