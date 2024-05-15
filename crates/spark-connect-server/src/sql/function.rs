#[cfg(test)]
mod tests {
    use crate::error::SparkResult;
    use crate::schema::JsonDataType;
    use crate::session::Session;
    use crate::tests::{execute_query, test_gold_set};
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
            |example: FunctionExample| -> SparkResult<Vec<String>> {
                let ctx = session.context();
                rt.block_on(async {
                    let result = execute_query(&ctx, &example.query).await?;
                    let result = result.into_iter().map(|x| format!("{:?}", x)).collect();
                    Ok(result)
                })
            },
        )?)
    }
}
