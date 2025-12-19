#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use datafusion::arrow::array::RecordBatch;
    use datafusion::arrow::error::ArrowError;
    use datafusion::arrow::util::display::{ArrayFormatter, FormatOptions};
    use sail_common::config::AppConfig;
    use sail_common::runtime::RuntimeManager;
    use sail_common::tests::test_gold_set;
    use sail_common_datafusion::extension::SessionExtensionAccessor;
    use sail_common_datafusion::session::JobService;
    use sail_plan::resolve_and_execute_plan;
    use serde::{Deserialize, Serialize};

    use crate::error::{SparkError, SparkResult};
    use crate::executor::read_stream;
    use crate::proto::data_type_json::JsonDataType;
    use crate::session::{SparkSession, SparkSessionKey};
    use crate::session_manager::create_spark_session_manager;
    use crate::spark::connect::relation::RelType;
    use crate::spark::connect::{Relation, Sql};

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub(crate) struct FunctionExample {
        query: String,
        result: Vec<String>,
        schema: JsonDataType,
    }

    #[allow(dead_code)]
    fn format_record_batches(batches: Vec<RecordBatch>) -> SparkResult<Vec<String>> {
        let options = FormatOptions::default();
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
        let config = Arc::new(AppConfig::load()?);
        let runtime = RuntimeManager::try_new(&config.runtime)?;
        let handle = runtime.handle();
        // We create the session manager inside an async context, even though the
        // `SessionManager::try_new()` function itself is sync. This is because the actor system
        // may need to spawn actors when the session runs in cluster mode.
        let manager = handle
            .primary()
            .block_on(async { create_spark_session_manager(config, handle.clone()) })?;
        let session_key = SparkSessionKey {
            user_id: "".to_string(),
            session_id: "test".to_string(),
        };
        let context = handle
            .primary()
            .block_on(manager.get_or_create_session_context(session_key))?;
        test_gold_set(
            "tests/gold_data/function/*.json",
            |example: FunctionExample| -> SparkResult<String> {
                let relation = Relation {
                    common: None,
                    #[allow(deprecated)]
                    rel_type: Some(RelType::Sql(Sql {
                        query: example.query,
                        args: HashMap::new(),
                        pos_args: vec![],
                        named_arguments: HashMap::new(),
                        pos_arguments: vec![],
                    })),
                };
                let plan = relation.try_into()?;
                let result = handle.primary().block_on(async {
                    let spark = context.extension::<SparkSession>()?;
                    let service = context.extension::<JobService>()?;
                    let (plan, _) =
                        resolve_and_execute_plan(&context, spark.plan_config()?, plan).await?;
                    let stream = service.runner().execute(&context, plan).await?;
                    read_stream(stream).await
                });
                // TODO: validate the result against the expected output
                // TODO: handle non-deterministic results and error messages
                match result {
                    // FIXME: the output can be non-deterministic
                    Ok(_) => Ok("ok".to_string()),
                    Err(x) => Err(x),
                }
            },
            SparkError::internal,
        )
        .map_err(|e| e.into())
    }
}
