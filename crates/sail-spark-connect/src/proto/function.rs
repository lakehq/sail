#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    use datafusion::arrow::array::RecordBatch;
    use datafusion::arrow::error::ArrowError;
    use datafusion::arrow::util::display::{ArrayFormatter, FormatOptions};
    use sail_common::config::AppConfig;
    use sail_common::tests::test_gold_set;
    use sail_plan::resolve_and_execute_plan;
    use sail_server::actor::ActorSystem;
    use serde::{Deserialize, Serialize};

    use crate::error::{SparkError, SparkResult};
    use crate::executor::read_stream;
    use crate::proto::data_type_json::JsonDataType;
    use crate::session::SparkExtension;
    use crate::session_manager::{SessionKey, SessionManager};
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
        let config = Arc::new(AppConfig::load()?);
        let system = Arc::new(Mutex::new(ActorSystem::new()));
        let session_key = SessionKey {
            user_id: None,
            session_id: "test".to_string(),
        };
        let context = rt.block_on(async {
            // We create the session inside an async context, even though the
            // `create_session_context` function itself is sync. This is because the actor system
            // may need to spawn actors when the session runs in cluster mode.
            SessionManager::create_session_context(config, system, session_key)
        })?;
        Ok(test_gold_set(
            "tests/gold_data/function/*.json",
            |example: FunctionExample| -> SparkResult<String> {
                let relation = Relation {
                    common: None,
                    rel_type: Some(RelType::Sql(Sql {
                        query: example.query,
                        args: HashMap::new(),
                        pos_args: vec![],
                    })),
                };
                let plan = relation.try_into()?;
                let result = rt.block_on(async {
                    let spark = SparkExtension::get(&context)?;
                    let plan =
                        resolve_and_execute_plan(&context, spark.plan_config()?, plan).await?;
                    let stream = spark.job_runner().execute(&context, plan).await?;
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
        )?)
    }
}
