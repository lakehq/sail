//! A minimal model of OpenLineage run events.
//!
//! See also: <https://openlineage.io/docs/spec/object-model>

use std::collections::BTreeMap;

use chrono::{DateTime, SecondsFormat, Utc};
use datafusion::arrow::datatypes::{DataType, Field, Fields};
use serde::Serialize;
use serde_json::{Value, json};
use uuid::Uuid;

use crate::options::{LineageTag, RunReference};

pub const PRODUCER: &str = concat!(
    "https://github.com/lakehq/sail/tree/v",
    env!("CARGO_PKG_VERSION")
);

const RUN_EVENT_SCHEMA_URL: &str =
    "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent";
const SCHEMA_FACET_URL: &str =
    "https://openlineage.io/spec/facets/1-1-1/SchemaDatasetFacet.json#/$defs/SchemaDatasetFacet";
const SYMLINKS_FACET_URL: &str = "https://openlineage.io/spec/facets/1-0-1/SymlinksDatasetFacet.json#/$defs/SymlinksDatasetFacet";
const DATASOURCE_FACET_URL: &str = "https://openlineage.io/spec/facets/1-0-1/DatasourceDatasetFacet.json#/$defs/DatasourceDatasetFacet";
const STORAGE_FACET_URL: &str =
    "https://openlineage.io/spec/facets/1-0-1/StorageDatasetFacet.json#/$defs/StorageDatasetFacet";
const LIFECYCLE_FACET_URL: &str = "https://openlineage.io/spec/facets/1-0-1/LifecycleStateChangeDatasetFacet.json#/$defs/LifecycleStateChangeDatasetFacet";
const JOB_TYPE_FACET_URL: &str =
    "https://openlineage.io/spec/facets/2-0-3/JobTypeJobFacet.json#/$defs/JobTypeJobFacet";
const PROCESSING_ENGINE_FACET_URL: &str = "https://openlineage.io/spec/facets/1-1-1/ProcessingEngineRunFacet.json#/$defs/ProcessingEngineRunFacet";
const PARENT_RUN_FACET_URL: &str =
    "https://openlineage.io/spec/facets/1-1-0/ParentRunFacet.json#/$defs/ParentRunFacet";
const ERROR_MESSAGE_FACET_URL: &str = "https://openlineage.io/spec/facets/1-0-1/ErrorMessageRunFacet.json#/$defs/ErrorMessageRunFacet";
const TAGS_JOB_FACET_URL: &str =
    "https://openlineage.io/spec/facets/1-0-0/TagsJobFacet.json#/$defs/TagsJobFacet";
const TAGS_RUN_FACET_URL: &str =
    "https://openlineage.io/spec/facets/1-0-0/TagsRunFacet.json#/$defs/TagsRunFacet";

/// The integration name reported in the job type facet.
const INTEGRATION: &str = "SAIL";

pub type Facets = BTreeMap<String, Value>;

/// Builds a facet object with the required `_producer` and `_schemaURL` fields.
fn facet(schema_url: &str, body: Value) -> Value {
    let mut value = json!({
        "_producer": PRODUCER,
        "_schemaURL": schema_url,
    });
    if let (Value::Object(target), Value::Object(source)) = (&mut value, body) {
        target.extend(source);
    }
    value
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum EventType {
    Start,
    Complete,
    Abort,
    Fail,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RunEvent {
    pub event_type: EventType,
    pub event_time: String,
    pub run: Run,
    pub job: Job,
    pub inputs: Vec<InputDataset>,
    pub outputs: Vec<OutputDataset>,
    pub producer: &'static str,
    #[serde(rename = "schemaURL")]
    pub schema_url: &'static str,
}

impl RunEvent {
    pub fn new(
        event_type: EventType,
        event_time: DateTime<Utc>,
        run: Run,
        job: Job,
        inputs: Vec<InputDataset>,
        outputs: Vec<OutputDataset>,
    ) -> Self {
        Self {
            event_type,
            event_time: event_time.to_rfc3339_opts(SecondsFormat::Millis, true),
            run,
            job,
            inputs,
            outputs,
            producer: PRODUCER,
            schema_url: RUN_EVENT_SCHEMA_URL,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Run {
    pub run_id: Uuid,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub facets: Facets,
}

impl Run {
    pub fn new(run_id: Uuid) -> Self {
        Self {
            run_id,
            facets: Facets::new(),
        }
    }

    pub fn with_processing_engine(mut self) -> Self {
        self.facets.insert(
            "processing_engine".to_string(),
            facet(
                PROCESSING_ENGINE_FACET_URL,
                json!({
                    "name": "Sail",
                    "version": env!("CARGO_PKG_VERSION"),
                }),
            ),
        );
        self
    }

    pub fn with_parent(mut self, parent: &RunReference, root: Option<&RunReference>) -> Self {
        let reference = |x: &RunReference| {
            json!({
                "run": { "runId": x.run_id },
                "job": { "namespace": x.namespace, "name": x.name },
            })
        };
        let mut body = reference(parent);
        if let (Some(root), Value::Object(map)) = (root, &mut body) {
            map.insert("root".to_string(), reference(root));
        }
        self.facets
            .insert("parent".to_string(), facet(PARENT_RUN_FACET_URL, body));
        self
    }

    pub fn with_tags(mut self, tags: &[LineageTag]) -> Self {
        if !tags.is_empty() {
            self.facets
                .insert("tags".to_string(), tags_facet(TAGS_RUN_FACET_URL, tags));
        }
        self
    }

    pub fn with_error_message(mut self, message: impl Into<String>) -> Self {
        self.facets.insert(
            "errorMessage".to_string(),
            facet(
                ERROR_MESSAGE_FACET_URL,
                json!({
                    "message": message.into(),
                    "programmingLanguage": "Rust",
                }),
            ),
        );
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcessingType {
    Batch,
    Streaming,
}

impl ProcessingType {
    fn as_str(&self) -> &'static str {
        match self {
            ProcessingType::Batch => "BATCH",
            ProcessingType::Streaming => "STREAMING",
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct Job {
    pub namespace: String,
    pub name: String,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub facets: Facets,
}

impl Job {
    pub fn new(namespace: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            namespace: namespace.into(),
            name: name.into(),
            facets: Facets::new(),
        }
    }

    pub fn with_job_type(mut self, processing_type: ProcessingType, job_type: &str) -> Self {
        self.facets.insert(
            "jobType".to_string(),
            facet(
                JOB_TYPE_FACET_URL,
                json!({
                    "processingType": processing_type.as_str(),
                    "integration": INTEGRATION,
                    "jobType": job_type,
                }),
            ),
        );
        self
    }

    pub fn with_tags(mut self, tags: &[LineageTag]) -> Self {
        if !tags.is_empty() {
            self.facets
                .insert("tags".to_string(), tags_facet(TAGS_JOB_FACET_URL, tags));
        }
        self
    }
}

fn tags_facet(schema_url: &str, tags: &[LineageTag]) -> Value {
    let tags = tags
        .iter()
        .map(|x| json!({ "key": x.key, "value": x.value, "source": x.source }))
        .collect::<Vec<_>>();
    facet(schema_url, json!({ "tags": tags }))
}

#[derive(Debug, Clone, Serialize)]
pub struct InputDataset {
    pub namespace: String,
    pub name: String,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub facets: Facets,
}

#[derive(Debug, Clone, Serialize)]
pub struct OutputDataset {
    pub namespace: String,
    pub name: String,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub facets: Facets,
}

/// The lifecycle state change of an output dataset.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LifecycleStateChange {
    Create,
    Overwrite,
    Drop,
    Truncate,
}

impl LifecycleStateChange {
    fn as_str(&self) -> &'static str {
        match self {
            LifecycleStateChange::Create => "CREATE",
            LifecycleStateChange::Overwrite => "OVERWRITE",
            LifecycleStateChange::Drop => "DROP",
            LifecycleStateChange::Truncate => "TRUNCATE",
        }
    }
}

/// A builder for dataset facets shared by input and output datasets.
#[derive(Debug, Default)]
pub(crate) struct DatasetFacetsBuilder {
    facets: Facets,
}

impl DatasetFacetsBuilder {
    pub fn schema(mut self, fields: &Fields) -> Self {
        self.facets.insert(
            "schema".to_string(),
            facet(SCHEMA_FACET_URL, json!({ "fields": schema_fields(fields) })),
        );
        self
    }

    pub fn symlink(mut self, namespace: &str, name: &str) -> Self {
        self.facets.insert(
            "symlinks".to_string(),
            facet(
                SYMLINKS_FACET_URL,
                json!({
                    "identifiers": [{
                        "namespace": namespace,
                        "name": name,
                        "type": "TABLE",
                    }],
                }),
            ),
        );
        self
    }

    pub fn data_source(mut self, namespace: &str) -> Self {
        self.facets.insert(
            "dataSource".to_string(),
            facet(
                DATASOURCE_FACET_URL,
                json!({ "name": namespace, "uri": namespace }),
            ),
        );
        self
    }

    pub fn storage(mut self, format: &str) -> Self {
        let format = format.to_lowercase();
        // Table formats are reported as the storage layer, while plain file formats
        // are reported as the file format.
        let body = match format.as_str() {
            "delta" => json!({ "storageLayer": format, "fileFormat": "parquet" }),
            "iceberg" => json!({ "storageLayer": format }),
            _ => json!({ "storageLayer": "default", "fileFormat": format }),
        };
        self.facets
            .insert("storage".to_string(), facet(STORAGE_FACET_URL, body));
        self
    }

    pub fn lifecycle(mut self, change: LifecycleStateChange) -> Self {
        self.facets.insert(
            "lifecycleStateChange".to_string(),
            facet(
                LIFECYCLE_FACET_URL,
                json!({ "lifecycleStateChange": change.as_str() }),
            ),
        );
        self
    }

    pub fn build(self) -> Facets {
        self.facets
    }
}

fn schema_fields(fields: &Fields) -> Vec<Value> {
    fields.iter().map(|f| schema_field(f)).collect()
}

fn schema_field(field: &Field) -> Value {
    let mut value = json!({
        "name": field.name(),
        "type": data_type_name(field.data_type()),
    });
    let nested = match field.data_type() {
        DataType::Struct(fields) => Some(schema_fields(fields)),
        DataType::List(f)
        | DataType::LargeList(f)
        | DataType::ListView(f)
        | DataType::LargeListView(f)
        | DataType::FixedSizeList(f, _)
        | DataType::Map(f, _) => Some(vec![schema_field(f)]),
        _ => None,
    };
    if let (Some(nested), Value::Object(map)) = (nested, &mut value) {
        map.insert("fields".to_string(), Value::Array(nested));
    }
    value
}

/// Returns a Spark-like type name for the Arrow data type.
fn data_type_name(data_type: &DataType) -> String {
    match data_type {
        DataType::Null => "void".to_string(),
        DataType::Boolean => "boolean".to_string(),
        DataType::Int8 | DataType::UInt8 => "tinyint".to_string(),
        DataType::Int16 | DataType::UInt16 => "smallint".to_string(),
        DataType::Int32 | DataType::UInt32 => "int".to_string(),
        DataType::Int64 | DataType::UInt64 => "bigint".to_string(),
        DataType::Float16 | DataType::Float32 => "float".to_string(),
        DataType::Float64 => "double".to_string(),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => "string".to_string(),
        DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::FixedSizeBinary(_) => "binary".to_string(),
        DataType::Date32 | DataType::Date64 => "date".to_string(),
        DataType::Timestamp(_, Some(_)) => "timestamp".to_string(),
        DataType::Timestamp(_, None) => "timestamp_ntz".to_string(),
        DataType::Decimal32(p, s)
        | DataType::Decimal64(p, s)
        | DataType::Decimal128(p, s)
        | DataType::Decimal256(p, s) => format!("decimal({p},{s})"),
        DataType::Struct(_) => "struct".to_string(),
        DataType::List(_)
        | DataType::LargeList(_)
        | DataType::ListView(_)
        | DataType::LargeListView(_)
        | DataType::FixedSizeList(_, _) => "array".to_string(),
        DataType::Map(_, _) => "map".to_string(),
        DataType::Dictionary(_, value) => data_type_name(value),
        other => other.to_string().to_lowercase(),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    #[test]
    fn test_schema_facet_nested_fields() {
        let fields = Fields::from(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(
                "tags",
                DataType::List(Arc::new(Field::new("element", DataType::Utf8, true))),
                true,
            ),
            Field::new(
                "point",
                DataType::Struct(Fields::from(vec![Field::new("x", DataType::Float64, true)])),
                true,
            ),
        ]);
        let value = schema_fields(&fields);
        assert_eq!(
            Value::Array(value),
            json!([
                {"name": "id", "type": "bigint"},
                {"name": "tags", "type": "array", "fields": [{"name": "element", "type": "string"}]},
                {"name": "point", "type": "struct", "fields": [{"name": "x", "type": "double"}]},
            ])
        );
    }

    #[test]
    fn test_run_event_serialization() -> Result<(), serde_json::Error> {
        let run_id = Uuid::nil();
        let time = DateTime::<Utc>::from_timestamp(0, 0).unwrap_or_default();
        let event = RunEvent::new(
            EventType::Start,
            time,
            Run::new(run_id),
            Job::new("sail", "app.query"),
            vec![],
            vec![],
        );
        let value = serde_json::to_value(&event)?;
        assert_eq!(value["eventType"], "START");
        assert_eq!(value["eventTime"], "1970-01-01T00:00:00.000Z");
        assert_eq!(value["run"]["runId"], run_id.to_string());
        assert_eq!(value["job"]["name"], "app.query");
        assert_eq!(value["schemaURL"], RUN_EVENT_SCHEMA_URL);
        assert!(value["run"].get("facets").is_none());
        Ok(())
    }
}
