use std::any::Any;
use std::fmt::{self, Debug, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::{Action, FlightData, FlightDescriptor};
use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::ipc::reader::StreamReader;
use datafusion::arrow::ipc::writer::StreamWriter;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{DataFusionError, Result};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use futures::TryStreamExt;

use crate::error::ExecutionResult;
use crate::rpc::{ClientHandle, ClientOptions, ClientService};

/// Metadata returned when discovering functions from a remote plugin.
#[derive(Debug, Clone)]
pub struct RemoteFunctionInfo {
    pub name: String,
    pub volatility: Volatility,
}

/// Arrow Flight client for communicating with a remote plugin server.
///
/// The plugin protocol uses standard Arrow Flight RPCs:
/// - `ListActions`: discover available UDFs
/// - `DoAction("return_type.<name>")`: resolve return type given input types
/// - `DoExchange`: execute a UDF on a RecordBatch
#[derive(Clone)]
pub struct RemotePluginClient {
    endpoint: String,
    inner: ClientHandle<FlightServiceClient<ClientService>>,
}

impl RemotePluginClient {
    pub fn new(endpoint: &str) -> ExecutionResult<Self> {
        let options = Self::parse_endpoint(endpoint)?;
        Ok(Self {
            endpoint: endpoint.to_string(),
            inner: ClientHandle::new(options),
        })
    }

    fn parse_endpoint(endpoint: &str) -> ExecutionResult<ClientOptions> {
        let enable_tls = endpoint.starts_with("grpc+tls://");
        let stripped = endpoint
            .strip_prefix("grpc+tls://")
            .or_else(|| endpoint.strip_prefix("grpc://"))
            .ok_or_else(|| {
                crate::error::ExecutionError::InternalError(format!(
                    "invalid plugin endpoint: {endpoint}. Expected grpc:// or grpc+tls:// prefix"
                ))
            })?;
        let (host, port_str) = stripped.rsplit_once(':').ok_or_else(|| {
            crate::error::ExecutionError::InternalError(format!(
                "invalid plugin endpoint (missing port): {endpoint}"
            ))
        })?;
        let port = port_str.parse::<u16>().map_err(|e| {
            crate::error::ExecutionError::InternalError(format!("invalid port in {endpoint}: {e}"))
        })?;
        Ok(ClientOptions {
            enable_tls,
            host: host.to_string(),
            port,
        })
    }

    /// Discover available UDFs from the remote plugin via `ListActions`.
    pub async fn discover_functions(&self) -> ExecutionResult<Vec<RemoteFunctionInfo>> {
        let mut client = self.inner.get().await?;
        let response = client
            .list_actions(arrow_flight::Empty {})
            .await
            .map_err(|e| {
                crate::error::ExecutionError::InternalError(format!(
                    "plugin {}: list_actions failed: {e}",
                    self.endpoint
                ))
            })?;
        let mut functions = Vec::new();
        let mut stream = response.into_inner();
        while let Some(action_type) = stream.message().await.map_err(|e| {
            crate::error::ExecutionError::InternalError(format!(
                "plugin {}: stream error: {e}",
                self.endpoint
            ))
        })? {
            if let Some(name) = action_type.r#type.strip_prefix("udf.") {
                let volatility = parse_volatility(&action_type.description);
                functions.push(RemoteFunctionInfo {
                    name: name.to_string(),
                    volatility,
                });
            }
        }
        Ok(functions)
    }

    /// Resolve the return type for a function given input argument types.
    ///
    /// Sends input types as an IPC-serialized Schema via `DoAction("return_type.<name>")`.
    /// The plugin responds with an IPC-serialized Schema containing a single field
    /// whose data type is the return type.
    pub async fn get_return_type(
        &self,
        function_name: &str,
        arg_types: &[DataType],
    ) -> ExecutionResult<DataType> {
        let mut client = self.inner.get().await?;
        let fields: Vec<Field> = arg_types
            .iter()
            .enumerate()
            .map(|(i, dt)| Field::new(format!("arg_{i}"), dt.clone(), true))
            .collect();
        let schema = Schema::new(fields);
        let body = serialize_schema_to_ipc(&schema)?;
        let action = Action {
            r#type: format!("return_type.{function_name}"),
            body: body.into(),
        };
        let response = client.do_action(action).await.map_err(|e| {
            crate::error::ExecutionError::InternalError(format!(
                "plugin {}: return_type for {function_name} failed: {e}",
                self.endpoint
            ))
        })?;
        let result = response
            .into_inner()
            .message()
            .await
            .map_err(|e| {
                crate::error::ExecutionError::InternalError(format!(
                    "plugin {}: return_type stream error: {e}",
                    self.endpoint
                ))
            })?
            .ok_or_else(|| {
                crate::error::ExecutionError::InternalError(format!(
                    "plugin {}: empty return_type response for {function_name}",
                    self.endpoint
                ))
            })?;
        let return_schema = deserialize_schema_from_ipc(&result.body)?;
        if return_schema.fields().is_empty() {
            return Err(crate::error::ExecutionError::InternalError(format!(
                "plugin {}: return_type schema has no fields for {function_name}",
                self.endpoint
            )));
        }
        Ok(return_schema.field(0).data_type().clone())
    }

    /// Execute a UDF by sending a RecordBatch of arguments via `DoExchange`.
    ///
    /// The `FlightDescriptor` path is `["udf", "<function_name>"]`.
    /// Returns the first column of the result batch.
    pub async fn execute(
        &self,
        function_name: &str,
        input_batch: RecordBatch,
    ) -> ExecutionResult<ArrayRef> {
        let mut client = self.inner.get().await?;

        let descriptor =
            FlightDescriptor::new_path(vec!["udf".to_string(), function_name.to_string()]);
        // Build FlightData items from the input batch.
        // FlightDataEncoderBuilder produces Result<FlightData>, so we collect
        // and unwrap before sending to do_exchange which expects plain FlightData.
        let encoder = FlightDataEncoderBuilder::new()
            .with_flight_descriptor(Some(descriptor))
            .build(futures::stream::once(async { Ok(input_batch) }));
        let flight_data_items: Vec<FlightData> = encoder.try_collect().await.map_err(|e| {
            crate::error::ExecutionError::InternalError(format!(
                "plugin {}: failed to encode flight data for {function_name}: {e}",
                self.endpoint
            ))
        })?;
        let flight_data_stream = futures::stream::iter(flight_data_items);

        let response = client.do_exchange(flight_data_stream).await.map_err(|e| {
            crate::error::ExecutionError::InternalError(format!(
                "plugin {}: do_exchange for {function_name} failed: {e}",
                self.endpoint
            ))
        })?;

        let stream = FlightRecordBatchStream::new_from_flight_data(
            response
                .into_inner()
                .map_err(|e| arrow_flight::error::FlightError::Tonic(Box::new(e))),
        );
        let batches: Vec<RecordBatch> = stream.try_collect().await.map_err(|e| {
            crate::error::ExecutionError::InternalError(format!(
                "plugin {}: result stream error for {function_name}: {e}",
                self.endpoint
            ))
        })?;
        if batches.is_empty() {
            return Err(crate::error::ExecutionError::InternalError(format!(
                "plugin {}: empty result for {function_name}",
                self.endpoint
            )));
        }
        let result = &batches[0];
        if result.num_columns() < 1 {
            return Err(crate::error::ExecutionError::InternalError(format!(
                "plugin {}: result has no columns for {function_name}",
                self.endpoint
            )));
        }
        Ok(result.column(0).clone())
    }
}

/// A `ScalarUDFImpl` that delegates execution to a remote Arrow Flight plugin.
///
/// During query planning, `return_type()` calls the plugin's `DoAction("return_type.<name>")`.
/// During execution, `invoke_with_args()` sends argument batches via `DoExchange`.
pub struct RemoteScalarUdf {
    name: String,
    endpoint: String,
    client: RemotePluginClient,
    signature: Signature,
    runtime: tokio::runtime::Handle,
}

impl RemoteScalarUdf {
    pub fn new(
        name: String,
        endpoint: String,
        client: RemotePluginClient,
        volatility: Volatility,
        runtime: tokio::runtime::Handle,
    ) -> Self {
        Self {
            name,
            endpoint,
            client,
            signature: Signature::variadic_any(volatility),
            runtime,
        }
    }

    /// The endpoint URL of the remote plugin server.
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }
}

impl Debug for RemoteScalarUdf {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("RemoteScalarUdf")
            .field("name", &self.name)
            .field("endpoint", &self.endpoint)
            .finish()
    }
}

impl PartialEq for RemoteScalarUdf {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name && self.endpoint == other.endpoint
    }
}

impl Eq for RemoteScalarUdf {}

impl Hash for RemoteScalarUdf {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
        self.endpoint.hash(state);
    }
}

impl ScalarUDFImpl for RemoteScalarUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let client = self.client.clone();
        let name = self.name.clone();
        let arg_types = arg_types.to_vec();
        // Use block_in_place to safely call async code from a sync context
        // within a tokio multi-threaded runtime. This avoids the panic that
        // would occur with block_on when already inside an async executor.
        tokio::task::block_in_place(|| {
            self.runtime
                .block_on(async { client.get_return_type(&name, &arg_types).await })
                .map_err(|e| DataFusionError::External(Box::new(e)))
        })
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let num_rows = args.number_rows;

        let mut columns = Vec::with_capacity(args.args.len());
        let mut fields = Vec::with_capacity(args.args.len());
        for (i, arg) in args.args.iter().enumerate() {
            let arr = arg.clone().into_array(num_rows)?;
            fields.push(Field::new(
                format!("arg_{i}"),
                arr.data_type().clone(),
                true,
            ));
            columns.push(arr);
        }
        let schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(schema, columns)?;

        let client = self.client.clone();
        let name = self.name.clone();
        // Use block_in_place to safely bridge async Flight calls from
        // DataFusion's synchronous ScalarUDFImpl::invoke_with_args.
        let result = tokio::task::block_in_place(|| {
            self.runtime
                .block_on(async { client.execute(&name, batch).await })
                .map_err(|e| DataFusionError::External(Box::new(e)))
        })?;

        Ok(ColumnarValue::Array(result))
    }
}

fn parse_volatility(description: &str) -> Volatility {
    // Try to parse JSON metadata; fall back to volatile if parsing fails
    if let Ok(value) = serde_json::from_str::<serde_json::Value>(description) {
        match value.get("volatility").and_then(|v| v.as_str()) {
            Some("immutable") => Volatility::Immutable,
            Some("stable") => Volatility::Stable,
            _ => Volatility::Volatile,
        }
    } else {
        Volatility::Volatile
    }
}

fn serialize_schema_to_ipc(schema: &Schema) -> ExecutionResult<Vec<u8>> {
    let mut buf = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buf, schema).map_err(|e| {
            crate::error::ExecutionError::InternalError(format!(
                "failed to create IPC stream writer: {e}"
            ))
        })?;
        writer.finish().map_err(|e| {
            crate::error::ExecutionError::InternalError(format!(
                "failed to finish IPC stream writer: {e}"
            ))
        })?;
    }
    Ok(buf)
}

fn deserialize_schema_from_ipc(buf: &[u8]) -> ExecutionResult<Schema> {
    let reader = StreamReader::try_new(std::io::Cursor::new(buf), None).map_err(|e| {
        crate::error::ExecutionError::InternalError(format!(
            "failed to create IPC stream reader: {e}"
        ))
    })?;
    Ok(reader.schema().as_ref().clone())
}
