use crate::session::SessionManager;
use crate::spark::connect as sc;
use crate::spark::connect::config_request as cr;
use crate::spark::connect::data_type as dt;
use crate::spark::connect::execute_plan_response as epr;
use crate::spark::connect::plan;
use crate::spark::connect::relation as rel;
use crate::spark::connect::spark_connect_service_server::SparkConnectService;
use crate::spark::connect::{
    AddArtifactsRequest, AddArtifactsResponse, AnalyzePlanRequest, AnalyzePlanResponse,
    ArtifactStatusesRequest, ArtifactStatusesResponse, ConfigRequest, ConfigResponse,
    ExecutePlanRequest, ExecutePlanResponse, InterruptRequest, InterruptResponse, KeyValue,
    LocalRelation, Plan, ReattachExecuteRequest, Relation, RelationCommon, ReleaseExecuteRequest,
    ReleaseExecuteResponse, ShowString,
};
use datafusion::arrow::array::StringArray;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::ipc::reader::StreamReader;
use datafusion::arrow::ipc::writer::StreamWriter;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::pretty::pretty_format_batches;
use std::io::Cursor;
use std::sync::Arc;
use tonic::codegen::tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};
use uuid::Uuid;

pub struct SparkConnectServer {
    session_manager: SessionManager,
}

impl Default for SparkConnectServer {
    fn default() -> Self {
        Self {
            session_manager: SessionManager::new(),
        }
    }
}

#[tonic::async_trait]
impl SparkConnectService for SparkConnectServer {
    type ExecutePlanStream = ReceiverStream<Result<ExecutePlanResponse, Status>>;

    async fn execute_plan(
        &self,
        request: Request<ExecutePlanRequest>,
    ) -> Result<Response<Self::ExecutePlanStream>, Status> {
        println!("{:?}", request);
        let request = request.into_inner();
        let session_id = request.session_id;
        let operation_id = request
            .operation_id
            .unwrap_or_else(|| Uuid::new_v4().to_string());
        let mut show_string = vec![];
        if let ExecutePlanRequest {
            plan:
                Some(Plan {
                    op_type:
                        Some(plan::OpType::Root(Relation {
                            common:
                                Some(RelationCommon {
                                    source_info: _,
                                    plan_id: Some(_),
                                }),
                            rel_type: Some(rel::RelType::ShowString(show)),
                        })),
                }),
            ..
        } = request
        {
            if let ShowString {
                input: Some(input), ..
            } = show.as_ref()
            {
                if let Relation {
                    common:
                        Some(RelationCommon {
                            source_info: _,
                            plan_id: Some(_),
                        }),
                    rel_type:
                        Some(rel::RelType::LocalRelation(LocalRelation {
                            data: Some(data),
                            schema: Some(_),
                        })),
                } = input.as_ref()
                {
                    let cursor = Cursor::new(data);
                    let mut reader = StreamReader::try_new(cursor, None)
                        .or_else(|_| Err(Status::internal("failed to create stream reader")))?;
                    while let Some(batch) = reader.next() {
                        let batch = batch
                            .or_else(|_| Err(Status::internal("failed to read record batch")))?;
                        let s = pretty_format_batches(&[batch])
                            .or_else(|_| Err(Status::internal("failed to format record batch")))?
                            .to_string();
                        show_string.push(s);
                    }
                }
            }
        }
        let show_string_array = StringArray::from(show_string);
        let show_string_field = Field::new("show_string", DataType::Utf8, false);
        let show_string_schema = Schema::new(vec![show_string_field]);
        let show_string_record_batch = RecordBatch::try_new(
            Arc::new(show_string_schema),
            vec![Arc::new(show_string_array)],
        )
        .or_else(|_| Err(Status::internal("failed to create record batch")))?;

        let schema = Some(sc::DataType {
            kind: Some(dt::Kind::Struct(dt::Struct {
                fields: vec![dt::StructField {
                    name: "show_string".to_string(),
                    data_type: Some(sc::DataType {
                        kind: Some(dt::Kind::String(dt::String {
                            type_variation_reference: 0,
                        })),
                    }),
                    nullable: false,
                    metadata: None,
                }],
                type_variation_reference: 0,
            })),
        });

        let mut output: Vec<u8> = Vec::new();
        {
            let cursor = Cursor::new(&mut output);
            let mut writer = StreamWriter::try_new(cursor, &show_string_record_batch.schema())
                .or_else(|_| Err(Status::internal("failed to create stream writer")))?;
            writer
                .write(&show_string_record_batch)
                .or_else(|_| Err(Status::internal("failed to write record batch")))?;
            writer
                .finish()
                .or_else(|_| Err(Status::internal("failed to finish stream writer")))?;
        }

        let responses = vec![
            ExecutePlanResponse {
                session_id: session_id.clone(),
                operation_id: operation_id.clone(),
                response_id: Uuid::new_v4().to_string(),
                metrics: None,
                observed_metrics: vec![],
                schema,
                response_type: Some(epr::ResponseType::ArrowBatch(epr::ArrowBatch {
                    row_count: show_string_record_batch.num_rows() as i64,
                    data: output,
                })),
            },
            ExecutePlanResponse {
                session_id: session_id.clone(),
                operation_id: operation_id.clone(),
                response_id: Uuid::new_v4().to_string(),
                metrics: None,
                observed_metrics: vec![],
                schema: None,
                response_type: Some(epr::ResponseType::ResultComplete(epr::ResultComplete {})),
            },
        ];
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        tokio::spawn(async move {
            for response in responses {
                let _ = tx.send(Ok(response)).await;
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn analyze_plan(
        &self,
        request: Request<AnalyzePlanRequest>,
    ) -> Result<Response<AnalyzePlanResponse>, Status> {
        println!("{:?}", request);
        todo!()
    }

    async fn config(
        &self,
        request: Request<ConfigRequest>,
    ) -> Result<Response<ConfigResponse>, Status> {
        println!("{:?}", request);
        let request = request.into_inner();
        let session_id = request.session_id;
        let session = self.session_manager.get_session(&session_id)?;
        let mut session = session
            .lock()
            .or_else(|_| Err(Status::internal("failed to lock session")))?;

        let mut pairs = vec![];
        let warnings = vec![];
        if let Some(cr::Operation { op_type: Some(op) }) = request.operation {
            match op {
                cr::operation::OpType::Get(cr::Get { keys }) => {
                    for key in keys {
                        pairs.push(KeyValue {
                            key: key.clone(),
                            value: session.get_config(&key).map(|v| v.clone()),
                        });
                    }
                }
                cr::operation::OpType::Set(cr::Set { pairs: kv }) => {
                    for KeyValue { key, value } in kv {
                        if let Some(value) = value {
                            session.set_config(&key, &value);
                        } else {
                            session.unset_config(&key);
                        }
                    }
                }
                cr::operation::OpType::GetWithDefault(cr::GetWithDefault { pairs: kv }) => {
                    for KeyValue { key, value } in kv {
                        pairs.push(KeyValue {
                            key: key.clone(),
                            value: session.get_config(&key).map(|v| v.clone()).or(value),
                        });
                    }
                }
                cr::operation::OpType::GetOption(cr::GetOption { keys }) => {
                    for key in keys {
                        if let Some(value) = session.get_config(&key) {
                            pairs.push(KeyValue {
                                key: key.clone(),
                                value: Some(value.clone()),
                            });
                        }
                    }
                }
                cr::operation::OpType::GetAll(cr::GetAll { prefix }) => {
                    for (k, v) in session.iter_config(&prefix) {
                        pairs.push(KeyValue {
                            key: k.clone(),
                            value: Some(v.clone()),
                        });
                    }
                }
                cr::operation::OpType::Unset(cr::Unset { keys }) => {
                    for key in keys {
                        session.unset_config(&key);
                    }
                }
                cr::operation::OpType::IsModifiable(cr::IsModifiable { keys }) => {
                    for key in keys {
                        pairs.push(KeyValue {
                            key: key.clone(),
                            value: Some("true".to_string()),
                        });
                    }
                }
            }
        }
        let response = ConfigResponse {
            session_id,
            pairs,
            warnings,
        };
        Ok(Response::new(response))
    }

    async fn add_artifacts(
        &self,
        request: Request<Streaming<AddArtifactsRequest>>,
    ) -> Result<Response<AddArtifactsResponse>, Status> {
        println!("{:?}", request);
        todo!()
    }

    async fn artifact_status(
        &self,
        request: Request<ArtifactStatusesRequest>,
    ) -> Result<Response<ArtifactStatusesResponse>, Status> {
        println!("{:?}", request);
        todo!()
    }

    async fn interrupt(
        &self,
        request: Request<InterruptRequest>,
    ) -> Result<Response<InterruptResponse>, Status> {
        println!("{:?}", request);
        todo!()
    }

    type ReattachExecuteStream = ReceiverStream<Result<ExecutePlanResponse, Status>>;

    async fn reattach_execute(
        &self,
        request: Request<ReattachExecuteRequest>,
    ) -> Result<Response<Self::ReattachExecuteStream>, Status> {
        println!("{:?}", request);
        todo!()
    }

    async fn release_execute(
        &self,
        request: Request<ReleaseExecuteRequest>,
    ) -> Result<Response<ReleaseExecuteResponse>, Status> {
        println!("{:?}", request);
        let request = request.into_inner();
        let session_id = request.session_id;
        let operation_id = request.operation_id;
        let response = ReleaseExecuteResponse {
            session_id,
            operation_id: Some(operation_id),
        };
        Ok(Response::new(response))
    }
}
