use async_stream;
use log::debug;
use tonic::codegen::tokio_stream::StreamExt;
use tonic::{Request, Response, Status, Streaming};
use uuid::Uuid;

use crate::error::ProtoFieldExt;
use crate::executor::ExecutorMetadata;
use crate::service;
use crate::service::ExecutePlanResponseStream;
use crate::session_manager::{SessionKey, SessionManager};
use crate::spark::connect as sc;
use crate::spark::connect::analyze_plan_request::Analyze;
use crate::spark::connect::analyze_plan_response::Result as AnalyzeResult;
use crate::spark::connect::command::CommandType;
use crate::spark::connect::config_request::operation::OpType as ConfigOpType;
use crate::spark::connect::interrupt_request::{Interrupt, InterruptType};
use crate::spark::connect::release_execute_request::{Release, ReleaseAll, ReleaseUntil};
use crate::spark::connect::spark_connect_service_server::SparkConnectService;
use crate::spark::connect::{
    plan, AddArtifactsRequest, AddArtifactsResponse, AnalyzePlanRequest, AnalyzePlanResponse,
    ArtifactStatusesRequest, ArtifactStatusesResponse, Command, ConfigRequest, ConfigResponse,
    ExecutePlanRequest, InterruptRequest, InterruptResponse, Plan, ReattachExecuteRequest,
    ReleaseExecuteRequest, ReleaseExecuteResponse,
};

#[derive(Debug)]
pub struct SparkConnectServer {
    session_manager: SessionManager,
}

impl SparkConnectServer {
    pub fn new(session_manager: SessionManager) -> Self {
        Self { session_manager }
    }
}

fn is_reattachable(request_options: &[sc::execute_plan_request::RequestOption]) -> bool {
    use sc::execute_plan_request::request_option::RequestOption;

    for item in request_options {
        if let Some(RequestOption::ReattachOptions(v)) = &item.request_option {
            return v.reattachable;
        }
    }
    false
}

#[tonic::async_trait]
impl SparkConnectService for SparkConnectServer {
    type ExecutePlanStream = ExecutePlanResponseStream;

    async fn execute_plan(
        &self,
        request: Request<ExecutePlanRequest>,
    ) -> Result<Response<Self::ExecutePlanStream>, Status> {
        let request = request.into_inner();
        debug!("{:?}", request);
        let session_key = SessionKey {
            user_id: request.user_context.map(|u| u.user_id),
            session_id: request.session_id.clone(),
        };
        let metadata = ExecutorMetadata {
            operation_id: request
                .operation_id
                .unwrap_or_else(|| Uuid::new_v4().to_string()),
            tags: request.tags,
            reattachable: is_reattachable(&request.request_options),
        };
        let ctx = self
            .session_manager
            .get_or_create_session_context(session_key)
            .await?;
        let Plan { op_type: op } = request.plan.required("plan")?;
        let op = op.required("plan op")?;
        let stream = match op {
            plan::OpType::Root(relation) => {
                service::handle_execute_relation(&ctx, relation, metadata).await?
            }
            plan::OpType::Command(Command {
                command_type: command,
            }) => {
                let command = command.required("command")?;
                match command {
                    CommandType::RegisterFunction(udf) => {
                        service::handle_execute_register_function(&ctx, udf, metadata).await?
                    }
                    CommandType::WriteOperation(write) => {
                        service::handle_execute_write_operation(&ctx, write, metadata).await?
                    }
                    CommandType::CreateDataframeView(view) => {
                        service::handle_execute_create_dataframe_view(&ctx, view, metadata).await?
                    }
                    CommandType::WriteOperationV2(write) => {
                        service::handle_execute_write_operation_v2(&ctx, write, metadata).await?
                    }
                    CommandType::SqlCommand(sql) => {
                        service::handle_execute_sql_command(&ctx, sql, metadata).await?
                    }
                    CommandType::WriteStreamOperationStart(start) => {
                        service::handle_execute_write_stream_operation_start(&ctx, start, metadata)
                            .await?
                    }
                    CommandType::StreamingQueryCommand(stream) => {
                        service::handle_execute_streaming_query_command(&ctx, stream, metadata)
                            .await?
                    }
                    CommandType::GetResourcesCommand(resource) => {
                        service::handle_execute_get_resources_command(&ctx, resource, metadata)
                            .await?
                    }
                    CommandType::StreamingQueryManagerCommand(manager) => {
                        service::handle_execute_streaming_query_manager_command(
                            &ctx, manager, metadata,
                        )
                        .await?
                    }
                    CommandType::RegisterTableFunction(udtf) => {
                        service::handle_execute_register_table_function(&ctx, udtf, metadata)
                            .await?
                    }
                    CommandType::Extension(_) => {
                        return Err(Status::unimplemented("unsupported command extension"));
                    }
                }
            }
        };
        Ok(Response::new(stream))
    }

    async fn analyze_plan(
        &self,
        request: Request<AnalyzePlanRequest>,
    ) -> Result<Response<AnalyzePlanResponse>, Status> {
        let request = request.into_inner();
        debug!("{:?}", request);
        let session_key = SessionKey {
            user_id: request.user_context.map(|u| u.user_id),
            session_id: request.session_id.clone(),
        };
        let ctx = self
            .session_manager
            .get_or_create_session_context(session_key)
            .await?;
        let analyze = request.analyze.required("analyze")?;
        let result = match analyze {
            Analyze::Schema(schema) => {
                let schema = service::handle_analyze_schema(&ctx, schema).await?;
                Some(AnalyzeResult::Schema(schema))
            }
            Analyze::Explain(explain) => {
                let explain = service::handle_analyze_explain(&ctx, explain).await?;
                Some(AnalyzeResult::Explain(explain))
            }
            Analyze::TreeString(tree) => {
                let tree = service::handle_analyze_tree_string(&ctx, tree).await?;
                Some(AnalyzeResult::TreeString(tree))
            }
            Analyze::IsLocal(local) => {
                let local = service::handle_analyze_is_local(&ctx, local).await?;
                Some(AnalyzeResult::IsLocal(local))
            }
            Analyze::IsStreaming(streaming) => {
                let streaming = service::handle_analyze_is_streaming(&ctx, streaming).await?;
                Some(AnalyzeResult::IsStreaming(streaming))
            }
            Analyze::InputFiles(input) => {
                let input = service::handle_analyze_input_files(&ctx, input).await?;
                Some(AnalyzeResult::InputFiles(input))
            }
            Analyze::SparkVersion(version) => {
                let version = service::handle_analyze_spark_version(&ctx, version).await?;
                Some(AnalyzeResult::SparkVersion(version))
            }
            Analyze::DdlParse(ddl) => {
                let ddl = service::handle_analyze_ddl_parse(&ctx, ddl).await?;
                Some(AnalyzeResult::DdlParse(ddl))
            }
            Analyze::SameSemantics(same) => {
                let same = service::handle_analyze_same_semantics(&ctx, same).await?;
                Some(AnalyzeResult::SameSemantics(same))
            }
            Analyze::SemanticHash(hash) => {
                let hash = service::handle_analyze_semantic_hash(&ctx, hash).await?;
                Some(AnalyzeResult::SemanticHash(hash))
            }
            Analyze::Persist(persist) => {
                let persist = service::handle_analyze_persist(&ctx, persist).await?;
                Some(AnalyzeResult::Persist(persist))
            }
            Analyze::Unpersist(unpersist) => {
                let unpersist = service::handle_analyze_unpersist(&ctx, unpersist).await?;
                Some(AnalyzeResult::Unpersist(unpersist))
            }
            Analyze::GetStorageLevel(level) => {
                let level = service::handle_analyze_get_storage_level(&ctx, level).await?;
                Some(AnalyzeResult::GetStorageLevel(level))
            }
        };
        let response = AnalyzePlanResponse {
            session_id: request.session_id,
            result,
        };
        debug!("{:?}", response);
        Ok(Response::new(response))
    }

    async fn config(
        &self,
        request: Request<ConfigRequest>,
    ) -> Result<Response<ConfigResponse>, Status> {
        let request = request.into_inner();
        debug!("{:?}", request);
        let session_key = SessionKey {
            user_id: request.user_context.map(|u| u.user_id),
            session_id: request.session_id.clone(),
        };
        let ctx = self
            .session_manager
            .get_or_create_session_context(session_key)
            .await?;
        let sc::config_request::Operation { op_type: op } =
            request.operation.required("operation")?;
        let op = op.required("operation type")?;
        let response = match op {
            ConfigOpType::Get(sc::config_request::Get { keys }) => {
                service::handle_config_get(&ctx, keys)?
            }
            ConfigOpType::Set(sc::config_request::Set { pairs }) => {
                service::handle_config_set(&ctx, pairs)?
            }
            ConfigOpType::GetWithDefault(sc::config_request::GetWithDefault { pairs }) => {
                service::handle_config_get_with_default(&ctx, pairs)?
            }
            ConfigOpType::GetOption(sc::config_request::GetOption { keys }) => {
                service::handle_config_get_option(&ctx, keys)?
            }
            ConfigOpType::GetAll(sc::config_request::GetAll { prefix }) => {
                service::handle_config_get_all(&ctx, prefix)?
            }
            ConfigOpType::Unset(sc::config_request::Unset { keys }) => {
                service::handle_config_unset(&ctx, keys)?
            }
            ConfigOpType::IsModifiable(sc::config_request::IsModifiable { keys }) => {
                service::handle_config_is_modifiable(&ctx, keys)?
            }
        };
        debug!("{:?}", response);
        Ok(Response::new(response))
    }

    async fn add_artifacts(
        &self,
        request: Request<Streaming<AddArtifactsRequest>>,
    ) -> Result<Response<AddArtifactsResponse>, Status> {
        let mut request = request.into_inner();
        let first = match request.next().await {
            Some(item) => item?,
            None => {
                return Err(Status::invalid_argument(
                    "at least one artifact request is required",
                ));
            }
        };
        debug!("{:?}", first);
        let session_key = SessionKey {
            user_id: first.user_context.map(|u| u.user_id),
            session_id: first.session_id.clone(),
        };
        let ctx = self
            .session_manager
            .get_or_create_session_context(session_key)
            .await?;
        let payload = first.payload;
        let session_id = first.session_id;
        let stream = async_stream::try_stream! {
            if let Some(payload) = payload {
                yield payload;
            }
            while let Some(item) = request.next().await {
                let item = item?;
                debug!("{:?}", item);
                if item.session_id != session_id {
                    Err(Status::invalid_argument("session ID must be consistent"))?;
                }
                if let Some(payload) = item.payload {
                    yield payload;
                }
            }
        };
        let artifacts = service::handle_add_artifacts(&ctx, stream).await?;
        let response = AddArtifactsResponse { artifacts };
        debug!("{:?}", response);
        Ok(Response::new(response))
    }

    async fn artifact_status(
        &self,
        request: Request<ArtifactStatusesRequest>,
    ) -> Result<Response<ArtifactStatusesResponse>, Status> {
        let request = request.into_inner();
        debug!("{:?}", request);
        let session_key = SessionKey {
            user_id: request.user_context.map(|u| u.user_id),
            session_id: request.session_id.clone(),
        };
        let ctx = self
            .session_manager
            .get_or_create_session_context(session_key)
            .await?;
        let statuses = service::handle_artifact_statuses(&ctx, request.names).await?;
        let response = ArtifactStatusesResponse { statuses };
        debug!("{:?}", response);
        Ok(Response::new(response))
    }

    async fn interrupt(
        &self,
        request: Request<InterruptRequest>,
    ) -> Result<Response<InterruptResponse>, Status> {
        let request = request.into_inner();
        debug!("{:?}", request);
        let session_key = SessionKey {
            user_id: request.user_context.map(|u| u.user_id),
            session_id: request.session_id.clone(),
        };
        let ctx = self
            .session_manager
            .get_or_create_session_context(session_key)
            .await?;
        let ids = match InterruptType::try_from(request.interrupt_type) {
            Ok(InterruptType::All) => Ok(service::handle_interrupt_all(&ctx).await?),
            Ok(InterruptType::Tag) => {
                if let Some(Interrupt::OperationTag(tag)) = request.interrupt {
                    Ok(service::handle_interrupt_tag(&ctx, tag).await?)
                } else {
                    Err(Status::invalid_argument("operation tag is required"))
                }
            }
            Ok(InterruptType::OperationId) => {
                if let Some(Interrupt::OperationId(id)) = request.interrupt {
                    Ok(service::handle_interrupt_operation_id(&ctx, id).await?)
                } else {
                    Err(Status::invalid_argument("operation ID is required"))
                }
            }
            Ok(InterruptType::Unspecified) | Err(_) => Err(Status::invalid_argument(
                "a valid interrupt type is required",
            )),
        };
        let response = InterruptResponse {
            session_id: request.session_id.clone(),
            interrupted_ids: ids?,
        };
        debug!("{:?}", response);
        Ok(Response::new(response))
    }

    type ReattachExecuteStream = ExecutePlanResponseStream;

    async fn reattach_execute(
        &self,
        request: Request<ReattachExecuteRequest>,
    ) -> Result<Response<Self::ReattachExecuteStream>, Status> {
        let request = request.into_inner();
        debug!("{:?}", request);
        let session_key = SessionKey {
            user_id: request.user_context.map(|u| u.user_id),
            session_id: request.session_id.clone(),
        };
        let ctx = self
            .session_manager
            .get_or_create_session_context(session_key)
            .await?;
        let stream =
            service::handle_reattach_execute(&ctx, request.operation_id, request.last_response_id)
                .await?;
        Ok(Response::new(stream))
    }

    async fn release_execute(
        &self,
        request: Request<ReleaseExecuteRequest>,
    ) -> Result<Response<ReleaseExecuteResponse>, Status> {
        let request = request.into_inner();
        debug!("{:?}", request);
        let session_key = SessionKey {
            user_id: request.user_context.map(|u| u.user_id),
            session_id: request.session_id.clone(),
        };
        let ctx = self
            .session_manager
            .get_or_create_session_context(session_key)
            .await?;
        let response_id = match request.release.required("release")? {
            Release::ReleaseAll(ReleaseAll {}) => None,
            Release::ReleaseUntil(ReleaseUntil { response_id }) => Some(response_id),
        };
        service::handle_release_execute(&ctx, request.operation_id.clone(), response_id).await?;
        let response = ReleaseExecuteResponse {
            session_id: request.session_id.clone(),
            operation_id: Some(request.operation_id),
        };
        debug!("{:?}", response);
        Ok(Response::new(response))
    }
}
