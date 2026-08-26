use async_stream;
use datafusion::prelude::SessionContext;
use log::debug;
use sail_session::session_manager::SessionManager;
use tonic::codegen::tokio_stream::StreamExt;
use tonic::{Request, Response, Status, Streaming};
use uuid::Uuid;

use crate::error::{ProtoFieldExt, SparkError, SparkResult};
use crate::executor::ExecutorMetadata;
use crate::service;
use crate::service::ExecutePlanResponseStream;
use crate::spark::connect::analyze_plan_request::Analyze;
use crate::spark::connect::interrupt_request::{Interrupt, InterruptType};
use crate::spark::connect::release_execute_request::{Release, ReleaseAll, ReleaseUntil};
use crate::spark::connect::spark_connect_service_server::SparkConnectService;
use crate::spark::connect::{
    AddArtifactsRequest, AddArtifactsResponse, AnalyzePlanRequest, AnalyzePlanResponse,
    ArtifactStatusesRequest, ArtifactStatusesResponse, CloneSessionRequest, CloneSessionResponse,
    Command, ConfigRequest, ConfigResponse, ExecutePlanRequest, FetchErrorDetailsRequest,
    FetchErrorDetailsResponse, GetStatusRequest, GetStatusResponse, InterruptRequest,
    InterruptResponse, Plan, ReattachExecuteRequest, ReleaseExecuteRequest, ReleaseExecuteResponse,
    ReleaseSessionRequest, ReleaseSessionResponse, config_request, plan,
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

fn is_reattachable(
    request_options: &[crate::spark::connect::execute_plan_request::RequestOption],
) -> bool {
    use crate::spark::connect::execute_plan_request::request_option::RequestOption;

    for item in request_options {
        if let Some(RequestOption::ReattachOptions(v)) = &item.request_option {
            return v.reattachable;
        }
    }
    false
}

fn resolve_clone_session_id(session_id: Option<String>) -> SparkResult<String> {
    let session_id = session_id
        .filter(|id| !id.is_empty())
        .unwrap_or_else(|| Uuid::new_v4().to_string());
    Uuid::parse_str(&session_id)
        .map_err(|_| SparkError::invalid("target session ID must be a UUID"))?;
    Ok(session_id)
}

/// Utility function to handle execution of a command by routing it to the appropriate handler.
/// Still has some CommandTypes that are not implemented.
async fn handle_command(
    ctx: &SessionContext,
    command: crate::spark::connect::command::CommandType,
    metadata: ExecutorMetadata,
) -> SparkResult<ExecutePlanResponseStream> {
    use crate::spark::connect::command::CommandType;

    match command {
        CommandType::RegisterFunction(udf) => {
            service::handle_execute_register_function(ctx, udf, metadata).await
        }
        CommandType::WriteOperation(write) => {
            service::handle_execute_write_operation(ctx, write, metadata).await
        }
        CommandType::CreateDataframeView(view) => {
            service::handle_execute_create_dataframe_view(ctx, view, metadata).await
        }
        CommandType::WriteOperationV2(write) => {
            service::handle_execute_write_operation_v2(ctx, write, metadata).await
        }
        CommandType::SqlCommand(sql) => {
            service::handle_execute_sql_command(ctx, sql, metadata).await
        }
        CommandType::WriteStreamOperationStart(start) => {
            service::handle_execute_write_stream_operation_start(ctx, start, metadata).await
        }
        CommandType::StreamingQueryCommand(stream) => {
            service::handle_execute_streaming_query_command(ctx, stream, metadata).await
        }
        CommandType::GetResourcesCommand(resource) => {
            service::handle_execute_get_resources_command(ctx, resource, metadata).await
        }
        CommandType::StreamingQueryManagerCommand(command) => {
            service::handle_execute_streaming_query_manager_command(ctx, command, metadata).await
        }
        CommandType::RegisterTableFunction(udtf) => {
            service::handle_execute_register_table_function(ctx, udtf, metadata).await
        }
        CommandType::StreamingQueryListenerBusCommand(command) => {
            service::handle_execute_streaming_query_listener_bus_command(ctx, command, metadata)
                .await
        }
        CommandType::RegisterDataSource(ds) => {
            service::handle_execute_register_datasource(ctx, ds, metadata).await
        }
        CommandType::CreateResourceProfileCommand(_) => {
            Err(SparkError::todo("create resource profile command"))
        }
        CommandType::CheckpointCommand(checkpoint) => {
            service::handle_execute_checkpoint_command(ctx, checkpoint, metadata).await
        }
        CommandType::RemoveCachedRemoteRelationCommand(command) => {
            service::handle_execute_remove_cached_remote_relation_command(ctx, command, metadata)
                .await
        }
        CommandType::MergeIntoTableCommand(command) => {
            service::handle_execute_merge_into_table_command(ctx, command, metadata).await
        }
        CommandType::MlCommand(_) => Err(SparkError::todo("ml command")),
        CommandType::ExecuteExternalCommand(_) => Err(SparkError::todo("execute external command")),
        CommandType::PipelineCommand(_) => Err(SparkError::todo("pipeline command")),
        CommandType::Extension(_) => Err(SparkError::todo("command extension")),
    }
}

// TODO: make sure that `server_side_session_id` is set properly

#[tonic::async_trait]
impl SparkConnectService for SparkConnectServer {
    type ExecutePlanStream = ExecutePlanResponseStream;

    async fn execute_plan(
        &self,
        request: Request<ExecutePlanRequest>,
    ) -> Result<Response<Self::ExecutePlanStream>, Status> {
        let request = request.into_inner();
        debug!("{request:?}");
        let session_id = request.session_id;
        let user_id = request.user_context.map(|u| u.user_id).unwrap_or_default();
        let metadata = ExecutorMetadata {
            operation_id: request
                .operation_id
                .unwrap_or_else(|| Uuid::new_v4().to_string()),
            tags: request.tags,
            reattachable: is_reattachable(&request.request_options),
        };
        let ctx = self
            .session_manager
            .get_or_create_session_context(session_id, user_id)
            .await
            .map_err(SparkError::from)?;
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
                handle_command(&ctx, command, metadata).await?
            }
            plan::OpType::CompressedOperation(_) => {
                return Err(Status::unimplemented("compressed operation plan"));
            }
        };
        Ok(Response::new(stream))
    }

    async fn analyze_plan(
        &self,
        request: Request<AnalyzePlanRequest>,
    ) -> Result<Response<AnalyzePlanResponse>, Status> {
        use crate::spark::connect::analyze_plan_response;

        let request = request.into_inner();
        debug!("{request:?}");
        let session_id = request.session_id.clone();
        let user_id = request.user_context.map(|u| u.user_id).unwrap_or_default();
        let ctx = self
            .session_manager
            .get_or_create_session_context(session_id, user_id)
            .await
            .map_err(SparkError::from)?;
        let analyze = request.analyze.required("analyze")?;
        let result = match analyze {
            Analyze::Schema(schema) => {
                let schema = service::handle_analyze_schema(&ctx, schema).await?;
                Some(analyze_plan_response::Result::Schema(schema))
            }
            Analyze::Explain(explain) => {
                let explain = service::handle_analyze_explain(&ctx, explain).await?;
                Some(analyze_plan_response::Result::Explain(explain))
            }
            Analyze::TreeString(tree) => {
                let tree = service::handle_analyze_tree_string(&ctx, tree).await?;
                Some(analyze_plan_response::Result::TreeString(tree))
            }
            Analyze::IsLocal(local) => {
                let local = service::handle_analyze_is_local(&ctx, local).await?;
                Some(analyze_plan_response::Result::IsLocal(local))
            }
            Analyze::IsStreaming(streaming) => {
                let streaming = service::handle_analyze_is_streaming(&ctx, streaming).await?;
                Some(analyze_plan_response::Result::IsStreaming(streaming))
            }
            Analyze::InputFiles(input) => {
                let input = service::handle_analyze_input_files(&ctx, input).await?;
                Some(analyze_plan_response::Result::InputFiles(input))
            }
            Analyze::SparkVersion(version) => {
                let version = service::handle_analyze_spark_version(&ctx, version).await?;
                Some(analyze_plan_response::Result::SparkVersion(version))
            }
            Analyze::DdlParse(ddl) => {
                let ddl = service::handle_analyze_ddl_parse(&ctx, ddl).await?;
                Some(analyze_plan_response::Result::DdlParse(ddl))
            }
            Analyze::SameSemantics(same) => {
                let same = service::handle_analyze_same_semantics(&ctx, same).await?;
                Some(analyze_plan_response::Result::SameSemantics(same))
            }
            Analyze::SemanticHash(hash) => {
                let hash = service::handle_analyze_semantic_hash(&ctx, hash).await?;
                Some(analyze_plan_response::Result::SemanticHash(hash))
            }
            Analyze::Persist(persist) => {
                let persist = service::handle_analyze_persist(&ctx, persist).await?;
                Some(analyze_plan_response::Result::Persist(persist))
            }
            Analyze::Unpersist(unpersist) => {
                let unpersist = service::handle_analyze_unpersist(&ctx, unpersist).await?;
                Some(analyze_plan_response::Result::Unpersist(unpersist))
            }
            Analyze::GetStorageLevel(level) => {
                let level = service::handle_analyze_get_storage_level(&ctx, level).await?;
                Some(analyze_plan_response::Result::GetStorageLevel(level))
            }
            Analyze::JsonToDdl(json) => {
                let json = service::handle_analyze_json_to_ddl(&ctx, json).await?;
                Some(analyze_plan_response::Result::JsonToDdl(json))
            }
        };
        let response = AnalyzePlanResponse {
            session_id: request.session_id.clone(),
            server_side_session_id: request.session_id,
            result,
        };
        debug!("{response:?}");
        Ok(Response::new(response))
    }

    async fn config(
        &self,
        request: Request<ConfigRequest>,
    ) -> Result<Response<ConfigResponse>, Status> {
        use crate::spark::connect::config_request::operation::OpType;

        let request = request.into_inner();
        debug!("{request:?}");
        let session_id = request.session_id.clone();
        let user_id = request.user_context.map(|u| u.user_id).unwrap_or_default();
        let ctx = self
            .session_manager
            .get_or_create_session_context(session_id, user_id)
            .await
            .map_err(SparkError::from)?;
        let config_request::Operation { op_type: op } = request.operation.required("operation")?;
        let op = op.required("operation type")?;
        let response = match op {
            OpType::Get(config_request::Get { keys }) => service::handle_config_get(&ctx, keys)?,
            OpType::Set(config_request::Set { pairs, silent: _ }) => {
                // TODO: ignore errors if `silent` is true
                service::handle_config_set(&ctx, pairs)?
            }
            OpType::GetWithDefault(config_request::GetWithDefault { pairs }) => {
                service::handle_config_get_with_default(&ctx, pairs)?
            }
            OpType::GetOption(config_request::GetOption { keys }) => {
                service::handle_config_get_option(&ctx, keys)?
            }
            OpType::GetAll(config_request::GetAll { prefix }) => {
                service::handle_config_get_all(&ctx, prefix)?
            }
            OpType::Unset(config_request::Unset { keys }) => {
                service::handle_config_unset(&ctx, keys)?
            }
            OpType::IsModifiable(config_request::IsModifiable { keys }) => {
                service::handle_config_is_modifiable(&ctx, keys)?
            }
        };
        debug!("{response:?}");
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
        debug!("{first:?}");
        let session_id = first.session_id.clone();
        let user_id = first.user_context.map(|u| u.user_id).unwrap_or_default();
        let ctx = self
            .session_manager
            .get_or_create_session_context(session_id.clone(), user_id)
            .await
            .map_err(SparkError::from)?;
        let payload = first.payload;
        let stream = async_stream::try_stream! {
            if let Some(payload) = payload {
                yield payload;
            }
            while let Some(item) = request.next().await {
                let item = item?;
                debug!("{item:?}");
                if item.session_id != session_id {
                    Err(Status::invalid_argument("session ID must be consistent"))?;
                }
                if let Some(payload) = item.payload {
                    yield payload;
                }
            }
        };
        let artifacts = service::handle_add_artifacts(&ctx, stream).await?;
        let response = AddArtifactsResponse {
            session_id: first.session_id.clone(),
            server_side_session_id: first.session_id,
            artifacts,
        };
        debug!("{response:?}");
        Ok(Response::new(response))
    }

    async fn artifact_status(
        &self,
        request: Request<ArtifactStatusesRequest>,
    ) -> Result<Response<ArtifactStatusesResponse>, Status> {
        let request = request.into_inner();
        debug!("{request:?}");
        let session_id = request.session_id.clone();
        let user_id = request.user_context.map(|u| u.user_id).unwrap_or_default();
        let ctx = self
            .session_manager
            .get_or_create_session_context(session_id, user_id)
            .await
            .map_err(SparkError::from)?;
        let statuses = service::handle_artifact_statuses(&ctx, request.names).await?;
        let response = ArtifactStatusesResponse {
            session_id: request.session_id.clone(),
            server_side_session_id: request.session_id,
            statuses,
        };
        debug!("{response:?}");
        Ok(Response::new(response))
    }

    async fn interrupt(
        &self,
        request: Request<InterruptRequest>,
    ) -> Result<Response<InterruptResponse>, Status> {
        let request = request.into_inner();
        debug!("{request:?}");
        let session_id = request.session_id.clone();
        let user_id = request.user_context.map(|u| u.user_id).unwrap_or_default();
        let ctx = self
            .session_manager
            .get_or_create_session_context(session_id, user_id)
            .await
            .map_err(SparkError::from)?;
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
            server_side_session_id: request.session_id,
            interrupted_ids: ids?,
        };
        debug!("{response:?}");
        Ok(Response::new(response))
    }

    type ReattachExecuteStream = ExecutePlanResponseStream;

    async fn reattach_execute(
        &self,
        request: Request<ReattachExecuteRequest>,
    ) -> Result<Response<Self::ReattachExecuteStream>, Status> {
        let request = request.into_inner();
        debug!("{request:?}");
        let session_id = request.session_id;
        let user_id = request.user_context.map(|u| u.user_id).unwrap_or_default();
        let ctx = self
            .session_manager
            .get_or_create_session_context(session_id, user_id)
            .await
            .map_err(SparkError::from)?;
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
        debug!("{request:?}");
        let session_id = request.session_id.clone();
        let user_id = request.user_context.map(|u| u.user_id).unwrap_or_default();
        let ctx = self
            .session_manager
            .get_or_create_session_context(session_id, user_id)
            .await
            .map_err(SparkError::from)?;
        let response_id = match request.release.required("release")? {
            Release::ReleaseAll(ReleaseAll {}) => None,
            Release::ReleaseUntil(ReleaseUntil { response_id }) => Some(response_id),
        };
        service::handle_release_execute(&ctx, request.operation_id.clone(), response_id).await?;
        let response = ReleaseExecuteResponse {
            session_id: request.session_id.clone(),
            server_side_session_id: request.session_id,
            operation_id: Some(request.operation_id),
        };
        debug!("{response:?}");
        Ok(Response::new(response))
    }

    async fn release_session(
        &self,
        request: Request<ReleaseSessionRequest>,
    ) -> Result<Response<ReleaseSessionResponse>, Status> {
        let request = request.into_inner();
        debug!("{request:?}");
        if request.allow_reconnect {
            Err(SparkError::unsupported("reconnect session"))?;
        }
        let session_id = request.session_id.clone();
        let _user_id = request.user_context.map(|u| u.user_id).unwrap_or_default();
        self.session_manager
            .delete_session(session_id)
            .await
            .map_err(SparkError::from)?;
        let response = ReleaseSessionResponse {
            session_id: request.session_id.clone(),
            server_side_session_id: request.session_id,
        };
        debug!("{response:?}");
        Ok(Response::new(response))
    }

    async fn fetch_error_details(
        &self,
        request: Request<FetchErrorDetailsRequest>,
    ) -> Result<Response<FetchErrorDetailsResponse>, Status> {
        let request = request.into_inner();
        debug!("{request:?}");
        Err(Status::unimplemented("fetch error details"))
    }

    async fn clone_session(
        &self,
        request: Request<CloneSessionRequest>,
    ) -> Result<Response<CloneSessionResponse>, Status> {
        let request = request.into_inner();
        debug!("{request:?}");
        let source_session_id = request.session_id.clone();
        let user_id = request.user_context.map(|u| u.user_id).unwrap_or_default();
        let target_session_id = resolve_clone_session_id(request.new_session_id)?;
        let observed_session_id = request
            .client_observed_server_side_session_id
            .filter(|id| !id.is_empty());
        self.session_manager
            .clone_session_context(
                source_session_id.clone(),
                target_session_id.clone(),
                user_id,
                observed_session_id,
            )
            .await
            .map_err(SparkError::from)?;
        let response = CloneSessionResponse {
            session_id: source_session_id.clone(),
            server_side_session_id: source_session_id,
            new_session_id: target_session_id.clone(),
            new_server_side_session_id: target_session_id,
        };
        debug!("{response:?}");
        Ok(Response::new(response))
    }

    async fn get_status(
        &self,
        request: Request<GetStatusRequest>,
    ) -> Result<Response<GetStatusResponse>, Status> {
        debug!("{:?}", request.into_inner());
        Err(Status::unimplemented("get status"))
    }
}

#[cfg(test)]
mod tests {
    #![expect(clippy::unwrap_used)]

    use std::sync::Arc;
    use std::time::Duration;

    use futures::future::join_all;
    use pyo3::Python;
    use sail_common::actor::ActorSystem;
    use sail_common::config::{AppConfig, ExecutionMode};
    use sail_common::runtime::RuntimeManager;
    use sail_common_datafusion::extension::SessionExtensionAccessor;
    use sail_common_datafusion::session::activity::ActivityTracker;

    use super::*;
    use crate::session_manager::{create_spark_session_manager, init_test_telemetry};
    use crate::spark::connect::{ReleaseSessionRequest, UserContext};

    fn clone_request(
        source: &str,
        target: Option<String>,
        observed: Option<String>,
    ) -> CloneSessionRequest {
        CloneSessionRequest {
            session_id: source.to_string(),
            client_observed_server_side_session_id: observed,
            user_context: Some(UserContext {
                user_id: "user".to_string(),
                user_name: String::new(),
                extensions: vec![],
            }),
            client_type: None,
            new_session_id: target,
        }
    }

    #[test]
    fn clone_session_treats_empty_target_as_generated() {
        let id = resolve_clone_session_id(Some(String::new())).unwrap();
        assert!(Uuid::parse_str(&id).is_ok());
    }

    #[test]
    fn clone_session_rpc_enforces_identity_and_target_lifecycle() {
        Python::initialize();
        let mut config = AppConfig::load().unwrap();
        config.mode = ExecutionMode::Local;
        let config = Arc::new(config);
        let runtime = RuntimeManager::try_new(&config.runtime).unwrap();
        let handle = runtime.handle();
        handle
            .primary()
            .block_on(async { init_test_telemetry(&config) });
        let mut system = ActorSystem::new();
        let manager = handle
            .primary()
            .block_on(create_spark_session_manager(
                config,
                handle.clone(),
                &mut system,
            ))
            .unwrap();
        let server = SparkConnectServer::new(manager.clone());
        let source_id = Uuid::new_v4().to_string();
        let clone_id = Uuid::new_v4().to_string();

        handle.primary().block_on(async {
            let unknown = Uuid::new_v4().to_string();
            let error = server
                .clone_session(Request::new(clone_request(
                    &unknown,
                    Some(Uuid::new_v4().to_string()),
                    None,
                )))
                .await
                .unwrap_err();
            assert!(error.message().contains("session not found"));

            let source = manager
                .get_or_create_session_context(source_id.clone(), "user".to_string())
                .await
                .unwrap();
            let source_activity = source.extension::<ActivityTracker>().unwrap();
            let source_active_at = source_activity.active_at().unwrap();

            let error = server
                .clone_session(Request::new(clone_request(
                    &source_id,
                    Some(Uuid::new_v4().to_string()),
                    Some(Uuid::new_v4().to_string()),
                )))
                .await
                .unwrap_err();
            assert!(error.message().contains("client-observed"));

            let mut wrong_user = clone_request(&source_id, Some(Uuid::new_v4().to_string()), None);
            wrong_user.user_context.as_mut().unwrap().user_id = "other".to_string();
            let error = server
                .clone_session(Request::new(wrong_user))
                .await
                .unwrap_err();
            assert!(error.message().contains("session not found"));

            let foreign_target = Uuid::new_v4().to_string();
            manager
                .get_or_create_session_context(foreign_target.clone(), "other".to_string())
                .await
                .unwrap();
            let error = server
                .clone_session(Request::new(clone_request(
                    &source_id,
                    Some(foreign_target),
                    None,
                )))
                .await
                .unwrap_err();
            assert!(error.message().contains("not available"));
            assert!(!error.message().contains("already exists"));

            let response = server
                .clone_session(Request::new(clone_request(
                    &source_id,
                    Some(clone_id.clone()),
                    Some(source_id.clone()),
                )))
                .await
                .unwrap()
                .into_inner();
            assert!(source_activity.active_at().unwrap() > source_active_at);
            assert_eq!(response.session_id, source_id);
            assert_eq!(response.server_side_session_id, source_id);
            assert_eq!(response.new_session_id, clone_id);
            assert_eq!(response.new_server_side_session_id, clone_id);

            let error = server
                .clone_session(Request::new(clone_request(
                    &source_id,
                    Some(clone_id.clone()),
                    None,
                )))
                .await
                .unwrap_err();
            assert!(error.message().contains("already exists"));

            let concurrent_id = Uuid::new_v4().to_string();
            let responses = join_all((0..8).map(|_| {
                server.clone_session(Request::new(clone_request(
                    &source_id,
                    Some(concurrent_id.clone()),
                    None,
                )))
            }))
            .await;
            assert_eq!(responses.iter().filter(|result| result.is_ok()).count(), 1);
            assert!(
                responses
                    .iter()
                    .filter_map(|result| result.as_ref().err())
                    .all(|error| error.message().contains("already exists"))
            );

            server
                .release_session(Request::new(ReleaseSessionRequest {
                    session_id: clone_id,
                    user_context: None,
                    client_type: None,
                    allow_reconnect: false,
                }))
                .await
                .unwrap();
            assert!(
                manager
                    .get_or_create_session_context(source_id.clone(), "user".to_string())
                    .await
                    .is_ok()
            );

            let error = server
                .clone_session(Request::new(clone_request(
                    &source_id,
                    Some(response.new_session_id),
                    None,
                )))
                .await
                .unwrap_err();
            assert!(error.message().contains("previously closed"));

            manager.shutdown().await.unwrap();
        });
        handle.primary().block_on(system.join());
    }

    #[test]
    fn cloned_session_uses_normal_timeout_cleanup() {
        Python::initialize();
        let mut config = AppConfig::load().unwrap();
        config.mode = ExecutionMode::Local;
        config.spark.session_timeout_secs = 1;
        let config = Arc::new(config);
        let runtime = RuntimeManager::try_new(&config.runtime).unwrap();
        let handle = runtime.handle();
        handle
            .primary()
            .block_on(async { init_test_telemetry(&config) });
        let mut system = ActorSystem::new();
        let manager = handle
            .primary()
            .block_on(create_spark_session_manager(
                config,
                handle.clone(),
                &mut system,
            ))
            .unwrap();
        let server = SparkConnectServer::new(manager.clone());
        let source_id = Uuid::new_v4().to_string();
        let clone_id = Uuid::new_v4().to_string();

        handle.primary().block_on(async {
            manager
                .get_or_create_session_context(source_id.clone(), "user".to_string())
                .await
                .unwrap();
            server
                .clone_session(Request::new(clone_request(
                    &source_id,
                    Some(clone_id.clone()),
                    None,
                )))
                .await
                .unwrap();

            tokio::time::sleep(Duration::from_millis(1200)).await;

            assert!(
                manager
                    .get_or_create_session_context(source_id, "user".to_string())
                    .await
                    .err()
                    .unwrap()
                    .to_string()
                    .contains("not running")
            );
            assert!(
                manager
                    .get_or_create_session_context(clone_id, "user".to_string())
                    .await
                    .err()
                    .unwrap()
                    .to_string()
                    .contains("not running")
            );
            manager.shutdown().await.unwrap();
        });
        handle.primary().block_on(system.join());
    }
}
