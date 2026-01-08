use datafusion::prelude::SessionContext;
use fastrace::collector::SpanContext;
use fastrace::Span;
use sail_common::config::AppConfig;
use sail_common::runtime::RuntimeHandle;
use sail_server::actor::ActorSystem;
use sail_telemetry::common::{ContextPropagationEnv, SpanAttribute};

use crate::error::ExecutionError;
use crate::worker::{WorkerActor, WorkerOptions};

pub async fn run_worker(
    config: &AppConfig,
    runtime: RuntimeHandle,
    session: SessionContext,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut system = ActorSystem::new();
    let options = WorkerOptions::new(config, runtime, session);
    let span = match std::env::var(ContextPropagationEnv::TRACEPARENT) {
        Ok(x) => {
            let Some(span_context) = SpanContext::decode_w3c_traceparent(&x) else {
                return Err(Box::new(ExecutionError::InvalidArgument(format!(
                    "traceparent: {x}"
                ))));
            };
            Span::root("worker", span_context).with_property(|| {
                (
                    SpanAttribute::CLUSTER_WORKER_ID,
                    options.worker_id.to_string(),
                )
            })
        }
        Err(std::env::VarError::NotPresent) => Span::noop(),
        Err(e) => {
            return Err(Box::new(e));
        }
    };
    let _handle = {
        let _guard = span.set_local_parent();
        system.spawn::<WorkerActor>(options)
    };
    system.join().await;
    Ok(())
}
