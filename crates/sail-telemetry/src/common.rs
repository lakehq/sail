/// The environment variables used for context propagation.
/// This follows the OpenTelemetry specification.
///
/// Reference: <https://opentelemetry.io/docs/specs/otel/context/env-carriers/>
pub struct ContextPropagationEnv;

impl ContextPropagationEnv {
    pub const TRACEPARENT: &'static str = "TRACEPARENT";
}
