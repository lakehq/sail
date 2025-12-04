use std::task::{Context, Poll};

use datafusion::object_store::HeaderValue;
use fastrace::collector::SpanContext;
use fastrace::future::{FutureExt, InSpan};
use fastrace::Span;
use tonic::codegen::http::Request;
use tonic::codegen::Service;
use tower::Layer;

use crate::common::{ContextPropagationHeader, SpanAttribute, SpanKind};

#[derive(Clone)]
pub struct TracingClientLayer;

impl<S> Layer<S> for TracingClientLayer {
    type Service = TracingClientService<S>;

    fn layer(&self, service: S) -> Self::Service {
        TracingClientService { inner: service }
    }
}

#[derive(Clone)]
pub struct TracingClientService<S> {
    inner: S,
}

impl<S, Body> Service<Request<Body>> for TracingClientService<S>
where
    S: Service<Request<Body>>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = InSpan<S::Future>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut req: Request<Body>) -> Self::Future {
        let span = Span::enter_with_local_parent(format!("{} {}", req.method(), req.uri().path()))
            .with_properties(|| [(SpanAttribute::SPAN_KIND, SpanKind::CLIENT)]);
        if let Some(current) = SpanContext::from_span(&span) {
            if let Ok(value) = HeaderValue::from_str(&current.encode_w3c_traceparent()) {
                req.headers_mut()
                    .insert(ContextPropagationHeader::TRACEPARENT, value);
            }
        }

        self.inner.call(req).in_span(span)
    }
}
