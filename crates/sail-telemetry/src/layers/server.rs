use std::task::{Context, Poll};

use fastrace::Span;
use fastrace::collector::SpanContext;
use fastrace::future::{FutureExt, InSpan};
use tonic::codegen::http::Request;
use tonic::server::NamedService;
use tower::{Layer, Service};

use crate::common::{ContextPropagationHeader, SpanAttribute, SpanKind};

#[derive(Clone)]
pub struct TracingServerLayer;

impl<S> Layer<S> for TracingServerLayer {
    type Service = TracingServerService<S>;

    fn layer(&self, service: S) -> Self::Service {
        TracingServerService { inner: service }
    }
}

#[derive(Clone)]
pub struct TracingServerService<S> {
    inner: S,
}

impl<S: NamedService> NamedService for TracingServerService<S> {
    const NAME: &'static str = S::NAME;
}

impl<S, Body> Service<Request<Body>> for TracingServerService<S>
where
    S: Service<Request<Body>>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = InSpan<S::Future>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request<Body>) -> Self::Future {
        let parent = req
            .headers()
            .get(ContextPropagationHeader::TRACEPARENT)
            .and_then(|x| SpanContext::decode_w3c_traceparent(x.to_str().ok()?));

        let span = if let Some(parent) = parent {
            Span::root(req.uri().path().to_string(), parent)
                .with_properties(|| [(SpanAttribute::SPAN_KIND, SpanKind::SERVER)])
        } else {
            Span::noop()
        };

        self.inner.call(req).in_span(span)
    }
}
