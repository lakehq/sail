use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use fastrace::prelude::*;
use tonic::codegen::http::Request;
use tower::{Layer, Service};

#[derive(Clone)]
pub struct TraceLayer {
    name: &'static str, // TODO: Arc needed?
}

impl TraceLayer {
    pub fn new(name: &'static str) -> Self {
        Self { name }
    }
}

impl<S> Layer<S> for TraceLayer {
    type Service = TraceService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        TraceService {
            inner,
            name: self.name,
        }
    }
}

#[derive(Clone)]
pub struct TraceService<S> {
    inner: S,
    name: &'static str, // TODO: Arc needed?
}

impl<S, R> Service<Request<R>> for TraceService<S>
where
    S: Service<Request<R>> + Send + 'static,
    S::Future: Send + 'static,
{
    type Response = S::Response;

    type Error = S::Error;

    type Future =
        Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send + 'static>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: Request<R>) -> Self::Future {
        Box::pin(
            self.inner
                .call(request)
                .in_span(Span::root(self.name, SpanContext::random())),
        )
    }
}
