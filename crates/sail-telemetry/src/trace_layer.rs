use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use fastrace::future::FutureExt;
use fastrace::prelude::*;
use log::error;
use tonic::codegen::http::{HeaderMap, HeaderValue, Request};
use tower::{Layer, Service};

use crate::error::TelemetryResult;

pub const HTTP_HEADER_TRACE_PARENT: &str = "traceparent";

#[derive(Debug, Clone)]
pub struct TraceLayer {
    name: &'static str, // TODO: Arc needed?
}

impl TraceLayer {
    pub fn new(name: &'static str) -> Self {
        Self { name: name.into() }
    }
}

impl<S> Layer<S> for TraceLayer {
    type Service = TraceService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        TraceService {
            inner,
            name: &self.name,
        }
    }
}

#[derive(Debug, Clone)]
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

    fn call(&mut self, mut request: Request<R>) -> Self::Future {
        let headers = request.headers_mut();
        let span_context = match span_context_from_headers(headers) {
            Ok(Some(span_context)) => {
                Some(span_context)
                // request.extensions_mut().insert(span_context);
            }
            other => {
                match other {
                    Err(error) => {
                        error!("Failed to extract span context for {} {error}", self.name);
                    }
                    _ => {}
                }
                let span_context = SpanContext::random();
                HeaderValue::from_str(&span_context.encode_w3c_traceparent())
                    .map(|header_value| {
                        headers.insert(HTTP_HEADER_TRACE_PARENT, header_value);
                        Some(span_context)
                    })
                    .map_err(|error| {
                        error!("Failed to set span context for {} {error}", self.name);
                    })
                    .unwrap_or(None)
            }
        };

        match span_context {
            Some(span_context) => {
                let root_span = Span::root(self.name, span_context);
                let future = self.inner.call(request).in_span(root_span);
                Box::pin(future)
            }
            None => {
                let future = self.inner.call(request);
                Box::pin(future)
            }
        }
    }
}

pub fn span_context_from_headers(headers: &HeaderMap) -> TelemetryResult<Option<SpanContext>> {
    if let Some(value) = headers.get(HTTP_HEADER_TRACE_PARENT) {
        Ok(SpanContext::decode_w3c_traceparent(value.to_str()?))
    } else {
        Ok(None)
    }
}

pub fn append_span_context(header: &mut HeaderMap) -> TelemetryResult<()> {
    if let Some(span_context) = SpanContext::current_local_parent() {
        header.insert(
            HTTP_HEADER_TRACE_PARENT,
            HeaderValue::from_str(&span_context.encode_w3c_traceparent())?,
        );
    }
    Ok(())
}
