use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

// use fastrace::future::FutureExt;
use fastrace::prelude::*;
use log::error;
use tonic::codegen::http::{HeaderMap, HeaderValue, Request};
use tower::{Layer, Service};

use crate::error::TelemetryResult;

pub const HTTP_HEADER_TRACE_PARENT: &str = "traceparent";

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

    // fn call(&mut self, mut request: Request<R>) -> Self::Future {
    //     match span_context_from_headers(&request.headers()) {
    //         Ok(Some(span_context)) => {
    //             request.extensions_mut().insert(span_context);
    //         }
    //         Err(error) => {
    //             error!(
    //                 "Failed to extract span context from headers for {} {error}",
    //                 self.name
    //             );
    //         }
    //         _ => {
    //             let span_context = SpanContext::random();
    //             let _ = HeaderValue::from_str(&span_context.encode_w3c_traceparent())
    //                 .map(|header_value| {
    //                     request
    //                         .headers_mut()
    //                         .insert(HTTP_HEADER_TRACE_PARENT, header_value);
    //                 })
    //                 .map_err(|error| {
    //                     error!("Failed to encode span context for {} {error}", self.name);
    //                 });
    //             request.extensions_mut().insert(span_context);
    //         }
    //     };
    //
    //     Box::pin(self.inner.call(request))
    //     // self.inner.call(request)
    // }

    fn call(&mut self, mut request: Request<R>) -> Self::Future {
        let span_context = match &request.headers().get(HTTP_HEADER_TRACE_PARENT) {
            Some(value) => {
                value
                    .to_str()
                    .map(|str_value| {
                        // println!("CHECK HERE: MADE IT TO THE TRACE LAYER");
                        SpanContext::decode_w3c_traceparent(str_value).unwrap_or({
                            // println!("CHECK HERE: FAILED DECODE");
                            error!("Failed to decode trace header value for {}", self.name);
                            SpanContext::random()
                        })
                    })
                    .map_err(|error| {
                        // println!("CHECK HERE: FAILED CONVERT");
                        error!(
                            "Failed covert trace header value to string for {} {error}",
                            self.name
                        );
                    })
            }
            None => {
                let span_context = SpanContext::random();
                HeaderValue::from_str(&span_context.encode_w3c_traceparent())
                    .map(|header_value| {
                        // println!("CHECK HERE: GOOD ENCODE");
                        request
                            .headers_mut()
                            .insert(HTTP_HEADER_TRACE_PARENT, header_value);
                        span_context
                    })
                    .map_err(|error| {
                        // println!("CHECK HERE: FAILED encode");
                        error!("Failed to encode span context for {} {error}", self.name);
                    })
            }
        };

        if let Ok(span_context) = span_context {
            // println!("CHECK HERE: SPAN CONTEXT");
            let root_span = Span::root(self.name, span_context);
            let future = self.inner.call(request).in_span(root_span);
            // .in_span(Span::enter_with_parent("Task", &root_span));
            Box::pin(future)
        } else {
            // println!("CHECK HERE: NO SPAN CONTEXT");
            let future = self.inner.call(request);
            Box::pin(future)
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
