use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use fastrace::future::FutureExt;
use fastrace::prelude::*;
use log::{debug, error};
use tonic::codegen::http::{HeaderMap, HeaderValue, Request};
use tonic::transport::Body;
use tower::{Layer, Service};

use crate::error::TelemetryResult;

pub const HTTP_HEADER_TRACE_PARENT: &str = "traceparent";

// #[derive(Debug, Clone)]
// pub struct TraceLayer {
//     name: &'static str, // TODO: Arc needed?
// }
// impl TraceLayer {
//     pub fn new(name: &'static str) -> Self {
//         Self { name }
//     }
// }
// impl<S> Layer<S> for TraceLayer {
//     type Service = TraceService<S>;
//
//     fn layer(&self, inner: S) -> Self::Service {
//         TraceService {
//             inner,
//             name: self.name,
//         }
//     }
// }
// #[derive(Debug, Clone)]
// pub struct TraceService<S> {
//     inner: S,
//     name: &'static str, // TODO: Arc needed?
// }
// impl<S, R> Service<Request<R>> for TraceService<S>
// where
//     S: Service<Request<R>> + Send + 'static,
//     S::Future: Send + 'static,
//     R: std::fmt::Debug,
// {
//     type Response = S::Response;
//     type Error = S::Error;
//     type Future =
//         Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send + 'static>>;
//
//     fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
//         self.inner.poll_ready(cx)
//     }
//
//     // fn call(&mut self, request: Request<R>) -> Self::Future {
//     //     let parent = SpanContext::random();
//     //     let root = Span::root("Root", parent);
//     //     let _span_guard = root.set_local_parent();
//     //     debug!("MEOW: {request:?}");
//     //     let future = self
//     //         .inner
//     //         .call(request)
//     //         .in_span(Span::enter_with_parent("Task", &root));
//     //     Box::pin(future)
//     // }
//
//     fn call(&mut self, mut request: Request<R>) -> Self::Future {
//         let headers = request.headers_mut();
//         let root_span = match span_context_from_headers(headers) {
//             Ok(Some(span_context)) => {
//                 let root_span = Span::root(self.name, span_context);
//                 let _span_guard = root_span.set_local_parent();
//                 Some(root_span)
//             }
//             other => {
//                 let span_context = SpanContext::random();
//                 let root_span = Span::root(self.name, span_context);
//                 let _span_guard = root_span.set_local_parent();
//                 if let Err(error) = other {
//                     error!("Failed to extract span context for {} {error}", self.name);
//                 }
//                 HeaderValue::from_str(&span_context.encode_w3c_traceparent())
//                     .map(|header_value| {
//                         headers.insert(HTTP_HEADER_TRACE_PARENT, header_value);
//                     })
//                     .unwrap_or_else(|error| {
//                         error!("Failed to set span context for {} {error}", self.name);
//                     });
//                 Some(root_span)
//             }
//         };
//         if let Some(root_span) = root_span {
//             let future = self.inner.call(request).in_span(Span::enter_with_parent("Task", &root_span));
//             Box::pin(future)
//         } else {
//             let future = self.inner.call(request);
//             Box::pin(future)
//         }
//     }
// }

#[derive(Debug, Clone, Copy, Default)]
pub struct TraceLayer;

impl<S> Layer<S> for TraceLayer {
    type Service = TraceService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        Self::Service { inner }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct TraceService<S> {
    inner: S,
}

impl<S> Service<Request<Body>> for TraceService<S>
where
    S: Service<Request<Body>> + Clone + Send + 'static,
    S::Future: Send + 'static,
{
    type Response = S::Response;

    type Error = S::Error;

    type Future =
        Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send + 'static>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: Request<Body>) -> Self::Future {
        let parent = SpanContext::random();
        let root = Span::root("Root", parent);
        let _span_guard = root.set_local_parent();
        debug!("MEOW: {:?}", request);
        let future = self
            .inner
            .call(request)
            .in_span(Span::enter_with_parent("Task", &root));
        Box::pin(future)
    }
}

pub fn span_context_from_headers(headers: &HeaderMap) -> TelemetryResult<Option<SpanContext>> {
    if let Some(value) = headers.get(HTTP_HEADER_TRACE_PARENT) {
        Ok(SpanContext::decode_w3c_traceparent(value.to_str()?))
    } else {
        Ok(None)
    }
}
