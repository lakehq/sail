use std::any::Any;

use datafusion::common::runtime::JoinSetTracer;
use fastrace::future::FutureExt;
use fastrace::Span;
use futures::future::BoxFuture;

pub struct FastraceJoinSetTracer;

impl JoinSetTracer for FastraceJoinSetTracer {
    fn trace_future(
        &self,
        fut: BoxFuture<'static, Box<dyn Any + Send>>,
    ) -> BoxFuture<'static, Box<dyn Any + Send>> {
        let span = Span::enter_with_local_parent("join_set_future");
        Box::pin(fut.in_span(span))
    }

    fn trace_block(
        &self,
        f: Box<dyn FnOnce() -> Box<dyn Any + Send> + Send>,
    ) -> Box<dyn FnOnce() -> Box<dyn Any + Send> + Send> {
        let span = Span::enter_with_local_parent("join_set_block");
        Box::new(move || {
            let _guard = span.set_local_parent();
            f()
        })
    }
}
