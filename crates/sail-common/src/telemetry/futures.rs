use std::future::Future;

use fastrace::Span;
use pin_project_lite::pin_project;

/// An extension trait for [`Future`]s to run them within a tracing span
/// and record custom information upon their completion.
/// This is inspired by [`fastrace::future::FutureExt::in_span`].
pub trait TracingFutureExt: Future {
    fn in_span_with_recorder<R>(self, span: Span, recorder: R) -> InSpanWithRecorder<Self, R>
    where
        Self: Sized,
        R: FnOnce(&Span, &Self::Output) + Send + 'static;
}

impl<T: Future> TracingFutureExt for T {
    fn in_span_with_recorder<R>(self, span: Span, recorder: R) -> InSpanWithRecorder<Self, R>
    where
        Self: Sized,
        R: FnOnce(&Span, &T::Output) + Send + 'static,
    {
        InSpanWithRecorder {
            inner: self,
            span: Some(span),
            recorder: Some(recorder),
        }
    }
}

pin_project! {
    pub struct InSpanWithRecorder<F, R> {
        #[pin]
        inner: F,
        span: Option<Span>,
        recorder: Option<R>,
    }
}

impl<F, R> Future for InSpanWithRecorder<F, R>
where
    F: Future,
    R: FnOnce(&Span, &F::Output) + Send + 'static,
{
    type Output = F::Output;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let this = self.project();

        let _guard = this.span.as_ref().map(|s| s.set_local_parent());
        let poll = this.inner.poll(cx);

        if let std::task::Poll::Ready(ref output) = poll {
            let span = this.span.take();
            let recorder = this.recorder.take();
            if let (Some(span), Some(recorder)) = (span, recorder) {
                recorder(&span, output);
            }
        }

        poll
    }
}
