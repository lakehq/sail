use core::fmt;

use fastrace::Span;

use crate::common::SpanAttribute;

/// Records the error message from a [`Result`] in the given span.
pub fn record_error<T, E>(span: &Span, result: &Result<T, E>)
where
    E: fmt::Display,
{
    if let Err(e) = result {
        span.add_property(|| (SpanAttribute::EXCEPTION_MESSAGE, e.to_string()));
    }
}
