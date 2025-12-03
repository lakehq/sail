use std::fmt;
use std::ops::Range;
use std::sync::Arc;

use fastrace::Span;
use fastrace_futures::StreamExt;
use futures::stream::BoxStream;
use futures::FutureExt;
use log::debug;
use object_store::path::Path;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result, UploadPart,
};
use sail_telemetry::common::SpanAttribute;
use sail_telemetry::futures::TracingFutureExt;
use sail_telemetry::recorder::record_error;
use tonic::codegen::Bytes;

#[derive(Debug)]
pub struct LoggingObjectStore {
    inner: Arc<dyn ObjectStore>,
}

impl LoggingObjectStore {
    pub fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self { inner }
    }
}

impl fmt::Display for LoggingObjectStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "LoggingObjectStore({})", self.inner)
    }
}

#[async_trait::async_trait]
#[warn(clippy::missing_trait_methods)]
impl ObjectStore for LoggingObjectStore {
    async fn put(&self, location: &Path, payload: PutPayload) -> Result<PutResult> {
        debug!("put: location: {location:?}");

        let span = Span::enter_with_local_parent("ObjectStore::put").with_properties(|| {
            [
                (SpanAttribute::OBJECT_STORE_INSTANCE, self.inner.to_string()),
                (SpanAttribute::OBJECT_STORE_LOCATION, location.to_string()),
                (
                    SpanAttribute::OBJECT_STORE_SIZE,
                    payload.content_length().to_string(),
                ),
            ]
        });
        self.inner
            .put(location, payload)
            .in_span_with_recorder(span, record_error)
            .await
    }

    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        debug!("put_opts: location: {location:?} opts: {opts:?}");

        let span = Span::enter_with_local_parent("ObjectStore::put_opts").with_properties(|| {
            [
                (SpanAttribute::OBJECT_STORE_INSTANCE, self.inner.to_string()),
                (SpanAttribute::OBJECT_STORE_LOCATION, location.to_string()),
                (
                    SpanAttribute::OBJECT_STORE_SIZE,
                    payload.content_length().to_string(),
                ),
                (SpanAttribute::OBJECT_STORE_OPTIONS, format!("{opts:?}")),
            ]
        });
        self.inner
            .put_opts(location, payload, opts)
            .in_span_with_recorder(span, record_error)
            .await
    }

    async fn put_multipart(&self, location: &Path) -> Result<Box<dyn MultipartUpload>> {
        debug!("put_multipart: location: {location:?}");

        let span =
            Span::enter_with_local_parent("ObjectStore::put_multipart").with_properties(|| {
                [
                    (SpanAttribute::OBJECT_STORE_INSTANCE, self.inner.to_string()),
                    (SpanAttribute::OBJECT_STORE_LOCATION, location.to_string()),
                ]
            });
        self.inner
            .put_multipart(location)
            .in_span_with_recorder(span, record_error)
            .await
            .map(|upload| Box::new(TracingMultipartUpload::new(upload)) as Box<dyn MultipartUpload>)
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        debug!("put_multipart_opts: location: {location:?} opts: {opts:?}");

        let span = Span::enter_with_local_parent("ObjectStore::put_multipart_opts")
            .with_properties(|| {
                [
                    (SpanAttribute::OBJECT_STORE_INSTANCE, self.inner.to_string()),
                    (SpanAttribute::OBJECT_STORE_LOCATION, location.to_string()),
                    (SpanAttribute::OBJECT_STORE_OPTIONS, format!("{opts:?}")),
                ]
            });
        self.inner
            .put_multipart_opts(location, opts)
            .in_span_with_recorder(span, record_error)
            .await
            .map(|upload| Box::new(TracingMultipartUpload::new(upload)) as Box<dyn MultipartUpload>)
    }

    async fn get(&self, location: &Path) -> Result<GetResult> {
        debug!("get: location: {location:?}");

        let span = Span::enter_with_local_parent("ObjectStore::get").with_properties(|| {
            [
                (SpanAttribute::OBJECT_STORE_INSTANCE, self.inner.to_string()),
                (SpanAttribute::OBJECT_STORE_LOCATION, location.to_string()),
            ]
        });
        self.inner
            .get(location)
            .in_span_with_recorder(span, |span, output| {
                record_error(span, output);
                if let Ok(output) = output {
                    span.add_property(|| {
                        (
                            SpanAttribute::OBJECT_STORE_RANGE,
                            format!("{:?}", output.range),
                        )
                    });
                }
            })
            .await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        debug!("get_opts: location: {location:?} options: {options:?}");

        let span = Span::enter_with_local_parent("ObjectStore::get_opts").with_properties(|| {
            [
                (SpanAttribute::OBJECT_STORE_INSTANCE, self.inner.to_string()),
                (SpanAttribute::OBJECT_STORE_LOCATION, location.to_string()),
                (SpanAttribute::OBJECT_STORE_OPTIONS, format!("{options:?}")),
            ]
        });
        self.inner
            .get_opts(location, options)
            .in_span_with_recorder(span, |span, output| {
                record_error(span, output);
                if let Ok(output) = output {
                    span.add_property(|| {
                        (
                            SpanAttribute::OBJECT_STORE_RANGE,
                            format!("{:?}", output.range),
                        )
                    });
                }
            })
            .await
    }

    async fn get_range(&self, location: &Path, range: Range<u64>) -> Result<Bytes> {
        debug!("get_range: location: {location:?} range: {range:?}");

        let span = Span::enter_with_local_parent("ObjectStore::get_range").with_properties(|| {
            [
                (SpanAttribute::OBJECT_STORE_INSTANCE, self.inner.to_string()),
                (SpanAttribute::OBJECT_STORE_LOCATION, location.to_string()),
                (SpanAttribute::OBJECT_STORE_RANGE, format!("{range:?}")),
            ]
        });
        self.inner
            .get_range(location, range)
            .in_span_with_recorder(span, |span, output| {
                record_error(span, output);
                if let Ok(output) = output {
                    span.add_property(|| {
                        (SpanAttribute::OBJECT_STORE_SIZE, output.len().to_string())
                    });
                }
            })
            .await
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        debug!("get_ranges: location: {location:?} ranges: {ranges:?}");

        let span = Span::enter_with_local_parent("ObjectStore::get_ranges").with_properties(|| {
            [
                (SpanAttribute::OBJECT_STORE_INSTANCE, self.inner.to_string()),
                (SpanAttribute::OBJECT_STORE_LOCATION, location.to_string()),
                (SpanAttribute::OBJECT_STORE_RANGES, format!("{ranges:?}")),
            ]
        });
        self.inner
            .get_ranges(location, ranges)
            .in_span_with_recorder(span, |span, output| {
                record_error(span, output);
                if let Ok(output) = output {
                    let sizes: Vec<usize> = output.iter().map(|b| b.len()).collect();
                    span.add_property(|| {
                        (
                            SpanAttribute::OBJECT_STORE_SIZES,
                            format!("{:?}", sizes.as_slice()),
                        )
                    });
                }
            })
            .await
    }

    async fn head(&self, location: &Path) -> Result<ObjectMeta> {
        debug!("head: location: {location:?}");

        let span = Span::enter_with_local_parent("ObjectStore::head").with_properties(|| {
            [
                (SpanAttribute::OBJECT_STORE_INSTANCE, self.inner.to_string()),
                (SpanAttribute::OBJECT_STORE_LOCATION, location.to_string()),
            ]
        });
        self.inner
            .head(location)
            .in_span_with_recorder(span, record_error)
            .await
    }

    async fn delete(&self, location: &Path) -> Result<()> {
        debug!("delete: location: {location:?}");

        let span = Span::enter_with_local_parent("ObjectStore::delete").with_properties(|| {
            [
                (SpanAttribute::OBJECT_STORE_INSTANCE, self.inner.to_string()),
                (SpanAttribute::OBJECT_STORE_LOCATION, location.to_string()),
            ]
        });
        self.inner
            .delete(location)
            .in_span_with_recorder(span, record_error)
            .await
    }

    fn delete_stream<'a>(
        &'a self,
        locations: BoxStream<'a, Result<Path>>,
    ) -> BoxStream<'a, Result<Path>> {
        debug!("delete_stream");

        let span = Span::enter_with_local_parent("ObjectStore::delete_stream")
            .with_property(|| (SpanAttribute::OBJECT_STORE_INSTANCE, self.inner.to_string()));
        Box::pin(self.inner.delete_stream(locations).in_span(span))
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        debug!("list: prefix: {prefix:?}");

        let span = Span::enter_with_local_parent("ObjectStore::list")
            .with_property(|| (SpanAttribute::OBJECT_STORE_INSTANCE, self.inner.to_string()));
        if let Some(prefix) = prefix {
            span.add_property(|| (SpanAttribute::OBJECT_STORE_PREFIX, prefix.to_string()));
        }
        Box::pin(self.inner.list(prefix).in_span(span))
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        debug!("list_with_offset: prefix: {prefix:?} offset: {offset:?}");

        let span = Span::enter_with_local_parent("ObjectStore::list_with_offset")
            .with_property(|| (SpanAttribute::OBJECT_STORE_INSTANCE, self.inner.to_string()));
        if let Some(prefix) = prefix {
            span.add_property(|| (SpanAttribute::OBJECT_STORE_PREFIX, prefix.to_string()));
        }
        span.add_property(|| (SpanAttribute::OBJECT_STORE_OFFSET, offset.to_string()));
        Box::pin(self.inner.list_with_offset(prefix, offset).in_span(span))
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        debug!("list_with_delimiter: prefix: {prefix:?}");

        let span = Span::enter_with_local_parent("ObjectStore::list_with_delimiter")
            .with_property(|| (SpanAttribute::OBJECT_STORE_INSTANCE, self.inner.to_string()));
        if let Some(prefix) = prefix {
            span.add_property(|| (SpanAttribute::OBJECT_STORE_PREFIX, prefix.to_string()));
        }
        self.inner
            .list_with_delimiter(prefix)
            .in_span_with_recorder(span, record_error)
            .await
    }

    async fn copy(&self, from: &Path, to: &Path) -> Result<()> {
        debug!("copy: from: {from:?} to: {to:?}");

        let span = Span::enter_with_local_parent("ObjectStore::copy").with_properties(|| {
            [
                (SpanAttribute::OBJECT_STORE_INSTANCE, self.inner.to_string()),
                (SpanAttribute::OBJECT_STORE_FROM, from.to_string()),
                (SpanAttribute::OBJECT_STORE_TO, to.to_string()),
            ]
        });
        self.inner
            .copy(from, to)
            .in_span_with_recorder(span, record_error)
            .await
    }

    async fn rename(&self, from: &Path, to: &Path) -> Result<()> {
        debug!("rename: from: {from:?} to: {to:?}");

        let span = Span::enter_with_local_parent("ObjectStore::rename").with_properties(|| {
            [
                (SpanAttribute::OBJECT_STORE_INSTANCE, self.inner.to_string()),
                (SpanAttribute::OBJECT_STORE_FROM, from.to_string()),
                (SpanAttribute::OBJECT_STORE_TO, to.to_string()),
            ]
        });
        self.inner
            .rename(from, to)
            .in_span_with_recorder(span, record_error)
            .await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        debug!("copy_if_not_exists: from: {from:?} to: {to:?}");

        let span = Span::enter_with_local_parent("ObjectStore::copy_if_not_exists")
            .with_properties(|| {
                [
                    (SpanAttribute::OBJECT_STORE_INSTANCE, self.inner.to_string()),
                    (SpanAttribute::OBJECT_STORE_FROM, from.to_string()),
                    (SpanAttribute::OBJECT_STORE_TO, to.to_string()),
                ]
            });
        self.inner
            .copy_if_not_exists(from, to)
            .in_span_with_recorder(span, record_error)
            .await
    }

    async fn rename_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        debug!("rename_if_not_exists: from: {from:?} to: {to:?}");

        let span = Span::enter_with_local_parent("ObjectStore::rename_if_not_exists")
            .with_properties(|| {
                [
                    (SpanAttribute::OBJECT_STORE_INSTANCE, self.inner.to_string()),
                    (SpanAttribute::OBJECT_STORE_FROM, from.to_string()),
                    (SpanAttribute::OBJECT_STORE_TO, to.to_string()),
                ]
            });
        self.inner
            .rename_if_not_exists(from, to)
            .in_span_with_recorder(span, record_error)
            .await
    }
}

struct TracingMultipartUpload {
    inner: Box<dyn MultipartUpload>,
    span: Span,
}

impl TracingMultipartUpload {
    pub fn new(inner: Box<dyn MultipartUpload>) -> Self {
        let span = Span::enter_with_local_parent("MultipartUpload");
        Self { inner, span }
    }
}

impl fmt::Debug for TracingMultipartUpload {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TracingMultipartUpload")
            .field("inner", &self.inner)
            .finish()
    }
}

#[async_trait::async_trait]
impl MultipartUpload for TracingMultipartUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        let span = Span::enter_with_parent("MultipartUpload::put_part", &self.span);
        self.inner
            .put_part(data)
            .in_span_with_recorder(span, record_error)
            .boxed()
    }

    async fn complete(&mut self) -> Result<PutResult> {
        let span = Span::enter_with_parent("MultipartUpload::complete", &self.span);
        self.inner
            .complete()
            .in_span_with_recorder(span, record_error)
            .await
    }

    async fn abort(&mut self) -> Result<()> {
        let span = Span::enter_with_parent("MultipartUpload::abort", &self.span);
        self.inner
            .abort()
            .in_span_with_recorder(span, record_error)
            .await
    }
}
