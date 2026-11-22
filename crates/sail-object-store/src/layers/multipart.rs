// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use fastrace::Span;
use log::debug;
use object_store::multipart::{MultipartStore, PartId};
use object_store::path::Path;
use object_store::{MultipartId, PutPayload, PutResult, Result};
use sail_common::telemetry::{SpanAttribute, TracingFutureExt, record_error};
use tokio::runtime::Handle;
use tokio_util::task::AbortOnDropHandle;

/// The multipart upload interface of an object store, with the requests
/// logged and traced, and run in the runtime of the handle.
///
/// This is the counterpart of the layers that the object store itself has,
/// for the requests that are not part of the object store interface.
pub struct RuntimeAwareMultipartStore {
    inner: Arc<dyn MultipartStore>,
    handle: Handle,
}

impl RuntimeAwareMultipartStore {
    pub fn new(inner: Arc<dyn MultipartStore>, handle: Handle) -> Self {
        Self { inner, handle }
    }
}

fn span(name: &'static str, path: &Path) -> Span {
    Span::enter_with_local_parent(name)
        .with_property(|| (SpanAttribute::OBJECT_STORE_LOCATION, path.to_string()))
}

#[async_trait::async_trait]
impl MultipartStore for RuntimeAwareMultipartStore {
    async fn create_multipart(&self, path: &Path) -> Result<MultipartId> {
        debug!("create_multipart: path: {path:?}");
        let span = span("MultipartStore::create_multipart", path);
        let inner = self.inner.clone();
        let path = path.clone();
        // The request is not canceled when the caller is, so that the upload
        // is known to the caller whenever it has been created.
        self.handle
            .spawn(
                async move { inner.create_multipart(&path).await }
                    .in_span_with_recorder(span, record_error),
            )
            .await?
    }

    async fn put_part(
        &self,
        path: &Path,
        id: &MultipartId,
        part_idx: usize,
        data: PutPayload,
    ) -> Result<PartId> {
        debug!("put_part: path: {path:?} id: {id:?} part_idx: {part_idx}");
        let span = span("MultipartStore::put_part", path).with_property(|| {
            (
                SpanAttribute::OBJECT_STORE_SIZE,
                data.content_length().to_string(),
            )
        });
        let inner = self.inner.clone();
        let path = path.clone();
        let id = id.clone();
        // The request is canceled when the caller is. An upload must be aborted
        // only after the requests for its parts have ended, since a part that
        // is stored after the upload was aborted is not removed.
        AbortOnDropHandle::new(
            self.handle.spawn(
                async move { inner.put_part(&path, &id, part_idx, data).await }
                    .in_span_with_recorder(span, record_error),
            ),
        )
        .await?
    }

    async fn complete_multipart(
        &self,
        path: &Path,
        id: &MultipartId,
        parts: Vec<PartId>,
    ) -> Result<PutResult> {
        debug!(
            "complete_multipart: path: {path:?} id: {id:?} parts: {}",
            parts.len()
        );
        let span = span("MultipartStore::complete_multipart", path);
        let inner = self.inner.clone();
        let path = path.clone();
        let id = id.clone();
        self.handle
            .spawn(
                async move { inner.complete_multipart(&path, &id, parts).await }
                    .in_span_with_recorder(span, record_error),
            )
            .await?
    }

    async fn abort_multipart(&self, path: &Path, id: &MultipartId) -> Result<()> {
        debug!("abort_multipart: path: {path:?} id: {id:?}");
        let span = span("MultipartStore::abort_multipart", path);
        let inner = self.inner.clone();
        let path = path.clone();
        let id = id.clone();
        self.handle
            .spawn(
                async move { inner.abort_multipart(&path, &id).await }
                    .in_span_with_recorder(span, record_error),
            )
            .await?
    }
}
