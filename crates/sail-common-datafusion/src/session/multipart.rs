// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use datafusion_common::Result;
use object_store::multipart::MultipartStore;
use url::Url;

use crate::extension::SessionExtension;

type Resolver = dyn Fn(&Url) -> Result<Option<Arc<dyn MultipartStore>>> + Send + Sync;

/// Gives the multipart upload interface of the object stores of the session.
///
/// An object store is known to the session only through the interface that
/// all object stores have. This is the way to the interface that allows
/// an upload to be completed by another process than the one that has
/// uploaded the data, for the object stores that have this interface.
pub struct MultipartStoreProvider {
    resolver: Box<Resolver>,
}

impl MultipartStoreProvider {
    /// Creates the provider from a function that returns the multipart upload
    /// interface of the object store for a URL, or [`None`] if the object
    /// store has no such interface.
    pub fn new(
        resolver: impl Fn(&Url) -> Result<Option<Arc<dyn MultipartStore>>> + Send + Sync + 'static,
    ) -> Self {
        Self {
            resolver: Box::new(resolver),
        }
    }

    /// Returns the multipart upload interface of the object store for the URL,
    /// or [`None`] if the object store has no such interface.
    pub fn multipart_store(&self, url: &Url) -> Result<Option<Arc<dyn MultipartStore>>> {
        (self.resolver)(url)
    }
}

impl SessionExtension for MultipartStoreProvider {
    fn name() -> &'static str {
        "MultipartStoreProvider"
    }
}
