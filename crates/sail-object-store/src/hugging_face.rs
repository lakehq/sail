use std::fmt::Display;
use std::path::PathBuf;

use async_trait::async_trait;
use chrono::{DateTime, TimeZone, Utc};
use futures::stream;
use futures::stream::BoxStream;
use hf_hub::api::tokio::{Api, ApiBuilder, ApiError, ApiRepo};
use hf_hub::{Repo, RepoType};
use lazy_static::lazy_static;
use log::debug;
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use object_store::{
    path, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMultipartOpts, PutOptions, PutPayload, PutResult,
};
use regex::Regex;
use reqwest::StatusCode;
use tonic::codegen::http;

lazy_static! {
    static ref HF_PATH_PATTERN: Regex = Regex::new(
        r"^(?P<username>[^/]+)/(?P<dataset>[^/@]+)(@(?P<revision>[^/]+))?/(?P<path>.*)$"
    )
    .unwrap();
}

#[derive(Debug, thiserror::Error)]
enum HuggingFaceError {
    #[error("Hugging Face API error: {0}")]
    ApiError(#[from] ApiError),
    #[error("date/time parse error: {0}")]
    DateTimeParseError(#[from] chrono::format::ParseError),
    #[error("HTTP request error: {0}")]
    RequestError(#[from] reqwest::Error),
    #[error("invalid path: {0}")]
    InvalidPath(PathBuf),
}

impl From<HuggingFaceError> for object_store::Error {
    fn from(value: HuggingFaceError) -> Self {
        const OBJECT_STORE_NAME: &str = "Hugging Face object store";

        match value {
            HuggingFaceError::ApiError(e @ ApiError::MissingHeader(_))
            | HuggingFaceError::ApiError(e @ ApiError::InvalidHeader(_))
            | HuggingFaceError::ApiError(e @ ApiError::InvalidHeaderValue(_))
            | HuggingFaceError::ApiError(e @ ApiError::ToStr(_))
            | HuggingFaceError::ApiError(e @ ApiError::ParseIntError(_))
            | HuggingFaceError::ApiError(e @ ApiError::IoError(_))
            | HuggingFaceError::ApiError(e @ ApiError::TooManyRetries(_))
            | HuggingFaceError::ApiError(e @ ApiError::TryAcquireError(_))
            | HuggingFaceError::ApiError(e @ ApiError::AcquireError(_))
            | HuggingFaceError::ApiError(e @ ApiError::LockAcquisition(_)) => {
                object_store::Error::Generic {
                    store: OBJECT_STORE_NAME,
                    source: Box::new(e),
                }
            }
            HuggingFaceError::ApiError(ApiError::RequestError(e))
            | HuggingFaceError::RequestError(e) => {
                if e.status().is_some_and(|s| s == StatusCode::NOT_FOUND) {
                    object_store::Error::NotFound {
                        path: e.url().map(|x| x.path().to_string()).unwrap_or_default(),
                        source: Box::new(e),
                    }
                } else {
                    object_store::Error::Generic {
                        store: OBJECT_STORE_NAME,
                        source: Box::new(e),
                    }
                }
            }
            HuggingFaceError::ApiError(ApiError::Join(e)) => {
                object_store::Error::JoinError { source: e }
            }
            HuggingFaceError::DateTimeParseError(e) => object_store::Error::Generic {
                store: OBJECT_STORE_NAME,
                source: Box::new(e),
            },
            HuggingFaceError::InvalidPath(path) => object_store::Error::InvalidPath {
                source: path::Error::InvalidPath { path },
            },
        }
    }
}

#[derive(Debug)]
struct HuggingFacePath {
    username: String,
    dataset: String,
    revision: Option<String>,
    path: String,
}

impl HuggingFacePath {
    fn parse(path: &Path) -> object_store::Result<Self> {
        let Some(captures) = HF_PATH_PATTERN.captures(path.as_ref()) else {
            Err(HuggingFaceError::InvalidPath(path.as_ref().into()))?
        };
        let username = captures.name("username").unwrap().as_str().to_string();
        let dataset = captures.name("dataset").unwrap().as_str().to_string();
        let revision = captures.name("revision").map(|m| m.as_str().to_string());
        let path = captures.name("path").unwrap().as_str().to_string();
        Ok(Self {
            username,
            dataset,
            revision,
            path,
        })
    }

    fn parse_optional(path: Option<&Path>) -> object_store::Result<Self> {
        let Some(path) = path else {
            Err(HuggingFaceError::InvalidPath(Default::default()))?
        };
        Self::parse(path)
    }

    fn repo(&self) -> Repo {
        let repo_id = format!("{}/{}", self.username, self.dataset);
        if let Some(revision) = &self.revision {
            Repo::with_revision(repo_id, RepoType::Dataset, revision.clone())
        } else {
            Repo::new(repo_id, RepoType::Dataset)
        }
    }

    fn base_path(&self) -> String {
        if let Some(revision) = &self.revision {
            format!("{}/{}@{}", self.username, self.dataset, revision)
        } else {
            format!("{}/{}", self.username, self.dataset)
        }
    }

    fn matches(&self, filename: &str) -> bool {
        filename.starts_with(&self.path)
    }
}

#[derive(Debug)]
pub struct HuggingFaceObjectStore {
    api: Api,
    local: LocalFileSystem,
}

impl HuggingFaceObjectStore {
    pub fn try_new() -> object_store::Result<Self> {
        Ok(Self {
            api: ApiBuilder::from_env()
                .with_progress(false)
                .build()
                .map_err(HuggingFaceError::from)?,
            local: LocalFileSystem::new(),
        })
    }

    async fn get_meta(
        api: &Api,
        repo: &ApiRepo,
        base_path: &str,
        filename: &str,
    ) -> object_store::Result<ObjectMeta> {
        let location = Path::parse(format!("{base_path}/{filename}"))?;
        debug!("Getting Hugging Face file metadata: {location:?}");
        let response = api
            .client()
            .head(repo.url(filename))
            .send()
            .await
            .map_err(HuggingFaceError::from)?
            .error_for_status()
            .map_err(HuggingFaceError::from)?;
        let headers = response.headers();

        let size = headers
            .get(http::header::CONTENT_LENGTH)
            .map(|x| Ok::<_, ApiError>(x.to_str()?.parse()?))
            .transpose()
            .map_err(HuggingFaceError::from)?
            .ok_or_else(|| {
                HuggingFaceError::from(ApiError::MissingHeader(http::header::CONTENT_LENGTH))
            })?;

        let last_modified = headers
            .get(http::header::LAST_MODIFIED)
            .map(|x| -> object_store::Result<_> {
                let s = x
                    .to_str()
                    .map_err(|x| HuggingFaceError::from(ApiError::ToStr(x)))?;
                let dt = DateTime::parse_from_rfc2822(s).map_err(HuggingFaceError::from)?;
                Ok(dt.with_timezone(&Utc))
            })
            .transpose()?
            .unwrap_or(Utc.timestamp_nanos(0));

        let e_tag = headers
            .get(http::header::ETAG)
            .map(|x| -> object_store::Result<_> {
                Ok(x.to_str()
                    .map_err(|x| HuggingFaceError::from(ApiError::ToStr(x)))?
                    .to_string())
            })
            .transpose()?;

        Ok(ObjectMeta {
            location,
            last_modified,
            size,
            e_tag,
            version: None,
        })
    }
}

impl Display for HuggingFaceObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "HuggingFaceObjectStore")
    }
}

#[async_trait]
impl ObjectStore for HuggingFaceObjectStore {
    async fn put_opts(
        &self,
        _location: &Path,
        _payload: PutPayload,
        _opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        Err(object_store::Error::NotImplemented)
    }

    async fn put_multipart_opts(
        &self,
        _location: &Path,
        _opts: PutMultipartOpts,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        Err(object_store::Error::NotImplemented)
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        let GetOptions {
            if_match,
            if_none_match,
            if_modified_since,
            if_unmodified_since,
            range,
            version,
            head,
            extensions: _,
        } = options;
        if if_match.is_some()
            || if_none_match.is_some()
            || if_modified_since.is_some()
            || if_unmodified_since.is_some()
            || version.is_some()
        {
            return Err(object_store::Error::NotImplemented);
        }
        let path = HuggingFacePath::parse(location)?;
        let repo = self.api.repo(path.repo());
        if head {
            let meta =
                Self::get_meta(&self.api, &repo, &path.base_path(), path.path.as_str()).await?;
            Ok(GetResult {
                payload: GetResultPayload::Stream(Box::pin(stream::empty())),
                meta,
                range: 0..0,
                attributes: Default::default(),
            })
        } else {
            // TODO: The cache is not ideal for cluster mode. In cluster mode, the driver
            //   only needs to access the metadata at the end of the Parquet file, but the
            //   entire file is downloaded and cached.
            // TODO: The cache is not efficient for wide tables with many columns when
            //   only a small set of columns are accessed.
            debug!("Fetching Hugging Face file if not cached: {location}");
            let location = repo
                .get(path.path.as_str())
                .await
                .map_err(HuggingFaceError::from)?;
            debug!(
                "Reading Hugging Face file from local cache: {}{}",
                location.as_path().display(),
                range
                    .as_ref()
                    .map(|x| format!(" ({x})"))
                    .unwrap_or_default()
            );
            self.local
                .get_opts(
                    &Path::from_filesystem_path(location)?,
                    GetOptions {
                        range,
                        ..GetOptions::default()
                    },
                )
                .await
        }
    }

    async fn delete(&self, _location: &Path) -> object_store::Result<()> {
        Err(object_store::Error::NotImplemented)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        let path = match HuggingFacePath::parse_optional(prefix) {
            Ok(x) => x,
            Err(e) => return Box::pin(stream::once(async { Err(e) })),
        };
        debug!("Listing Hugging Face files: {path:?}");
        let api = self.api.clone();
        let stream = async_stream::try_stream! {
            let repo = api.repo(path.repo());
            let info = repo.info().await.map_err(HuggingFaceError::from)?;
            for sibling in info.siblings {
                let filename = sibling.rfilename.as_str();
                if path.matches(filename) {
                    yield Self::get_meta(&api, &repo, &path.base_path(), filename).await?;
                }
            }
        };
        Box::pin(stream)
    }

    async fn list_with_delimiter(
        &self,
        _prefix: Option<&Path>,
    ) -> object_store::Result<ListResult> {
        Err(object_store::Error::NotImplemented)
    }

    async fn copy(&self, _from: &Path, _to: &Path) -> object_store::Result<()> {
        Err(object_store::Error::NotImplemented)
    }

    async fn copy_if_not_exists(&self, _from: &Path, _to: &Path) -> object_store::Result<()> {
        Err(object_store::Error::NotImplemented)
    }
}
