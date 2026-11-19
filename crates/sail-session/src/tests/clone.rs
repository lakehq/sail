// SPDX-License-Identifier: Apache-2.0

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::prelude::SessionContext;
use futures::TryStreamExt;
use futures::stream::BoxStream;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use sail_catalog::command::{CatalogCommand, CloneTarget};
use sail_catalog::error::CatalogError;
use sail_catalog::manager::CatalogManager;
use sail_catalog::provider::{CreateTableOptions, DropTableOptions};
use sail_common::spec::CreateTableMode;
use sail_common_datafusion::datasource::DataSourceRegistry;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_delta_lake::DeltaLakeSource;
use tokio::sync::oneshot;

use super::create_session;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

const LOG: &[u8] = br#"{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}
{"metaData":{"id":"source-id","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{}}}
{"add":{"path":"part.parquet","partitionValues":{},"size":4,"modificationTime":0,"dataChange":true}}
"#;

#[derive(Debug)]
struct BlockedOperation {
    path: Path,
    entered: oneshot::Sender<()>,
    resume: oneshot::Receiver<()>,
}

#[derive(Debug, Default)]
struct LostReplyStore {
    inner: InMemory,
    fail_after_put: Mutex<Option<Path>>,
    blocked_put: Mutex<Option<BlockedOperation>>,
    blocked_get: Mutex<Option<BlockedOperation>>,
}

impl std::fmt::Display for LostReplyStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "LostReplyStore")
    }
}

#[expect(clippy::unwrap_used)]
async fn wait_for_operation(blocked: &Mutex<Option<BlockedOperation>>, path: &Path) {
    let operation = blocked.lock().unwrap().take_if(|op| &op.path == path);
    if let Some(operation) = operation {
        let _ = operation.entered.send(());
        let _ = operation.resume.await;
    }
}

#[async_trait]
impl ObjectStore for LostReplyStore {
    #[expect(clippy::unwrap_used)]
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        wait_for_operation(&self.blocked_put, location).await;
        let result = self.inner.put_opts(location, payload, options).await?;
        let fail = {
            let mut target = self.fail_after_put.lock().unwrap();
            if target.as_ref() == Some(location) {
                target.take();
                true
            } else {
                false
            }
        };
        if fail {
            Err(object_store::Error::Generic {
                store: "lost-clone-reply",
                source: Box::new(std::io::Error::other("injected error after durable commit")),
            })
        } else {
            Ok(result)
        }
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        options: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, options).await
    }
    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        wait_for_operation(&self.blocked_get, location).await;
        self.inner.get_opts(location, options).await
    }
    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(locations)
    }
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }
    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

fn fixture() -> TestResult<(SessionContext, Arc<LostReplyStore>)> {
    let session = create_session()?;
    let store = Arc::new(LostReplyStore::default());
    session
        .runtime_env()
        .register_object_store(ObjectStoreUrl::parse("memory:///")?.as_ref(), store.clone());
    session
        .extension::<DataSourceRegistry>()?
        .register_data_source(Arc::new(DeltaLakeSource))?;
    Ok((session, store))
}

async fn seed(store: &LostReplyStore, root: &str) -> TestResult {
    store
        .put(
            &Path::parse(format!("{root}/_delta_log/00000000000000000000.json"))?,
            LOG.into(),
        )
        .await?;
    store
        .put(
            &Path::parse(format!("{root}/part.parquet"))?,
            PutPayload::from_static(b"data"),
        )
        .await?;
    Ok(())
}

async fn inventory(store: &LostReplyStore) -> TestResult<BTreeMap<String, Vec<u8>>> {
    let mut files = BTreeMap::new();
    let mut listing = store.list(None);
    while let Some(file) = listing.try_next().await? {
        files.insert(
            file.location.to_string(),
            store.get(&file.location).await?.bytes().await?.to_vec(),
        );
    }
    Ok(files)
}

async fn clone(
    session: &SessionContext,
    source: &str,
    destination: &str,
    named: bool,
) -> Result<(), CatalogError> {
    let manager = session.extension::<CatalogManager>()?;
    CatalogCommand::CloneTable {
        target: if named {
            CloneTarget::Table {
                name: vec!["cloned".into()],
                default_location: destination.into(),
            }
        } else {
            CloneTarget::Path(destination.into())
        },
        source: vec!["delta".into(), source.into()],
        location: named.then(|| destination.into()),
        mode: CreateTableMode::CreateOrReplace,
        is_external: true,
    }
    .execute(&session.task_ctx(), &manager)
    .await?;
    Ok(())
}

#[tokio::test]
#[expect(clippy::unwrap_used)]
async fn shallow_clone_rejects_reregistered_target_during_preparation() -> TestResult {
    let (session, store) = fixture()?;
    let destination = "memory:///destination/";
    seed(&store, "source").await?;
    clone(&session, "memory:///source/", destination, true).await?;
    let before = inventory(&store).await?;
    let (entered, reading) = oneshot::channel();
    let (resume, paused) = oneshot::channel();
    *store.blocked_get.lock().unwrap() = Some(BlockedOperation {
        path: Path::from("source/_delta_log/00000000000000000000.json"),
        entered,
        resume: paused,
    });
    let pending = clone(&session, "memory:///source/", destination, true);
    tokio::pin!(pending);
    tokio::select! {
        result = &mut pending => return Err(format!("clone finished before reading source: {result:?}").into()),
        result = reading => result?,
    }
    let manager = session.extension::<CatalogManager>()?;
    manager
        .drop_table(
            &["cloned"],
            DropTableOptions {
                if_exists: false,
                purge: false,
            },
        )
        .await?;
    let registered = manager
        .create_table(
            &["cloned"],
            CreateTableOptions {
                columns: vec![],
                comment: Some("new registration".into()),
                constraints: vec![],
                location: Some(destination.into()),
                format: "delta".into(),
                partition_by: vec![],
                sort_by: vec![],
                bucket_by: None,
                mode: CreateTableMode::Create,
                properties: vec![],
                is_external: false,
                is_write_precondition: false,
            },
        )
        .await?;
    resume.send(()).unwrap();
    let result = pending.await;
    assert!(
        matches!(result, Err(CatalogError::Conflict(_))),
        "{result:?}"
    );
    assert_eq!(manager.get_table(&["cloned"]).await?.kind, registered.kind);
    assert_eq!(inventory(&store).await?, before);
    Ok(())
}

#[tokio::test]
#[expect(clippy::unwrap_used)]
async fn shallow_clone_lost_commit_reply_fails_closed() -> TestResult {
    for replacing in [false, true] {
        let (session, store) = fixture()?;
        seed(&store, "source").await?;
        if replacing {
            clone(
                &session,
                "memory:///source/",
                "memory:///destination/",
                true,
            )
            .await?;
        }
        let before = inventory(&store).await?;
        let version = i32::from(replacing);
        let commit_path = Path::parse(format!("destination/_delta_log/{version:020}.json"))?;
        *store.fail_after_put.lock().unwrap() = Some(commit_path.clone());
        let result = clone(
            &session,
            "memory:///source/",
            "memory:///destination/",
            true,
        )
        .await;
        assert!(
            matches!(result, Err(CatalogError::CommitStateUnknown(_))),
            "{result:?}"
        );
        let after = inventory(&store).await?;
        assert!(
            after.contains_key(commit_path.as_ref()),
            "the injected failure must follow a durable write"
        );
        for (path, bytes) in &before {
            assert_eq!(
                after.get(path),
                Some(bytes),
                "existing artifact changed: {path}"
            );
        }
        assert_eq!(
            after.len(),
            before.len() + 1,
            "only the new JSON commit may be added"
        );
        let manager = session.extension::<CatalogManager>()?;
        assert!(matches!(
            manager.get_table(&["cloned"]).await,
            Err(CatalogError::CommitStateUnknown(_))
        ));
        assert!(matches!(
            manager
                .drop_table(
                    &["cloned"],
                    DropTableOptions {
                        if_exists: true,
                        purge: false
                    }
                )
                .await,
            Err(CatalogError::CommitStateUnknown(_))
        ));
        assert!(matches!(
            clone(
                &session,
                "memory:///source/",
                "memory:///destination/",
                true
            )
            .await,
            Err(CatalogError::CommitStateUnknown(_))
        ));
        assert_eq!(
            inventory(&store).await?,
            after,
            "uncertain entries cannot be dropped or retried"
        );
    }
    Ok(())
}

#[tokio::test]
async fn shallow_clone_rejects_decoded_location_aliases() -> TestResult {
    let (session, store) = fixture()?;
    seed(&store, "root").await?;
    seed(&store, "root/child").await?;
    let before = inventory(&store).await?;
    for (source, destination) in [
        ("memory:///root/", "memory:///%72oot/"),
        ("memory:///%72oot/", "memory:///root/"),
        ("memory:///root/", "memory:///%72oot/child/"),
        ("memory:///root/child/", "memory:///%72oot/"),
        ("memory:///root/", "memory:///%72oot%2Fchild/"),
    ] {
        let result = clone(&session, source, destination, false).await;
        assert!(
            result.is_err(),
            "accepted overlapping locations {source} and {destination}"
        );
        assert_eq!(inventory(&store).await?, before);
    }
    // A lexical prefix without a segment boundary is not an overlapping location.
    clone(&session, "memory:///root/", "memory:///root2/", false).await?;
    Ok(())
}

#[tokio::test]
async fn shallow_clone_rejects_files_inside_encoded_destination() -> TestResult {
    let (session, store) = fixture()?;
    let log = std::str::from_utf8(LOG)?
        .replace("\"part.parquet\"", "\"memory:///destination/part.parquet\"");
    store
        .put(
            &Path::from("source/_delta_log/00000000000000000000.json"),
            log.into(),
        )
        .await?;
    store
        .put(
            &Path::from("destination/part.parquet"),
            PutPayload::from_static(b"data"),
        )
        .await?;
    // Seed a destination log, so rejection exercises file ownership, not nonempty-directory validation.
    store
        .put(
            &Path::from("destination/_delta_log/00000000000000000000.json"),
            LOG.into(),
        )
        .await?;
    let before = inventory(&store).await?;
    let result = clone(
        &session,
        "memory:///source/",
        "memory:///%64estination/",
        false,
    )
    .await;
    assert!(result.is_err());
    assert_eq!(inventory(&store).await?, before);
    Ok(())
}

#[tokio::test]
#[expect(clippy::unwrap_used)]
async fn shallow_clone_publishes_before_checksum() -> TestResult {
    let (session, store) = fixture()?;
    seed(&store, "source").await?;
    let checksum = Path::from("destination/_delta_log/00000000000000000000.crc");
    let (entered, writing_checksum) = oneshot::channel();
    let (resume, wait) = oneshot::channel();
    *store.blocked_put.lock().unwrap() = Some(BlockedOperation {
        path: checksum.clone(),
        entered,
        resume: wait,
    });
    let cloning = clone(
        &session,
        "memory:///source/",
        "memory:///destination/",
        true,
    );
    let read_catalog = async {
        writing_checksum.await?;
        let manager = session.extension::<CatalogManager>()?;
        assert_eq!(manager.get_table(&["cloned"]).await?.name, "cloned");
        let _ = resume.send(());
        TestResult::Ok(())
    };
    let (cloned, read) = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        tokio::join!(cloning, read_catalog)
    })
    .await?;
    cloned?;
    read?;
    store.head(&checksum).await?;
    Ok(())
}
