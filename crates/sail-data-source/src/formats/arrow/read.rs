use std::io::{Seek, SeekFrom};
use std::sync::Arc;

use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::ipc::convert::fb_to_schema;
use datafusion::arrow::ipc::reader::{FileReader, StreamReader};
use datafusion::arrow::ipc::root_as_message;
use datafusion::catalog::Session;
use datafusion::datasource::file_format::arrow::ArrowFormat;
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::physical_plan::ArrowSource;
use datafusion_common::parsers::CompressionTypeVariant;
use datafusion_common::{internal_datafusion_err, Result};
use datafusion_datasource::file_scan_config::{FileScanConfig, FileScanConfigBuilder};
use futures::stream::BoxStream;
use futures::StreamExt;
use object_store::path::Path;
use object_store::{GetOptions, GetRange, GetResultPayload, ObjectStore, ObjectStoreExt};

use crate::listing::source::{ListingScanInput, ReadFormat};

#[derive(Debug, Default, Clone)]
pub struct ArrowReadFormat;

const ARROW_MAGIC: [u8; 6] = [b'A', b'R', b'R', b'O', b'W', b'1'];
const CONTINUATION_MARKER: [u8; 4] = [0xff; 4];

async fn is_object_in_arrow_ipc_file_format(
    store: Arc<dyn ObjectStore>,
    object_location: &Path,
) -> Result<bool> {
    let get_opts = GetOptions {
        range: Some(GetRange::Bounded(0..6)),
        ..Default::default()
    };
    let bytes = store
        .get_opts(object_location, get_opts)
        .await?
        .bytes()
        .await?;
    Ok(bytes.len() >= 6 && bytes[0..6] == ARROW_MAGIC)
}

#[async_trait::async_trait]
impl ReadFormat for ArrowReadFormat {
    fn create_read_format(
        &self,
        _compression: Option<CompressionTypeVariant>,
    ) -> Result<Arc<dyn FileFormat>> {
        Ok(Arc::new(ArrowFormat))
    }

    async fn infer_schema(
        &self,
        _ctx: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        objects: &[object_store::ObjectMeta],
        _compression: CompressionTypeVariant,
    ) -> Result<SchemaRef> {
        let mut schemas = vec![];
        for object in objects {
            let r = store.as_ref().get(&object.location).await?;
            let schema = match r.payload {
                #[cfg(not(target_arch = "wasm32"))]
                GetResultPayload::File(mut file, _) => match FileReader::try_new(&mut file, None) {
                    Ok(reader) => reader.schema(),
                    Err(file_error) => {
                        // `FileReader` read some bytes while trying to parse the file,
                        // so we need to rewind it to the beginning.
                        file.seek(SeekFrom::Start(0))?;
                        match StreamReader::try_new(&mut file, None) {
                            Ok(reader) => reader.schema(),
                            Err(stream_error) => {
                                return Err(internal_datafusion_err!(
                                    "Failed to parse Arrow file as either file format or stream format. File format error: {file_error}. Stream format error: {stream_error}"
                                ));
                            }
                        }
                    }
                },
                GetResultPayload::Stream(stream) => infer_stream_schema(stream).await?,
            };
            schemas.push(schema.as_ref().clone());
        }
        Ok(Arc::new(Schema::try_merge(schemas)?))
    }

    async fn scan(&self, ctx: &dyn Session, input: ListingScanInput) -> Result<FileScanConfig> {
        let object_store = ctx.runtime_env().object_store(&input.object_store_url)?;
        let object_location = &input
            .file_groups
            .first()
            .ok_or_else(|| internal_datafusion_err!("No files found in file group"))?
            .files()
            .first()
            .ok_or_else(|| internal_datafusion_err!("No files found in file group"))?
            .object_meta
            .location;

        let source =
            if is_object_in_arrow_ipc_file_format(Arc::clone(&object_store), object_location)
                .await?
            {
                ArrowSource::new_file_source(input.schema)
            } else {
                ArrowSource::new_stream_file_source(input.schema)
            };

        let config = FileScanConfigBuilder::new(input.object_store_url, Arc::new(source))
            .with_file_groups(input.file_groups)
            .with_constraints(input.constraints)
            .with_statistics(input.statistics)
            .with_projection_indices(input.projection)?
            .with_limit(input.limit)
            .with_output_ordering(input.output_ordering)
            .with_preserve_order(input.preserve_order)
            .with_partitioned_by_file_group(input.partitioned_by_file_group)
            .build();

        Ok(config)
    }
}

// Custom implementation of inferring schema from Arrow IPC stream payload.
// Adapted from DataFusion's Arrow data source implementation.
async fn infer_stream_schema(
    mut stream: BoxStream<'static, object_store::Result<bytes::Bytes>>,
) -> Result<SchemaRef> {
    // For the purposes of this function, the arrow "preamble" is the magic number, padding, and
    // the continuation marker. 16 bytes covers the preamble and metadata length no matter which
    // version or format is used.
    let bytes = extend_bytes_to_n_length_from_stream(vec![], 16, &mut stream).await?;

    // The preamble length is everything before the metadata length
    let preamble_len = if bytes[0..6] == ARROW_MAGIC {
        // File format starts with magic number "ARROW1"
        if bytes[8..12] == CONTINUATION_MARKER {
            // Continuation marker was added in v0.15.0
            12
        } else {
            // File format before v0.15.0
            8
        }
    } else if bytes[0..4] == CONTINUATION_MARKER {
        // Stream format after v0.15.0 starts with continuation marker
        4
    } else {
        // Stream format before v0.15.0 does not have a preamble
        0
    };

    let meta_len_bytes: [u8; 4] =
        bytes[preamble_len..preamble_len + 4]
            .try_into()
            .map_err(|err| {
                ArrowError::ParseError(format!(
                    "Unable to read IPC message metadata length: {err:?}"
                ))
            })?;

    let meta_len = i32::from_le_bytes(meta_len_bytes);
    if meta_len < 0 {
        return Err(
            ArrowError::ParseError("IPC message metadata length is negative".to_string()).into(),
        );
    }

    let bytes = extend_bytes_to_n_length_from_stream(
        bytes,
        preamble_len + 4 + (meta_len as usize),
        &mut stream,
    )
    .await?;

    let message = root_as_message(&bytes[preamble_len + 4..]).map_err(|err| {
        ArrowError::ParseError(format!("Unable to read IPC message metadata: {err:?}"))
    })?;
    let fb_schema = message
        .header_as_schema()
        .ok_or_else(|| ArrowError::IpcError("Unable to read IPC message schema".to_string()))?;
    Ok(Arc::new(fb_to_schema(fb_schema)))
}

async fn extend_bytes_to_n_length_from_stream(
    bytes: Vec<u8>,
    n: usize,
    stream: &mut BoxStream<'static, object_store::Result<bytes::Bytes>>,
) -> Result<Vec<u8>> {
    if bytes.len() >= n {
        return Ok(bytes);
    }

    let mut buf = bytes;
    while let Some(b) = stream.next().await.transpose()? {
        buf.extend_from_slice(&b);
        if buf.len() >= n {
            break;
        }
    }

    if buf.len() < n {
        return Err(ArrowError::ParseError(
            "Unexpected end of byte stream for Arrow IPC file".to_string(),
        )
        .into());
    }

    Ok(buf)
}
