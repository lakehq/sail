//! Fjall transactional backend expressed through the typed store contracts.

use std::borrow::Borrow;
use std::ops::Bound;
use std::path::Path;

use fjall::{
    KeyspaceCreateOptions, PersistMode, Readable, SingleWriterTxDatabase, SingleWriterTxKeyspace,
    SingleWriterWriteTx, Snapshot,
};

use crate::access::{
    Commit, IndexReader, IndexWriter, SeriesReader, SeriesWriter, StoreReader, StoreWriter,
    TableReader, TableWriter, TransactionalStoreBackend,
};
use crate::backend::codec::{
    CodecError, CodecResult, NEXT_METRIC_SERIES_ID_KEY, OrderedKeyCodec, OrderedKeyCodecExt,
    ValueCodec,
};
use crate::catalog::{JobRow, OptionRow, SessionRow, StageRow, TaskRow, WorkerRow};
use crate::model::{
    JobPrimaryKey, JobTable, MetricAttributeIndex, MetricAttributeKey, MetricFloatPointSeries,
    MetricHistogramPointSeries, MetricIntegerPointSeries, MetricNameIndex, MetricPointValues,
    MetricSeriesId, MetricSeriesIdentityTable, MetricSeriesKey, MetricSeriesMetadata,
    MetricSeriesTable, NextMetricSeriesIdTable, OptionPrimaryKey, OptionTable, SessionPrimaryKey,
    SessionTable, StagePrimaryKey, StageTable, StoreIndex, StoreSeries, StoreTable, TaskPrimaryKey,
    TaskTable, WorkerPrimaryKey, WorkerTable,
};

impl From<CodecError> for fjall::Error {
    fn from(error: CodecError) -> Self {
        Self::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, error))
    }
}

#[derive(Clone)]
pub(crate) struct FjallBackend {
    pub(crate) db: SingleWriterTxDatabase,
    pub(crate) options: SingleWriterTxKeyspace,
    pub(crate) sessions: SingleWriterTxKeyspace,
    pub(crate) jobs: SingleWriterTxKeyspace,
    pub(crate) stages: SingleWriterTxKeyspace,
    pub(crate) tasks: SingleWriterTxKeyspace,
    pub(crate) workers: SingleWriterTxKeyspace,
    pub(crate) metadata: SingleWriterTxKeyspace,
    pub(crate) metric_series: SingleWriterTxKeyspace,
    pub(crate) metric_series_identities: SingleWriterTxKeyspace,
    pub(crate) metric_names: SingleWriterTxKeyspace,
    pub(crate) metric_attributes: SingleWriterTxKeyspace,
    pub(crate) metric_integer_points: SingleWriterTxKeyspace,
    pub(crate) metric_float_points: SingleWriterTxKeyspace,
    pub(crate) metric_histogram_points: SingleWriterTxKeyspace,
}

impl FjallBackend {
    pub(crate) fn open(path: impl AsRef<Path>) -> fjall::Result<Self> {
        let path = path.as_ref();
        if !path.is_dir() {
            return Err(fjall::Error::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!(
                    "system store path does not exist or is not a directory: {}",
                    path.display()
                ),
            )));
        }
        let db = SingleWriterTxDatabase::builder(path).open()?;
        let open_keyspace = |name| db.keyspace(name, KeyspaceCreateOptions::default);
        Ok(Self {
            options: open_keyspace(OptionTable::NAME)?,
            sessions: open_keyspace(SessionTable::NAME)?,
            jobs: open_keyspace(JobTable::NAME)?,
            stages: open_keyspace(StageTable::NAME)?,
            tasks: open_keyspace(TaskTable::NAME)?,
            workers: open_keyspace(WorkerTable::NAME)?,
            metadata: open_keyspace(NextMetricSeriesIdTable::NAME)?,
            metric_series: open_keyspace(MetricSeriesTable::NAME)?,
            metric_series_identities: open_keyspace(MetricSeriesIdentityTable::NAME)?,
            metric_names: open_keyspace(MetricNameIndex::NAME)?,
            metric_attributes: open_keyspace(MetricAttributeIndex::NAME)?,
            metric_integer_points: open_keyspace(MetricIntegerPointSeries::NAME)?,
            metric_float_points: open_keyspace(MetricFloatPointSeries::NAME)?,
            metric_histogram_points: open_keyspace(MetricHistogramPointSeries::NAME)?,
            db,
        })
    }
}

/// Adapts a Fjall reader and backend handle to the typed store contracts.
pub(crate) struct FjallAccessor<I, B> {
    inner: I,
    backend: B,
}

pub(crate) type FjallSnapshot = FjallAccessor<Snapshot, FjallBackend>;
pub(crate) type FjallTransaction<'a> = FjallAccessor<SingleWriterWriteTx<'a>, &'a FjallBackend>;

fn encoded_bound<K>(bound: Bound<K>, encode: impl Fn(&K) -> Vec<u8>) -> Bound<Vec<u8>> {
    match bound {
        Bound::Included(key) => Bound::Included(encode(&key)),
        Bound::Excluded(key) => Bound::Excluded(encode(&key)),
        Bound::Unbounded => Bound::Unbounded,
    }
}

fn table_scan<R, K, V>(
    reader: &R,
    keyspace: &SingleWriterTxKeyspace,
    lower: Bound<K>,
    upper: Bound<K>,
    encode: impl Fn(&K) -> Vec<u8>,
    decode: impl Fn(&[u8]) -> CodecResult<K>,
    visitor: &mut dyn FnMut(K, V) -> bool,
) -> fjall::Result<()>
where
    R: Readable,
    V: ValueCodec,
{
    let lower = encoded_bound(lower, &encode);
    let upper = encoded_bound(upper, &encode);
    for guard in reader.range(keyspace, (lower, upper)) {
        let (key, value) = guard.into_inner()?;
        let key = decode(&key)?;
        let value = V::decode_value(&value)?;
        if !visitor(key, value) {
            break;
        }
    }
    Ok(())
}

macro_rules! table {
    ($marker:ty, $key:ty, $value:ty, $keyspace:ident, $encode:expr, $decode:expr) => {
        impl<I, B> TableReader<$marker, fjall::Error> for FjallAccessor<I, B>
        where
            I: Readable,
            B: Borrow<FjallBackend>,
        {
            fn get(&self, key: &$key) -> Result<Option<$value>, fjall::Error> {
                let Some(value) = self
                    .inner
                    .get(&self.backend.borrow().$keyspace, $encode(key))?
                else {
                    return Ok(None);
                };
                Ok(Some(<$value as ValueCodec>::decode_value(&value)?))
            }

            fn scan(
                &self,
                lower: Bound<$key>,
                upper: Bound<$key>,
                visitor: &mut dyn FnMut($key, $value) -> bool,
            ) -> Result<(), fjall::Error> {
                let backend = self.backend.borrow();
                table_scan(
                    &self.inner,
                    &backend.$keyspace,
                    lower,
                    upper,
                    $encode,
                    $decode,
                    visitor,
                )
            }
        }

        impl<B> TableWriter<$marker, fjall::Error> for FjallAccessor<SingleWriterWriteTx<'_>, B>
        where
            B: Borrow<FjallBackend>,
        {
            fn put(&mut self, key: $key, value: $value) -> Result<(), fjall::Error> {
                self.inner.insert(
                    &self.backend.borrow().$keyspace,
                    $encode(&key),
                    <$value as ValueCodec>::encode_value(&value)?,
                );
                Ok(())
            }
        }
    };
}

table!(
    OptionTable,
    OptionPrimaryKey,
    OptionRow,
    options,
    |key: &OptionPrimaryKey| key.encoded_key(),
    OptionPrimaryKey::decode_key
);
table!(
    SessionTable,
    SessionPrimaryKey,
    SessionRow,
    sessions,
    |key: &SessionPrimaryKey| key.encoded_key(),
    SessionPrimaryKey::decode_key
);
table!(
    JobTable,
    JobPrimaryKey,
    JobRow,
    jobs,
    |key: &JobPrimaryKey| key.encoded_key(),
    JobPrimaryKey::decode_key
);
table!(
    StageTable,
    StagePrimaryKey,
    StageRow,
    stages,
    |key: &StagePrimaryKey| key.encoded_key(),
    StagePrimaryKey::decode_key
);
table!(
    TaskTable,
    TaskPrimaryKey,
    TaskRow,
    tasks,
    |key: &TaskPrimaryKey| key.encoded_key(),
    TaskPrimaryKey::decode_key
);
table!(
    WorkerTable,
    WorkerPrimaryKey,
    WorkerRow,
    workers,
    |key: &WorkerPrimaryKey| key.encoded_key(),
    WorkerPrimaryKey::decode_key
);
table!(
    NextMetricSeriesIdTable,
    (),
    MetricSeriesId,
    metadata,
    |_key: &()| NEXT_METRIC_SERIES_ID_KEY.to_vec(),
    |_key: &[u8]| Ok(())
);
table!(
    MetricSeriesTable,
    MetricSeriesId,
    MetricSeriesMetadata,
    metric_series,
    |key: &MetricSeriesId| key.encoded_key(),
    MetricSeriesId::decode_key
);
table!(
    MetricSeriesIdentityTable,
    MetricSeriesKey,
    MetricSeriesId,
    metric_series_identities,
    |key: &MetricSeriesKey| key.encoded_key(),
    MetricSeriesKey::decode_key
);

fn index_scan<R, K, V>(
    reader: &R,
    keyspace: &SingleWriterTxKeyspace,
    lower: Bound<Vec<u8>>,
    upper: Bound<Vec<u8>>,
    decode: impl Fn(&[u8], &[u8]) -> CodecResult<(K, V)>,
    visitor: &mut dyn FnMut(K, V) -> bool,
) -> fjall::Result<()>
where
    R: Readable,
{
    for guard in reader.range(keyspace, (lower, upper)) {
        let (key, value) = guard.into_inner()?;
        let (key, value) = decode(&key, &value)?;
        if !visitor(key, value) {
            break;
        }
    }
    Ok(())
}

fn prefix_end(mut prefix: Vec<u8>) -> Option<Vec<u8>> {
    while let Some(byte) = prefix.pop() {
        if byte != u8::MAX {
            prefix.push(byte + 1);
            return Some(prefix);
        }
    }
    None
}

fn index_lower_bound<K: OrderedKeyCodec>(bound: Bound<K>) -> Bound<Vec<u8>> {
    match bound {
        Bound::Included(key) => Bound::Included(key.encoded_key()),
        Bound::Excluded(key) => prefix_end(key.encoded_key())
            .map(Bound::Included)
            .unwrap_or(Bound::Unbounded),
        Bound::Unbounded => Bound::Unbounded,
    }
}

fn index_upper_bound<K: OrderedKeyCodec>(bound: Bound<K>) -> Bound<Vec<u8>> {
    match bound {
        Bound::Included(key) => prefix_end(key.encoded_key())
            .map(Bound::Excluded)
            .unwrap_or(Bound::Unbounded),
        Bound::Excluded(key) => Bound::Excluded(key.encoded_key()),
        Bound::Unbounded => Bound::Unbounded,
    }
}

macro_rules! index {
    (
        $marker:ty,
        $key:ty,
        $value:ty,
        $keyspace:ident,
        $lower_bound:expr,
        $upper_bound:expr,
        $decode:expr,
        $encode:expr
    ) => {
        impl<I, B> IndexReader<$marker, fjall::Error> for FjallAccessor<I, B>
        where
            I: Readable,
            B: Borrow<FjallBackend>,
        {
            fn scan(
                &self,
                lower: Bound<$key>,
                upper: Bound<$key>,
                visitor: &mut dyn FnMut($key, $value) -> bool,
            ) -> Result<(), fjall::Error> {
                let backend = self.backend.borrow();
                index_scan(
                    &self.inner,
                    &backend.$keyspace,
                    ($lower_bound)(lower),
                    ($upper_bound)(upper),
                    $decode,
                    visitor,
                )
            }
        }

        impl<B> IndexWriter<$marker, fjall::Error> for FjallAccessor<SingleWriterWriteTx<'_>, B>
        where
            B: Borrow<FjallBackend>,
        {
            fn put(&mut self, key: $key, value: $value) -> Result<(), fjall::Error> {
                let (key, value) = ($encode)(key, value);
                self.inner
                    .insert(&self.backend.borrow().$keyspace, key, value);
                Ok(())
            }
        }
    };
}

index!(
    MetricNameIndex,
    String,
    MetricSeriesId,
    metric_names,
    index_lower_bound,
    index_upper_bound,
    |key, _value| <(String, MetricSeriesId)>::decode_key(key),
    |key: String, value: MetricSeriesId| ((key, value).encoded_key(), Vec::<u8>::new())
);
index!(
    MetricAttributeIndex,
    MetricAttributeKey,
    MetricSeriesId,
    metric_attributes,
    index_lower_bound,
    index_upper_bound,
    |key, _value| <(MetricAttributeKey, MetricSeriesId)>::decode_key(key),
    |key: MetricAttributeKey, value: MetricSeriesId| ((key, value).encoded_key(), Vec::<u8>::new(),)
);

fn series_bounds(
    id: MetricSeriesId,
    lower: Bound<crate::predicate::TimestampMicros>,
    upper: Bound<crate::predicate::TimestampMicros>,
) -> (Bound<Vec<u8>>, Bound<Vec<u8>>) {
    let lower = match lower {
        Bound::Included(point) => Bound::Included((id, point).encoded_key()),
        Bound::Excluded(point) => Bound::Excluded((id, point).encoded_key()),
        Bound::Unbounded => Bound::Included(id.encoded_key()),
    };
    let upper = match upper {
        Bound::Included(point) => Bound::Included((id, point).encoded_key()),
        Bound::Excluded(point) => Bound::Excluded((id, point).encoded_key()),
        Bound::Unbounded => match id.checked_add(1) {
            Some(next) => Bound::Excluded(next.encoded_key()),
            None => Bound::Unbounded,
        },
    };
    (lower, upper)
}

fn series_scan<R, V>(
    reader: &R,
    keyspace: &SingleWriterTxKeyspace,
    id: MetricSeriesId,
    lower: Bound<crate::predicate::TimestampMicros>,
    upper: Bound<crate::predicate::TimestampMicros>,
    visitor: &mut dyn FnMut(crate::predicate::TimestampMicros, V) -> bool,
) -> fjall::Result<()>
where
    R: Readable,
    V: Clone + serde::Serialize + serde::de::DeserializeOwned,
{
    let (lower, upper) = series_bounds(id, lower, upper);
    for guard in reader.range(keyspace, (lower, upper)) {
        let (key, value) = guard.into_inner()?;
        let (_, timestamp) =
            <(MetricSeriesId, crate::predicate::TimestampMicros)>::decode_key(&key)?;
        let values = MetricPointValues::<V>::decode_value(&value)?;
        for value in values {
            if !visitor(timestamp, value) {
                return Ok(());
            }
        }
    }
    Ok(())
}

impl<B> FjallAccessor<SingleWriterWriteTx<'_>, B>
where
    B: Borrow<FjallBackend>,
{
    /// Loads the existing value or initializes `V::default()`, invokes `modify`, then writes the
    /// resulting value back to the keyspace.
    fn update_or_insert_with<V>(
        &mut self,
        keyspace: &SingleWriterTxKeyspace,
        key: Vec<u8>,
        modify: impl FnOnce(&mut V),
    ) -> Result<(), fjall::Error>
    where
        V: Default + ValueCodec,
    {
        let mut value = match self.inner.get(keyspace, &key)? {
            Some(value) => V::decode_value(&value)?,
            None => V::default(),
        };
        modify(&mut value);
        self.inner.insert(keyspace, key, value.encode_value()?);
        Ok(())
    }
}

macro_rules! series {
    ($marker:ty, $value:ty, $keyspace:ident) => {
        impl<I, B> SeriesReader<$marker, fjall::Error> for FjallAccessor<I, B>
        where
            I: Readable,
            B: Borrow<FjallBackend>,
        {
            fn scan(
                &self,
                series: &MetricSeriesId,
                lower: Bound<crate::predicate::TimestampMicros>,
                upper: Bound<crate::predicate::TimestampMicros>,
                visitor: &mut dyn FnMut(crate::predicate::TimestampMicros, $value) -> bool,
            ) -> Result<(), fjall::Error> {
                series_scan(
                    &self.inner,
                    &self.backend.borrow().$keyspace,
                    *series,
                    lower,
                    upper,
                    visitor,
                )
            }
        }

        impl<B> SeriesWriter<$marker, fjall::Error> for FjallAccessor<SingleWriterWriteTx<'_>, B>
        where
            B: Borrow<FjallBackend>,
        {
            fn put(
                &mut self,
                series: MetricSeriesId,
                timestamp: crate::predicate::TimestampMicros,
                value: $value,
            ) -> Result<(), fjall::Error> {
                let keyspace = self.backend.borrow().$keyspace.clone();
                self.update_or_insert_with::<MetricPointValues<$value>>(
                    &keyspace,
                    (series, timestamp).encoded_key(),
                    |existing| existing.push(value),
                )
            }
        }
    };
}

series!(MetricIntegerPointSeries, i64, metric_integer_points);
series!(MetricFloatPointSeries, f64, metric_float_points);
series!(
    MetricHistogramPointSeries,
    crate::types::MetricHistogram,
    metric_histogram_points
);

impl<I, B> StoreReader for FjallAccessor<I, B>
where
    I: Readable,
    B: Borrow<FjallBackend>,
{
    type Error = fjall::Error;
}

impl<B> StoreWriter for FjallAccessor<SingleWriterWriteTx<'_>, B> where B: Borrow<FjallBackend> {}

impl TransactionalStoreBackend for FjallBackend {
    type Error = fjall::Error;
    type Snapshot = FjallSnapshot;
    type Transaction<'a> = FjallTransaction<'a>;

    fn snapshot(&self) -> Result<Self::Snapshot, Self::Error> {
        Ok(FjallAccessor {
            inner: self.db.read_tx(),
            backend: self.clone(),
        })
    }

    fn transaction(&self) -> Result<Self::Transaction<'_>, Self::Error> {
        Ok(FjallAccessor {
            inner: self.db.write_tx(),
            backend: self,
        })
    }

    fn flush(&self) -> Result<(), Self::Error> {
        self.db.persist(PersistMode::SyncAll)
    }
}

impl Commit for FjallTransaction<'_> {
    type Error = fjall::Error;

    fn commit(self) -> Result<(), Self::Error> {
        self.inner.commit()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::ops::Bound;

    use super::FjallBackend;
    use crate::access::{Commit, StoreReader, TransactionalStoreBackend};
    use crate::engine::{MetricSample, write_event, write_metrics};
    use crate::model::{
        MetricAttributeIndex, MetricAttributeKey, MetricIntegerPointSeries, MetricNameIndex,
        OptionPrimaryKey, OptionTable,
    };
    use crate::predicate::TimestampMicros;
    use crate::types::{MetricNumber, MetricValue};
    use crate::{SystemEvent, SystemStoreResult};

    #[test]
    fn persists_typed_table_and_series_writes() -> SystemStoreResult<()> {
        let directory = tempfile::tempdir().map_err(fjall::Error::from)?;
        let store = FjallBackend::open(directory.path())?;
        let mut transaction = store.transaction()?;
        write_event(
            &mut transaction,
            SystemEvent::OptionCreated {
                key: "key".to_string(),
                value: "value".to_string(),
            },
        )?;
        write_metrics(
            &mut transaction,
            vec![
                MetricSample {
                    name: "sail.metric".to_string(),
                    attributes: BTreeMap::from([("host".to_string(), "one".to_string())]),
                    timestamp: TimestampMicros(1),
                    value: MetricValue::Gauge(MetricNumber::Integer(1)),
                },
                MetricSample {
                    name: "sail.metric".to_string(),
                    attributes: BTreeMap::from([("host".to_string(), "one".to_string())]),
                    timestamp: TimestampMicros(1),
                    value: MetricValue::Gauge(MetricNumber::Integer(2)),
                },
            ],
        )?;
        transaction.commit()?;
        drop(store);

        let reopened = FjallBackend::open(directory.path())?;
        let snapshot = reopened.snapshot()?;
        assert_eq!(
            snapshot
                .table::<OptionTable>()
                .get(&OptionPrimaryKey {
                    key: "key".to_string(),
                })?
                .map(|row| row.value),
            Some("value".to_string())
        );
        let mut points = Vec::new();
        snapshot.series::<MetricIntegerPointSeries>().scan(
            &0,
            Bound::Unbounded,
            Bound::Unbounded,
            &mut |point, value| {
                points.push((point, value));
                true
            },
        )?;
        assert_eq!(
            points,
            vec![(TimestampMicros(1), 1), (TimestampMicros(1), 2)]
        );
        let mut name_ids = Vec::new();
        snapshot.index::<MetricNameIndex>().scan(
            Bound::Included("sail.metric".to_string()),
            Bound::Included("sail.metric".to_string()),
            &mut |_, id| {
                name_ids.push(id);
                true
            },
        )?;
        assert_eq!(name_ids, vec![0]);
        let attribute = MetricAttributeKey {
            key: "host".to_string(),
            value: "one".to_string(),
        };
        let mut attribute_ids = Vec::new();
        snapshot.index::<MetricAttributeIndex>().scan(
            Bound::Included(attribute.clone()),
            Bound::Included(attribute),
            &mut |_, id| {
                attribute_ids.push(id);
                true
            },
        )?;
        assert_eq!(attribute_ids, vec![0]);
        Ok(())
    }
}
