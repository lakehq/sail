//! Backend-independent typed storage access contracts and facades.

use std::convert::Infallible;
use std::marker::PhantomData;
use std::ops::Bound;

use crate::SystemStoreError;
use crate::model::{
    JobTable, MetricAttributeIndex, MetricFloatPointSeries, MetricHistogramPointSeries,
    MetricIntegerPointSeries, MetricNameIndex, MetricSeriesIdentityTable, MetricSeriesTable,
    NextMetricSeriesIdTable, OptionTable, SessionTable, StageTable, StoreIndex, StoreSeries,
    StoreTable, TaskTable, WorkerTable,
};

/// Primitive reads of one typed table.
pub trait TableReader<T: StoreTable, E> {
    fn get(&self, key: &T::Key) -> Result<Option<T::Value>, E>;
    fn scan(
        &self,
        lower: Bound<T::Key>,
        upper: Bound<T::Key>,
        visitor: &mut dyn FnMut(T::Key, T::Value) -> bool,
    ) -> Result<(), E>;
}

/// Primitive writes of one typed table.
pub trait TableWriter<T: StoreTable, E>: TableReader<T, E> {
    fn put(&mut self, key: T::Key, value: T::Value) -> Result<(), E>;
}

/// Primitive reads of one typed multi-value index.
pub trait IndexReader<T: StoreIndex, E> {
    fn scan(
        &self,
        lower: Bound<T::Key>,
        upper: Bound<T::Key>,
        visitor: &mut dyn FnMut(T::Key, T::Value) -> bool,
    ) -> Result<(), E>;
}

/// Primitive writes of one typed multi-value index.
pub trait IndexWriter<T: StoreIndex, E>: IndexReader<T, E> {
    fn put(&mut self, key: T::Key, value: T::Value) -> Result<(), E>;
}

/// Primitive reads of one typed series.
pub trait SeriesReader<T: StoreSeries, E> {
    fn scan(
        &self,
        series: &T::SeriesKey,
        lower: Bound<T::PointKey>,
        upper: Bound<T::PointKey>,
        visitor: &mut dyn FnMut(T::PointKey, T::PointValue) -> bool,
    ) -> Result<(), E>;
}

/// Primitive writes of one typed series.
pub trait SeriesWriter<T: StoreSeries, E>: SeriesReader<T, E> {
    fn put(
        &mut self,
        series: T::SeriesKey,
        point: T::PointKey,
        value: T::PointValue,
    ) -> Result<(), E>;
}

/// Reader facade for a typed table.
pub struct Table<'a, T, R, E> {
    reader: &'a R,
    marker: PhantomData<(T, E)>,
}

impl<T: StoreTable, R: TableReader<T, E>, E> Table<'_, T, R, E> {
    pub fn get(&self, key: &T::Key) -> Result<Option<T::Value>, E> {
        TableReader::<T, E>::get(self.reader, key)
    }

    pub fn scan(
        &self,
        lower: Bound<T::Key>,
        upper: Bound<T::Key>,
        visitor: &mut dyn FnMut(T::Key, T::Value) -> bool,
    ) -> Result<(), E> {
        TableReader::<T, E>::scan(self.reader, lower, upper, visitor)
    }
}

/// Writer facade for a typed table.
pub struct TableMut<'a, T, W, E> {
    writer: &'a mut W,
    marker: PhantomData<(T, E)>,
}

impl<T: StoreTable, W: TableWriter<T, E>, E> TableMut<'_, T, W, E> {
    pub fn put(&mut self, key: T::Key, value: T::Value) -> Result<(), E> {
        TableWriter::<T, E>::put(self.writer, key, value)
    }

    pub fn insert_if_absent(&mut self, key: T::Key, value: T::Value) -> Result<bool, E> {
        if TableReader::<T, E>::get(self.writer, &key)?.is_some() {
            return Ok(false);
        }
        TableWriter::<T, E>::put(self.writer, key, value)?;
        Ok(true)
    }

    pub fn update_if_present(
        &mut self,
        key: &T::Key,
        update: impl FnOnce(&mut T::Value),
    ) -> Result<bool, E> {
        let Some(mut value) = TableReader::<T, E>::get(self.writer, key)? else {
            return Ok(false);
        };
        update(&mut value);
        TableWriter::<T, E>::put(self.writer, key.clone(), value)?;
        Ok(true)
    }
}

/// Reader facade for a typed index.
pub struct Index<'a, T, R, E> {
    reader: &'a R,
    marker: PhantomData<(T, E)>,
}

impl<T: StoreIndex, R: IndexReader<T, E>, E> Index<'_, T, R, E> {
    pub fn scan(
        &self,
        lower: Bound<T::Key>,
        upper: Bound<T::Key>,
        visitor: &mut dyn FnMut(T::Key, T::Value) -> bool,
    ) -> Result<(), E> {
        IndexReader::<T, E>::scan(self.reader, lower, upper, visitor)
    }
}

/// Writer facade for a typed index.
pub struct IndexMut<'a, T, W, E> {
    writer: &'a mut W,
    marker: PhantomData<(T, E)>,
}

impl<T: StoreIndex, W: IndexWriter<T, E>, E> IndexMut<'_, T, W, E> {
    pub fn put(&mut self, key: T::Key, value: T::Value) -> Result<(), E> {
        IndexWriter::<T, E>::put(self.writer, key, value)
    }
}

/// Reader facade for a typed series.
pub struct Series<'a, T, R, E> {
    reader: &'a R,
    marker: PhantomData<(T, E)>,
}

impl<T: StoreSeries, R: SeriesReader<T, E>, E> Series<'_, T, R, E> {
    pub fn scan(
        &self,
        series: &T::SeriesKey,
        lower: Bound<T::PointKey>,
        upper: Bound<T::PointKey>,
        visitor: &mut dyn FnMut(T::PointKey, T::PointValue) -> bool,
    ) -> Result<(), E> {
        SeriesReader::<T, E>::scan(self.reader, series, lower, upper, visitor)
    }
}

/// Writer facade for a typed series.
pub struct SeriesMut<'a, T, W, E> {
    writer: &'a mut W,
    marker: PhantomData<(T, E)>,
}

impl<T: StoreSeries, W: SeriesWriter<T, E>, E> SeriesMut<'_, T, W, E> {
    pub fn put(
        &mut self,
        series: T::SeriesKey,
        point: T::PointKey,
        value: T::PointValue,
    ) -> Result<(), E> {
        SeriesWriter::<T, E>::put(self.writer, series, point, value)
    }
}

/// The complete read surface of the system store.
pub trait StoreReader:
    TableReader<OptionTable, Self::Error>
    + TableReader<SessionTable, Self::Error>
    + TableReader<JobTable, Self::Error>
    + TableReader<StageTable, Self::Error>
    + TableReader<TaskTable, Self::Error>
    + TableReader<WorkerTable, Self::Error>
    + TableReader<NextMetricSeriesIdTable, Self::Error>
    + TableReader<MetricSeriesTable, Self::Error>
    + TableReader<MetricSeriesIdentityTable, Self::Error>
    + IndexReader<MetricNameIndex, Self::Error>
    + IndexReader<MetricAttributeIndex, Self::Error>
    + SeriesReader<MetricIntegerPointSeries, Self::Error>
    + SeriesReader<MetricFloatPointSeries, Self::Error>
    + SeriesReader<MetricHistogramPointSeries, Self::Error>
{
    type Error: Into<SystemStoreError>;

    fn table<T: StoreTable>(&self) -> Table<'_, T, Self, Self::Error>
    where
        Self: Sized + TableReader<T, Self::Error>,
    {
        Table {
            reader: self,
            marker: PhantomData,
        }
    }

    fn index<T: StoreIndex>(&self) -> Index<'_, T, Self, Self::Error>
    where
        Self: Sized + IndexReader<T, Self::Error>,
    {
        Index {
            reader: self,
            marker: PhantomData,
        }
    }

    fn series<T: StoreSeries>(&self) -> Series<'_, T, Self, Self::Error>
    where
        Self: Sized + SeriesReader<T, Self::Error>,
    {
        Series {
            reader: self,
            marker: PhantomData,
        }
    }
}

/// The complete write surface of the system store.
pub trait StoreWriter:
    StoreReader
    + TableWriter<OptionTable, <Self as StoreReader>::Error>
    + TableWriter<SessionTable, <Self as StoreReader>::Error>
    + TableWriter<JobTable, <Self as StoreReader>::Error>
    + TableWriter<StageTable, <Self as StoreReader>::Error>
    + TableWriter<TaskTable, <Self as StoreReader>::Error>
    + TableWriter<WorkerTable, <Self as StoreReader>::Error>
    + TableWriter<NextMetricSeriesIdTable, <Self as StoreReader>::Error>
    + TableWriter<MetricSeriesTable, <Self as StoreReader>::Error>
    + TableWriter<MetricSeriesIdentityTable, <Self as StoreReader>::Error>
    + IndexWriter<MetricNameIndex, <Self as StoreReader>::Error>
    + IndexWriter<MetricAttributeIndex, <Self as StoreReader>::Error>
    + SeriesWriter<MetricIntegerPointSeries, <Self as StoreReader>::Error>
    + SeriesWriter<MetricFloatPointSeries, <Self as StoreReader>::Error>
    + SeriesWriter<MetricHistogramPointSeries, <Self as StoreReader>::Error>
{
    fn table_mut<T: StoreTable>(&mut self) -> TableMut<'_, T, Self, Self::Error>
    where
        Self: Sized + TableWriter<T, <Self as StoreReader>::Error>,
    {
        TableMut {
            writer: self,
            marker: PhantomData,
        }
    }

    fn index_mut<T: StoreIndex>(&mut self) -> IndexMut<'_, T, Self, Self::Error>
    where
        Self: Sized + IndexWriter<T, <Self as StoreReader>::Error>,
    {
        IndexMut {
            writer: self,
            marker: PhantomData,
        }
    }

    fn series_mut<T: StoreSeries>(&mut self) -> SeriesMut<'_, T, Self, Self::Error>
    where
        Self: Sized + SeriesWriter<T, <Self as StoreReader>::Error>,
    {
        SeriesMut {
            writer: self,
            marker: PhantomData,
        }
    }
}

/// Acquires borrowed readers and writers for an in-process backend.
pub trait DirectStoreBackend: Send + 'static {
    type Reader<'a>: StoreReader<Error = Infallible>
    where
        Self: 'a;
    type Writer<'a>: StoreWriter<Error = Infallible>
    where
        Self: 'a;
    fn read(&self) -> Self::Reader<'_>;
    fn write(&mut self) -> Self::Writer<'_>;
}

/// Commits a backend transaction.
pub trait Commit {
    type Error;
    fn commit(self) -> Result<(), Self::Error>;
}

/// Acquires owned snapshots and lifetime-bound transactions from a transactional backend.
pub trait TransactionalStoreBackend: Clone + Send + Sync + 'static {
    type Error: Into<SystemStoreError>;
    type Snapshot: StoreReader<Error = Self::Error> + Send + 'static;
    type Transaction<'a>: StoreWriter<Error = Self::Error> + Commit<Error = Self::Error>
    where
        Self: 'a;
    fn snapshot(&self) -> Result<Self::Snapshot, Self::Error>;
    fn transaction(&self) -> Result<Self::Transaction<'_>, Self::Error>;
    fn flush(&self) -> Result<(), Self::Error>;
}
