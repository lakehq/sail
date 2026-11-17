//! The queues that hold the output of one task in memory until it is read.
//!
//! A task has one writer and any number of queues, each of which is read by one
//! reader. All queues of the task share one state behind one lock, so that every
//! change that the writer or a reader waits for is made, and the one who waits is
//! found, in the same critical section.

use std::collections::VecDeque;
use std::future::poll_fn;
use std::ops::Range;
use std::pin::{Pin, pin};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use futures::Stream;
use tokio::sync::Notify;

/// How long a writer waits for its readers while none of them takes anything,
/// before it checks whether one of them waits for the writer in turn.
pub(crate) const STALL_TIMEOUT: Duration = Duration::from_secs(300);

/// The memory that the queues of all tasks of a process hold together.
#[derive(Default)]
pub(crate) struct StreamMemory {
    /// The size in bytes of the items in the queues.
    used: AtomicUsize,
    /// The ceiling that a writer has waited at, or zero if no writer has waited.
    /// The writers of a process have the same ceiling.
    ceiling: AtomicUsize,
    /// Notifies the writers that wait at the ceiling when the size falls below it.
    released: Notify,
}

impl StreamMemory {
    #[cfg(test)]
    pub(crate) fn used(&self) -> usize {
        self.used.load(Ordering::Relaxed)
    }
}

/// The bytes of one item, which count for the memory of the process until the
/// item is gone from all queues of its channel. The queues of a channel hold the
/// same data, which stays in memory as long as any of them has the item.
struct Charge {
    memory: Arc<StreamMemory>,
    bytes: usize,
}

impl Charge {
    fn new(memory: &Arc<StreamMemory>, bytes: usize) -> Arc<Self> {
        memory.used.fetch_add(bytes, Ordering::Relaxed);
        Arc::new(Self {
            memory: memory.clone(),
            bytes,
        })
    }
}

impl Drop for Charge {
    fn drop(&mut self) {
        // A writer that waits at the ceiling has stored the ceiling before it looked
        // at the size. So either the writer sees the size without these bytes, or
        // the ceiling is seen here and the writer is notified.
        let memory = &self.memory;
        let used = memory.used.fetch_sub(self.bytes, Ordering::SeqCst);
        let ceiling = memory.ceiling.load(Ordering::SeqCst);
        if used >= ceiling && used - self.bytes < ceiling {
            memory.released.notify_waiters();
        }
    }
}

/// When the writer of the queues of a task waits for the readers.
#[derive(Debug, Clone, Copy)]
pub(crate) struct QueueLimit {
    /// The number of bytes that the queues of the writer can always grow to.
    pub bytes: usize,
    /// The number of bytes that the queues of all tasks of the process can hold
    /// before the writers wait. With one byte, a writer waits as soon as its own
    /// queues hold the bytes above. A ceiling of [`usize::MAX`] is never reached.
    pub ceiling: usize,
}

/// Creates the queues for one writer, and one reader for each queue. Each channel
/// of the writer has the given number of queues, which all get the items that are
/// written to the channel. The readers are returned in the order of the channels.
///
/// With a limit, the writer waits for the readers while both of the following hold.
/// Its queues together have at least the bytes of the limit, and the queues in the
/// given memory of the process have reached the ceiling of the limit. A write into
/// empty queues never waits, whatever its size, and neither does a write of items
/// without bytes. Without a limit the writer never waits. The items count for the
/// memory in either case.
///
/// The writer does not continue because a reader waits for an item. If a reader
/// only takes its next item once the writer has added more items for the other
/// readers, both wait for each other. The readers must therefore take the items
/// as they come. As a last resort, the writer stops waiting for good when no
/// reader has taken anything for some time while one of them waits for an item
/// and the write still has to wait.
///
/// `size` returns the size of an item in bytes.
pub(crate) fn task_queues<T>(
    channels: &[usize],
    limit: Option<QueueLimit>,
    memory: Arc<StreamMemory>,
    size: fn(&T) -> usize,
) -> (QueueWriter<T>, Vec<QueueReader<T>>) {
    let mut queues = 0;
    let channels = channels
        .iter()
        .map(|count| {
            let start = queues;
            queues += count;
            start..queues
        })
        .collect();
    let state = State {
        queues: (0..queues)
            .map(|_| Queue {
                items: VecDeque::new(),
                reader: None,
                closed: false,
            })
            .collect(),
        bytes: 0,
        live_readers: queues,
        waiting_readers: 0,
        taken: 0,
        writer: None,
        ended: false,
        limit,
    };
    let shared = Arc::new(Shared {
        state: Mutex::new(state),
        memory,
    });
    let readers = (0..queues)
        .map(|index| QueueReader {
            shared: shared.clone(),
            index,
        })
        .collect();
    let writer = QueueWriter {
        shared,
        channels,
        size,
        wakers: vec![],
    };
    (writer, readers)
}

struct Shared<T> {
    state: Mutex<State<T>>,
    memory: Arc<StreamMemory>,
}

impl<T> Shared<T> {
    fn lock(&self) -> MutexGuard<'_, State<T>> {
        // The state is consistent whenever the lock is released.
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

struct State<T> {
    queues: Vec<Queue<T>>,
    /// The bytes of the items in all queues.
    bytes: usize,
    /// The number of readers that have not been dropped.
    live_readers: usize,
    /// The number of readers that wait for an item.
    waiting_readers: usize,
    /// The number of items that the readers have taken.
    taken: u64,
    /// The writer if it waits for the readers.
    writer: Option<Waker>,
    /// Whether the writer has finished or has been dropped.
    ended: bool,
    limit: Option<QueueLimit>,
}

struct Queue<T> {
    items: VecDeque<Item<T>>,
    /// The reader if it waits for an item.
    reader: Option<Waker>,
    /// Whether the reader has been dropped. A closed queue has no items.
    closed: bool,
}

struct Item<T> {
    value: T,
    /// The bytes of the item, if it has any.
    charge: Option<Arc<Charge>>,
}

impl<T> Item<T> {
    fn bytes(&self) -> usize {
        self.charge.as_ref().map_or(0, |charge| charge.bytes)
    }
}

impl<T> State<T> {
    /// Whether a write with bytes has to wait. This is the only place that decides it,
    /// for the writer and for the readers that look for a writer to wake up.
    fn is_full(&self, memory: &StreamMemory) -> bool {
        let Some(limit) = self.limit else {
            return false;
        };
        self.bytes > 0
            && self.bytes >= limit.bytes
            && memory.used.load(Ordering::SeqCst) >= limit.ceiling
    }

    fn is_consistent(&self) -> bool {
        let items = || self.queues.iter().flat_map(|queue| queue.items.iter());
        let live = self.queues.iter().filter(|queue| !queue.closed).count();
        let waiting = self
            .queues
            .iter()
            .filter(|queue| queue.reader.is_some())
            .count();
        items().map(Item::bytes).sum::<usize>() == self.bytes
            && live == self.live_readers
            && waiting == self.waiting_readers
            && self
                .queues
                .iter()
                .all(|queue| queue.items.is_empty() || (!queue.closed && queue.reader.is_none()))
    }
}

/// The outcome of a write to the queues of a task.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum QueueWrite {
    /// The items are in the queues.
    Queued,
    /// The items are in the queues, and the writer has stopped waiting for its
    /// readers for good, since none of them had taken anything for some time while
    /// one of them waited for an item. This is the outcome of one write at most.
    QueuedAfterStall,
    /// All readers are gone, and no item has been added.
    Closed,
}

enum Admission {
    Queued,
    Closed,
    /// The write has to wait. This is the number of items taken so far.
    Full {
        taken: u64,
    },
}

/// The writer of all queues of a task. The readers get the items that remain in
/// their queues, followed by the end, once the writer has finished or has been
/// dropped.
pub(crate) struct QueueWriter<T> {
    shared: Arc<Shared<T>>,
    /// The queues of each channel.
    channels: Vec<Range<usize>>,
    size: fn(&T) -> usize,
    /// The readers to wake up after a write, kept here to reuse the allocation.
    wakers: Vec<Waker>,
}

impl<T: Clone> QueueWriter<T> {
    /// Adds at most one item to the queues of each channel, after waiting for the
    /// readers as described in [`task_queues`]. Either all items are added or none
    /// of them, so nothing is added if the returned future is dropped before it
    /// completes. An item is not added to the queues whose reader is gone.
    pub(crate) async fn write(&mut self, items: Vec<Option<T>>) -> QueueWrite {
        let mut stalled = false;
        loop {
            let taken = match self.admit(&items, None) {
                Admission::Queued => break,
                Admission::Closed => return QueueWrite::Closed,
                Admission::Full { taken } => taken,
            };
            // The write is tried again when the readers of this task take items, and
            // when the memory of the process falls below the ceiling since the readers
            // of other tasks take items. The writer registers for the latter before
            // it finds that it has to wait.
            let memory = self.shared.memory.clone();
            let mut released = pin!(memory.released.notified());
            let admission = poll_fn(|cx| {
                while released.as_mut().poll(cx).is_ready() {
                    released.set(memory.released.notified());
                }
                match self.admit(&items, Some(cx.waker())) {
                    Admission::Full { .. } => Poll::Pending,
                    admission => Poll::Ready(admission),
                }
            });
            let admission = tokio::time::timeout(STALL_TIMEOUT, admission).await;
            match admission {
                Ok(Admission::Closed) => return QueueWrite::Closed,
                Ok(_) => break,
                Err(_) => stalled |= self.lift_limit_if_stalled(taken),
            }
        }
        if stalled {
            QueueWrite::QueuedAfterStall
        } else {
            QueueWrite::Queued
        }
    }

    /// Adds the items to the queues unless the write has to wait.
    /// In that case the waker is notified when the write should be tried again.
    fn admit(&mut self, items: &[Option<T>], waker: Option<&Waker>) -> Admission {
        let memory = &self.shared.memory;
        let mut guard = self.shared.lock();
        let state = &mut *guard;
        debug_assert_eq!(items.len(), self.channels.len());
        if state.live_readers == 0 {
            return Admission::Closed;
        }
        let open = |queues: &Range<usize>| {
            let queues = &state.queues[queues.clone()];
            queues.iter().filter(|queue| !queue.closed).count()
        };
        let bytes = self
            .channels
            .iter()
            .zip(items.iter())
            .filter_map(|(queues, item)| Some(open(queues) * (self.size)(item.as_ref()?)))
            .sum::<usize>();
        if let (Some(_), Some(limit)) = (waker, state.limit) {
            memory.ceiling.store(limit.ceiling, Ordering::SeqCst);
        }
        if bytes > 0 && state.is_full(memory) {
            if let Some(waker) = waker
                && !state.writer.as_ref().is_some_and(|x| x.will_wake(waker))
            {
                state.writer = Some(waker.clone());
            }
            return Admission::Full { taken: state.taken };
        }
        state.writer = None;
        for (queues, item) in self.channels.iter().zip(items) {
            let Some(value) = item else {
                continue;
            };
            let queues = &mut state.queues[queues.clone()];
            let mut queues = queues.iter_mut().filter(|queue| !queue.closed).peekable();
            if queues.peek().is_none() {
                continue;
            }
            let size = (self.size)(value);
            let charge = (size > 0).then(|| Charge::new(memory, size));
            for queue in queues {
                queue.items.push_back(Item {
                    value: value.clone(),
                    charge: charge.clone(),
                });
                if let Some(reader) = queue.reader.take() {
                    state.waiting_readers -= 1;
                    self.wakers.push(reader);
                }
            }
        }
        state.bytes += bytes;
        debug_assert!(state.is_consistent());
        drop(guard);
        for waker in self.wakers.drain(..) {
            waker.wake();
        }
        Admission::Queued
    }

    /// Lifts the limit if no reader has taken anything since the given number of
    /// items was taken, while a reader waits for an item. The limit stays if the
    /// writer no longer has to wait.
    fn lift_limit_if_stalled(&self, taken: u64) -> bool {
        let mut state = self.shared.lock();
        let stalled =
            state.taken == taken && state.waiting_readers > 0 && state.is_full(&self.shared.memory);
        if stalled {
            state.limit = None;
        }
        stalled
    }

    /// Ends the queues, which is the same as dropping the writer.
    pub(crate) fn finish(self) {}
}

impl<T> Drop for QueueWriter<T> {
    /// Ends the queues. The items stay for their readers, since a reader may as
    /// well have taken them before the writer was dropped.
    fn drop(&mut self) {
        let mut guard = self.shared.lock();
        let state = &mut *guard;
        state.ended = true;
        state.writer = None;
        for queue in state.queues.iter_mut() {
            self.wakers.extend(queue.reader.take());
        }
        state.waiting_readers = 0;
        debug_assert!(state.is_consistent());
        drop(guard);
        for waker in self.wakers.drain(..) {
            waker.wake();
        }
    }
}

/// The reader of one queue. It is a stream of the items in the queue, which ends
/// when the writer has finished or has been dropped. The queue is closed when the
/// reader is dropped.
pub(crate) struct QueueReader<T> {
    shared: Arc<Shared<T>>,
    index: usize,
}

impl<T> Stream for QueueReader<T> {
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let memory = &self.shared.memory;
        let mut guard = self.shared.lock();
        let state = &mut *guard;
        let queue = &mut state.queues[self.index];
        if let Some(item) = queue.items.pop_front() {
            state.bytes -= item.bytes();
            state.taken = state.taken.wrapping_add(1);
            // The bytes of the item may no longer count, which the writer has to see.
            drop(item.charge);
            let writer = if state.writer.is_some() && !state.is_full(memory) {
                state.writer.take()
            } else {
                None
            };
            debug_assert!(state.is_consistent());
            drop(guard);
            if let Some(writer) = writer {
                writer.wake();
            }
            return Poll::Ready(Some(item.value));
        }
        if state.ended {
            return Poll::Ready(None);
        }
        if queue.reader.is_none() {
            state.waiting_readers += 1;
        }
        if !queue
            .reader
            .as_ref()
            .is_some_and(|x| x.will_wake(cx.waker()))
        {
            queue.reader = Some(cx.waker().clone());
        }
        Poll::Pending
    }
}

impl<T> Drop for QueueReader<T> {
    fn drop(&mut self) {
        let mut guard = self.shared.lock();
        let state = &mut *guard;
        let queue = &mut state.queues[self.index];
        queue.closed = true;
        let items = std::mem::take(&mut queue.items);
        if queue.reader.take().is_some() {
            state.waiting_readers -= 1;
        }
        state.bytes -= items.iter().map(Item::bytes).sum::<usize>();
        state.live_readers -= 1;
        // The writer may not have to wait anymore, or may find that all readers are gone.
        let writer = state.writer.take();
        debug_assert!(state.is_consistent());
        drop(guard);
        drop(items);
        if let Some(writer) = writer {
            writer.wake();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::pin::pin;

    use futures::task::ArcWake;
    use futures::{FutureExt, StreamExt, poll};
    use rand::rngs::StdRng;
    use rand::{RngExt, SeedableRng};
    use tokio::time::Instant;

    use super::*;

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    fn value(item: &usize) -> usize {
        *item
    }

    fn one<T>(_: &T) -> usize {
        1
    }

    fn limit(bytes: usize, ceiling: usize) -> Option<QueueLimit> {
        Some(QueueLimit { bytes, ceiling })
    }

    /// Creates one queue for each of the given number of channels, in a process
    /// that has no memory left for them, so that the writer waits whenever its
    /// queues have the given number of bytes.
    fn strict_queues<T>(
        channels: usize,
        bytes: Option<usize>,
        size: fn(&T) -> usize,
    ) -> (QueueWriter<T>, Vec<QueueReader<T>>) {
        let limit = bytes.and_then(|bytes| limit(bytes, 1));
        task_queues(&vec![1; channels], limit, Arc::default(), size)
    }

    /// Completes a future that no longer has to wait.
    async fn soon<F: Future>(future: F) -> Result<F::Output, tokio::time::error::Elapsed> {
        tokio::time::timeout(Duration::from_secs(5), future).await
    }

    fn write_now<T: Clone>(writer: &mut QueueWriter<T>, items: Vec<Option<T>>) {
        let outcome = writer.write(items).now_or_never();
        assert_eq!(outcome, Some(QueueWrite::Queued));
    }

    async fn read_all<T>(reader: QueueReader<T>) -> Vec<T> {
        reader.collect().await
    }

    /// Counts how many times a waker is notified.
    #[derive(Default)]
    struct Wakes(AtomicUsize);

    impl ArcWake for Wakes {
        fn wake_by_ref(arc_self: &Arc<Self>) {
            arc_self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    impl Wakes {
        fn count(&self) -> usize {
            self.0.load(Ordering::SeqCst)
        }
    }

    #[tokio::test]
    async fn write_waits_at_the_limit_until_the_readers_take_enough() -> TestResult {
        let (mut writer, mut readers) = strict_queues(2, Some(4), value);
        write_now(&mut writer, vec![Some(3), Some(1)]);
        {
            let mut write = pin!(writer.write(vec![Some(1), None]));
            assert!(poll!(&mut write).is_pending());
            // Any reader can make room for the writer.
            assert_eq!(readers[1].next().await, Some(1));
            assert_eq!(soon(write).await?, QueueWrite::Queued);
        }
        // The queues have four bytes again.
        {
            let mut write = pin!(writer.write(vec![None, Some(2)]));
            assert!(poll!(&mut write).is_pending());
            assert_eq!(readers[0].next().await, Some(3));
            assert_eq!(soon(write).await?, QueueWrite::Queued);
        }

        let (mut writer, mut readers) = strict_queues(2, Some(12), value);
        for _ in 0..3 {
            write_now(&mut writer, vec![Some(3), Some(1)]);
        }
        {
            let mut write = pin!(writer.write(vec![Some(3), Some(1)]));
            assert!(poll!(&mut write).is_pending());
            assert_eq!(readers[1].next().await, Some(1));
            assert_eq!(soon(write).await?, QueueWrite::Queued);
        }
        Ok(())
    }

    #[tokio::test]
    async fn write_waits_only_for_empty_queues_when_the_limit_is_zero() -> TestResult {
        let (mut writer, mut readers) = strict_queues(2, Some(0), value);
        write_now(&mut writer, vec![Some(1), Some(1)]);
        {
            let mut write = pin!(writer.write(vec![Some(5), Some(5)]));
            assert!(poll!(&mut write).is_pending());
            assert_eq!(readers[0].next().await, Some(1));
            assert!(poll!(&mut write).is_pending());
            assert_eq!(readers[1].next().await, Some(1));
            // A write into empty queues never waits, whatever its size.
            assert_eq!(soon(write).await?, QueueWrite::Queued);
        }
        assert_eq!(readers[0].next().await, Some(5));
        assert_eq!(readers[1].next().await, Some(5));
        write_now(&mut writer, vec![Some(100), None]);
        Ok(())
    }

    #[tokio::test]
    async fn write_waits_although_a_reader_waits_for_an_item() -> TestResult {
        let (mut writer, mut readers) = strict_queues(2, Some(1), one);
        write_now(&mut writer, vec![Some(0), None]);
        assert!(poll!(readers[1].next()).is_pending());
        let mut write = pin!(writer.write(vec![Some(1), None]));
        assert!(poll!(&mut write).is_pending());
        assert!(poll!(readers[1].next()).is_pending());
        assert!(poll!(&mut write).is_pending());
        // Only the reader that has items lets the writer continue.
        assert_eq!(readers[0].next().await, Some(0));
        assert_eq!(soon(write).await?, QueueWrite::Queued);
        Ok(())
    }

    #[tokio::test]
    async fn write_never_waits_without_limit() {
        let (mut writer, readers) = strict_queues(2, None, value);
        for i in 0..100 {
            write_now(&mut writer, vec![Some(i), Some(i)]);
        }
        writer.finish();
        for reader in readers {
            assert_eq!(read_all(reader).await, (0..100).collect::<Vec<_>>());
        }
    }

    #[tokio::test]
    async fn waiting_writer_is_notified_when_there_is_room() -> TestResult {
        let (mut writer, mut readers) = strict_queues(3, Some(8), value);
        write_now(&mut writer, vec![Some(3), None, None]);
        write_now(&mut writer, vec![None, Some(1), Some(1)]);
        write_now(&mut writer, vec![Some(3), None, None]);
        let wakes = Arc::new(Wakes::default());
        let waker = futures::task::waker(wakes.clone());
        let mut cx = Context::from_waker(&waker);
        {
            let mut write = pin!(writer.write(vec![Some(4), None, None]));
            assert!(write.as_mut().poll(&mut cx).is_pending());
            assert_eq!(wakes.count(), 0);
            assert_eq!(readers[1].next().await, Some(1));
            assert_eq!(wakes.count(), 1);
            let outcome = write.as_mut().poll(&mut cx);
            assert_eq!(outcome, Poll::Ready(QueueWrite::Queued));
        }

        // The writer is not notified while the queues are still at the limit.
        let mut write = pin!(writer.write(vec![Some(4), None, None]));
        assert!(write.as_mut().poll(&mut cx).is_pending());
        assert_eq!(readers[2].next().await, Some(1));
        assert_eq!(wakes.count(), 1);
        assert_eq!(readers[0].next().await, Some(3));
        assert_eq!(wakes.count(), 2);
        assert_eq!(soon(write).await?, QueueWrite::Queued);
        Ok(())
    }

    #[tokio::test]
    async fn dropped_reader_closes_its_queue() -> TestResult {
        fn weight(item: &Arc<usize>) -> usize {
            **item
        }

        let (mut writer, mut readers) = strict_queues(2, Some(3), weight);
        let item = Arc::new(2);
        let data = Arc::downgrade(&item);
        let items = vec![Some(item), Some(Arc::new(1))];
        assert_eq!(writer.write(items).await, QueueWrite::Queued);
        {
            // The bytes of the reader that is gone do not count anymore.
            let mut write = pin!(writer.write(vec![Some(Arc::new(1)), Some(Arc::new(1))]));
            assert!(poll!(&mut write).is_pending());
            drop(readers.remove(0));
            assert_eq!(data.strong_count(), 0);
            assert_eq!(soon(write).await?, QueueWrite::Queued);
        }
        // The item for the closed queue is dropped right away.
        let item = Arc::new(1);
        let data = Arc::downgrade(&item);
        let items = vec![Some(item), Some(Arc::new(1))];
        assert_eq!(writer.write(items).await, QueueWrite::Queued);
        assert_eq!(data.strong_count(), 0);

        // A write that has nothing for the readers that are left never waits.
        let items = vec![Some(Arc::new(7)), None];
        assert_eq!(writer.write(items).now_or_never(), Some(QueueWrite::Queued));

        // The writer stops when the last reader is gone, and adds nothing.
        {
            let mut write = pin!(writer.write(vec![None, Some(Arc::new(1))]));
            assert!(poll!(&mut write).is_pending());
            readers.clear();
            assert_eq!(soon(write).await?, QueueWrite::Closed);
        }
        let item = Arc::new(1);
        let data = Arc::downgrade(&item);
        assert_eq!(
            writer.write(vec![None, Some(item)]).await,
            QueueWrite::Closed
        );
        assert_eq!(data.strong_count(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn finished_writer_leaves_the_items_to_the_readers() -> TestResult {
        let (mut writer, mut readers) = strict_queues(3, Some(8), one);
        write_now(&mut writer, vec![Some(0), Some(0), None]);
        write_now(&mut writer, vec![Some(1), None, None]);
        let waiting = readers.split_off(2);
        let waiting = tokio::spawn(read_all(waiting.into_iter().next().ok_or("no reader")?));
        tokio::task::yield_now().await;
        assert!(!waiting.is_finished());
        writer.finish();
        // The reader that waits for an item sees the end.
        assert_eq!(soon(waiting).await??, Vec::<usize>::new());
        let mut readers = readers.into_iter();
        assert_eq!(read_all(readers.next().ok_or("no reader")?).await, [0, 1]);
        let mut reader = readers.next().ok_or("no reader")?;
        assert_eq!(reader.next().await, Some(0));
        assert_eq!(reader.next().await, None);
        assert_eq!(reader.next().await, None);
        Ok(())
    }

    #[tokio::test]
    async fn dropped_writer_leaves_the_items_to_the_readers() -> TestResult {
        let (mut writer, mut readers) = strict_queues(2, Some(8), one);
        let item = Arc::new(0);
        let data = Arc::downgrade(&item);
        let items = vec![Some(item), None];
        assert_eq!(writer.write(items).await, QueueWrite::Queued);
        let items = vec![Some(Arc::new(1)), None];
        assert_eq!(writer.write(items).await, QueueWrite::Queued);
        let waiting = readers.split_off(1);
        let waiting = tokio::spawn(read_all(waiting.into_iter().next().ok_or("no reader")?));
        tokio::task::yield_now().await;
        assert!(!waiting.is_finished());
        drop(writer);
        // The reader that waits for an item sees the end.
        assert!(soon(waiting).await??.is_empty());
        // The other reader gets its items first.
        assert_eq!(data.strong_count(), 1);
        assert_eq!(readers[0].next().await.as_deref(), Some(&0));
        assert_eq!(data.strong_count(), 0);
        assert_eq!(readers[0].next().await.as_deref(), Some(&1));
        assert!(readers[0].next().await.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn dropped_write_adds_nothing() {
        let (mut writer, mut readers) = strict_queues(2, Some(2), one);
        write_now(&mut writer, vec![Some(0), Some(0)]);
        {
            let mut write = pin!(writer.write(vec![Some(1), Some(1)]));
            assert!(poll!(&mut write).is_pending());
        }
        assert_eq!(readers[0].next().await, Some(0));
        // The writer can be used for other writes afterwards.
        write_now(&mut writer, vec![Some(2), None]);
        writer.finish();
        let items = futures::future::join_all(readers.into_iter().map(read_all)).await;
        assert_eq!(items, [vec![2], vec![0]]);
    }

    #[tokio::test]
    async fn reader_is_notified_of_items_and_the_end() {
        let (mut writer, mut readers) = strict_queues(2, None, one);
        let wakes = Arc::new(Wakes::default());
        let waker = futures::task::waker(wakes.clone());
        let mut cx = Context::from_waker(&waker);
        // The reader can stop waiting for an item, and wait again later.
        assert!(readers[0].poll_next_unpin(&mut cx).is_pending());
        assert!(readers[0].poll_next_unpin(&mut cx).is_pending());
        write_now(&mut writer, vec![None, Some(0)]);
        assert_eq!(wakes.count(), 0);
        write_now(&mut writer, vec![Some(1), Some(1)]);
        assert_eq!(wakes.count(), 1);
        write_now(&mut writer, vec![Some(2), None]);
        assert_eq!(wakes.count(), 1);
        assert_eq!(readers[0].next().await, Some(1));
        assert_eq!(readers[0].next().await, Some(2));
        assert!(readers[0].poll_next_unpin(&mut cx).is_pending());
        writer.finish();
        assert_eq!(wakes.count(), 2);
        assert_eq!(readers[0].next().await, None);
        assert_eq!(readers[1].next().await, Some(0));
    }

    #[tokio::test]
    async fn items_keep_the_order_of_the_writes_in_each_queue() {
        let (mut writer, readers) = strict_queues(3, None, one);
        let mut expected = vec![vec![]; 3];
        for i in 0..300 {
            let items = (0..3)
                .map(|queue| (i % (queue + 2) != 0).then_some(i))
                .collect::<Vec<_>>();
            for (expected, item) in expected.iter_mut().zip(&items) {
                expected.extend(*item);
            }
            write_now(&mut writer, items);
        }
        writer.finish();
        for (reader, expected) in readers.into_iter().zip(expected) {
            assert_eq!(read_all(reader).await, expected);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn stalled_writer_stops_waiting_when_a_reader_waits_for_an_item() {
        let (mut writer, mut readers) = strict_queues(2, Some(1), one);
        write_now(&mut writer, vec![Some(0), None]);
        // The second reader waits for an item while the first reader takes nothing.
        assert!(poll!(readers[1].next()).is_pending());
        let start = Instant::now();
        let outcome = writer.write(vec![Some(1), None]).await;
        assert_eq!(outcome, QueueWrite::QueuedAfterStall);
        assert!(start.elapsed() >= STALL_TIMEOUT);
        assert!(start.elapsed() < STALL_TIMEOUT + Duration::from_secs(1));
        // The writer never waits again.
        for i in 2..100 {
            write_now(&mut writer, vec![Some(i), None]);
        }
        writer.finish();
        assert_eq!(readers[0].by_ref().collect::<Vec<_>>().await.len(), 100);
        assert_eq!(readers[1].next().await, None);
    }

    #[tokio::test(start_paused = true)]
    async fn stalled_writer_keeps_waiting_when_no_reader_waits_for_an_item() {
        let (mut writer, readers) = strict_queues(2, Some(1), one);
        write_now(&mut writer, vec![Some(0), None]);
        let write = writer.write(vec![Some(1), None]);
        let outcome = tokio::time::timeout(10 * STALL_TIMEOUT, write).await;
        assert!(outcome.is_err());
        drop(readers);
    }

    #[tokio::test(start_paused = true)]
    async fn writer_continues_when_another_task_releases_the_memory_of_the_process() -> TestResult {
        let memory = Arc::new(StreamMemory::default());
        // The queue of another writer takes all the memory.
        let (mut other, mut others) = task_queues(&[1], None, memory.clone(), value);
        write_now(&mut other, vec![Some(100)]);
        let limit = limit(1, 100);
        let (mut writer, mut readers) = task_queues(&[1, 1], limit, memory.clone(), one);
        write_now(&mut writer, vec![Some(0), None]);
        // The second reader waits for an item while the first reader takes nothing.
        assert!(poll!(readers[1].next()).is_pending());
        let start = Instant::now();
        let write = tokio::spawn(async move {
            let outcome = writer.write(vec![Some(1), None]).await;
            (writer, outcome)
        });
        tokio::task::yield_now().await;
        assert!(!write.is_finished());
        // The writer is notified when the reader of the other task takes its item,
        // and continues although its own readers have not taken anything.
        assert_eq!(others[0].next().await, Some(100));
        assert_eq!(memory.used(), 1);
        let (mut writer, outcome) = write.await?;
        assert_eq!(outcome, QueueWrite::Queued);
        assert!(start.elapsed() < STALL_TIMEOUT);

        // The writer waits again when the process is short of memory.
        let (mut other, _others) = task_queues(&[1], None, memory.clone(), value);
        write_now(&mut other, vec![Some(100)]);
        assert!(writer.write(vec![Some(2), None]).now_or_never().is_none());
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn writer_keeps_waiting_while_the_readers_take_items() {
        // The writer continues once ten readers have taken their items, which they
        // do one after another. Another reader waits for an item all the time.
        let (mut writer, mut readers) = strict_queues(11, Some(0), one);
        let mut items = vec![Some(0); 10];
        items.push(None);
        write_now(&mut writer, items);
        let mut waiting = readers.split_off(10);
        assert!(poll!(waiting[0].next()).is_pending());
        let start = Instant::now();
        let slow = tokio::spawn(async move {
            tokio::time::sleep(STALL_TIMEOUT / 4).await;
            for reader in readers.iter_mut() {
                assert_eq!(reader.next().await, Some(0));
                tokio::time::sleep(STALL_TIMEOUT * 2 / 3).await;
            }
            readers
        });
        let mut items = vec![None; 10];
        items.push(Some(1));
        assert_eq!(writer.write(items).await, QueueWrite::Queued);
        assert!(start.elapsed() >= 6 * STALL_TIMEOUT);
        assert_eq!(waiting[0].next().await, Some(1));
        assert!(slow.await.is_ok());
    }

    #[tokio::test]
    async fn write_never_waits_while_the_process_has_memory() {
        let memory = Arc::new(StreamMemory::default());
        let limit = limit(1, usize::MAX);
        let (mut writer, readers) = task_queues(&[1, 1], limit, memory.clone(), value);
        // The queues have far more bytes than the writer can always add.
        for _ in 0..1000 {
            write_now(&mut writer, vec![Some(3), Some(4)]);
        }
        assert_eq!(memory.used(), 7000);
        drop(readers);
        assert_eq!(memory.used(), 0);
    }

    #[tokio::test]
    async fn write_waits_only_for_the_bytes_above_the_ceiling() -> TestResult {
        let (mut writer, mut readers) = task_queues(&[1], limit(2, 100), Arc::default(), value);
        for _ in 0..10 {
            write_now(&mut writer, vec![Some(10)]);
        }
        for _ in 0..3 {
            let mut write = pin!(writer.write(vec![Some(10)]));
            assert!(poll!(&mut write).is_pending());
            // The queue is below the ceiling again, though far above the two
            // bytes that the writer can always add.
            assert_eq!(readers[0].next().await, Some(10));
            assert_eq!(soon(write).await?, QueueWrite::Queued);
        }
        Ok(())
    }

    #[tokio::test]
    async fn write_waits_at_the_ceiling_only_above_its_limit() -> TestResult {
        let memory = Arc::new(StreamMemory::default());
        // The queue of another writer takes all the memory.
        let (mut other, mut others) = task_queues(&[1], None, memory.clone(), value);
        write_now(&mut other, vec![Some(100)]);
        assert_eq!(memory.used(), 100);

        let limit = limit(3, 100);
        let (mut writer, mut readers) = task_queues(&[1], limit, memory.clone(), one);
        for i in 0..3 {
            write_now(&mut writer, vec![Some(i)]);
        }
        let wakes = Arc::new(Wakes::default());
        let waker = futures::task::waker(wakes.clone());
        let mut cx = Context::from_waker(&waker);
        {
            let mut write = pin!(writer.write(vec![Some(3)]));
            assert!(write.as_mut().poll(&mut cx).is_pending());
            // The reader of the writer lets it continue.
            assert_eq!(readers[0].next().await, Some(0));
            assert_eq!(wakes.count(), 1);
            let outcome = write.as_mut().poll(&mut cx);
            assert_eq!(outcome, Poll::Ready(QueueWrite::Queued));
        }

        // The writer adds far more than three items while the process has memory.
        assert_eq!(others[0].next().await, Some(100));
        for i in 4..10 {
            write_now(&mut writer, vec![Some(i)]);
        }
        write_now(&mut other, vec![Some(100)]);
        let mut write = pin!(writer.write(vec![Some(10)]));
        assert!(write.as_mut().poll(&mut cx).is_pending());
        assert_eq!(readers[0].next().await, Some(1));
        assert_eq!(wakes.count(), 1);
        // The writer is notified when the other queue is taken, whatever its own
        // queue holds then.
        assert_eq!(others[0].next().await, Some(100));
        assert_eq!(memory.used(), 8);
        assert_eq!(wakes.count(), 2);
        assert_eq!(soon(write).await?, QueueWrite::Queued);
        Ok(())
    }

    #[tokio::test]
    async fn item_counts_for_the_memory_until_it_is_gone() {
        // The items count whether or not the writer ever waits.
        let memory = Arc::new(StreamMemory::default());
        let (mut writer, mut readers) = task_queues(&[1, 1, 1], None, memory.clone(), value);
        write_now(&mut writer, vec![Some(2), Some(30), Some(500)]);
        write_now(&mut writer, vec![Some(7), None, Some(9000)]);
        assert_eq!(memory.used(), 9539);
        // Each item counts on its own, whichever write it came with.
        assert_eq!(readers[0].next().await, Some(2));
        assert_eq!(memory.used(), 9537);
        assert_eq!(readers[2].next().await, Some(500));
        assert_eq!(memory.used(), 9037);
        write_now(&mut writer, vec![Some(0), Some(1), None]);
        assert_eq!(memory.used(), 9038);
        drop(writer);
        assert_eq!(memory.used(), 9038);
        drop(readers.remove(2));
        assert_eq!(memory.used(), 38);
        drop(readers);
        assert_eq!(memory.used(), 0);
    }

    #[tokio::test]
    async fn item_counts_once_for_all_queues_of_its_channel() {
        let memory = Arc::new(StreamMemory::default());
        let (mut writer, mut readers) = task_queues(&[3, 1], None, memory.clone(), value);
        write_now(&mut writer, vec![Some(100), Some(1)]);
        write_now(&mut writer, vec![Some(20), None]);
        assert_eq!(memory.used(), 121);
        // The queues of a channel have the same data, which stays in memory
        // until the last one of them is taken or its reader is gone.
        assert_eq!(readers[0].next().await, Some(100));
        assert_eq!(readers[1].next().await, Some(100));
        assert_eq!(readers[0].next().await, Some(20));
        assert_eq!(memory.used(), 121);
        assert_eq!(readers[2].next().await, Some(100));
        assert_eq!(memory.used(), 21);
        drop(readers.remove(1));
        assert_eq!(memory.used(), 21);
        drop(readers.remove(1));
        assert_eq!(memory.used(), 1);
        // An item only counts for the queues that it is added to.
        write_now(&mut writer, vec![Some(3), None]);
        assert_eq!(readers[0].next().await, Some(3));
        assert_eq!(memory.used(), 1);
        drop(readers);
        assert_eq!(memory.used(), 0);
    }

    #[tokio::test]
    async fn dropped_write_counts_for_nothing() {
        let memory = Arc::new(StreamMemory::default());
        let (mut writer, mut readers) = task_queues(&[1], limit(1, 1), memory.clone(), value);
        write_now(&mut writer, vec![Some(5)]);
        {
            let mut write = pin!(writer.write(vec![Some(5)]));
            assert!(poll!(&mut write).is_pending());
        }
        assert_eq!(memory.used(), 5);
        assert_eq!(readers[0].next().await, Some(5));
        assert_eq!(memory.used(), 0);
    }

    /// Several writers of one process add items to their queues while items are
    /// taken, and readers and writers leave, in random order. Nothing counts for
    /// the memory of the process once everyone is gone.
    #[tokio::test]
    async fn memory_is_released_when_the_queues_are_gone() {
        for seed in 0..200 {
            let mut rng = StdRng::seed_from_u64(seed);
            let memory = Arc::new(StreamMemory::default());
            let mut writers = vec![];
            let mut readers = vec![];
            for _ in 0..rng.random_range(1..=4usize) {
                let channels = (0..rng.random_range(1..=3usize))
                    .map(|_| rng.random_range(1..=2usize))
                    .collect::<Vec<_>>();
                let limit = (rng.random_range(0..4) != 0).then(|| QueueLimit {
                    bytes: rng.random_range(0..=600),
                    ceiling: [1, 1000, usize::MAX][rng.random_range(0..3)],
                });
                let (writer, queues) = task_queues(&channels, limit, memory.clone(), value);
                writers.push((writer, channels.len()));
                readers.extend(queues);
            }
            for _ in 0..rng.random_range(0..400) {
                match rng.random_range(0..20) {
                    0 if !writers.is_empty() => {
                        drop(writers.swap_remove(rng.random_range(0..writers.len())));
                    }
                    1 if !readers.is_empty() => {
                        drop(readers.swap_remove(rng.random_range(0..readers.len())));
                    }
                    2..=11 if !writers.is_empty() => {
                        let index = rng.random_range(0..writers.len());
                        let (writer, channels) = &mut writers[index];
                        let items = (0..*channels)
                            .map(|_| {
                                (rng.random_range(0..3) != 0).then(|| rng.random_range(0..200))
                            })
                            .collect();
                        // A write that has to wait is dropped.
                        let _ = writer.write(items).now_or_never();
                    }
                    _ if !readers.is_empty() => {
                        let index = rng.random_range(0..readers.len());
                        let _ = readers[index].next().now_or_never();
                    }
                    _ => {}
                }
            }
            drop(writers);
            drop(readers);
            assert_eq!(memory.used(), 0, "seed {seed}");
        }
    }

    /// Several writers of one process start to wait at the ceiling while the reader
    /// of another task takes the items that fill the memory, on other threads. The
    /// readers of the writers take nothing, so a writer continues only if it either
    /// sees the memory that the other task has released, or is notified of it.
    /// A writer that misses both shows as a timeout.
    #[test]
    fn writers_at_the_ceiling_continue_when_another_task_releases_memory() -> TestResult {
        const CEILING: usize = 100;
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(6)
            .enable_all()
            .build()?;
        for seed in 0..1000 {
            let mut rng = StdRng::seed_from_u64(seed);
            let memory = Arc::new(StreamMemory::default());
            // The memory falls below the ceiling when the last of these items is taken,
            // and stays there whatever the writers add.
            let (mut other, mut others) = task_queues(&[1], None, memory.clone(), value);
            let items = rng.random_range(1..=4usize);
            for _ in 0..items {
                write_now(&mut other, vec![Some(CEILING)]);
            }
            let mut writers = vec![];
            let mut unread = vec![];
            for _ in 0..rng.random_range(4..=8usize) {
                let limit = limit(1, CEILING);
                let (mut writer, readers) = task_queues(&[1], limit, memory.clone(), one);
                write_now(&mut writer, vec![Some(0)]);
                writers.push((writer, rng.random_range(0..3usize)));
                unread.push(readers);
            }
            let pause = rng.random_range(0..3usize);
            let outcome = runtime.block_on(async move {
                let mut tasks = tokio::task::JoinSet::new();
                for (mut writer, pause) in writers {
                    tasks.spawn(async move {
                        for _ in 0..pause {
                            tokio::task::yield_now().await;
                        }
                        writer.write(vec![Some(1)]).await == QueueWrite::Queued
                    });
                }
                tasks.spawn(async move {
                    for _ in 0..items {
                        for _ in 0..pause {
                            tokio::task::yield_now().await;
                        }
                        if others[0].next().await.is_none() {
                            return false;
                        }
                    }
                    true
                });
                tokio::time::timeout(Duration::from_secs(5), tasks.join_all()).await
            });
            match outcome {
                Ok(outcomes) if outcomes.iter().all(|x| *x) => {}
                Ok(_) => return Err(format!("seed {seed}: a write was not queued").into()),
                Err(_) => return Err(format!("seed {seed}: a writer was not notified").into()),
            }
            drop(unread);
            drop(other);
            assert_eq!(memory.used(), 0, "seed {seed}");
        }
        Ok(())
    }

    /// One writer adds items to its queues, while the readers run on other threads
    /// and take their items, pause, stop waiting for an item, leave early, or leave
    /// without having taken anything. Every reader takes the items as they come,
    /// so nobody waits forever unless a notification is lost, which shows as a
    /// timeout. Each reader must get its items in order, and nothing counts for
    /// the memory of the process in the end.
    #[test]
    fn stress() -> TestResult {
        // The second number of an item is its bytes.
        fn weight(item: &(usize, usize)) -> usize {
            item.1
        }

        let rounds = std::env::var("SAIL_TEST_QUEUE_STRESS_ROUNDS")
            .ok()
            .and_then(|x| x.parse().ok())
            .unwrap_or(300);
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(6)
            .enable_all()
            .build()?;
        for seed in 0..rounds {
            let mut rng = StdRng::seed_from_u64(seed);
            let channels = (0..rng.random_range(1..=4usize))
                .map(|_| rng.random_range(1..=2usize))
                .collect::<Vec<_>>();
            let limit = rng.random_range(0..=30usize);
            let writes = rng.random_range(1..=3000usize);
            let ceiling = [1, 64, usize::MAX][rng.random_range(0..3)];
            let memory = Arc::new(StreamMemory::default());
            let (mut writer, readers) = task_queues(
                &channels,
                Some(QueueLimit {
                    bytes: limit,
                    ceiling,
                }),
                memory.clone(),
                weight,
            );
            let outcome = runtime.block_on(async move {
                let mut tasks = tokio::task::JoinSet::new();
                for (index, mut reader) in readers.into_iter().enumerate() {
                    let mut rng = StdRng::seed_from_u64(seed * 1000 + index as u64);
                    let unread = rng.random_range(0..6) == 0;
                    let leave_after =
                        (rng.random_range(0..4) == 0).then(|| rng.random_range(0..writes));
                    tasks.spawn(async move {
                        if unread {
                            // The reader leaves without having taken anything.
                            let delay = rng.random_range(0..20);
                            tokio::time::sleep(Duration::from_millis(delay)).await;
                            return Ok(());
                        }
                        let mut next = 0;
                        loop {
                            let item = match rng.random_range(0..10) {
                                0 => {
                                    tokio::task::yield_now().await;
                                    continue;
                                }
                                1 => {
                                    // The reader does not wait if there is no item.
                                    let Poll::Ready(item) = poll!(reader.next()) else {
                                        continue;
                                    };
                                    item
                                }
                                _ => reader.next().await,
                            };
                            let Some((value, _)) = item else {
                                return Ok(());
                            };
                            if value != next {
                                return Err(format!("queue {index}: got {value} for {next}"));
                            }
                            next += 1;
                            if leave_after.is_some_and(|k| next > k) {
                                return Ok(());
                            }
                        }
                    });
                }
                let writer = tokio::spawn(async move {
                    let mut next = vec![0; channels.len()];
                    for _ in 0..writes {
                        let items = next
                            .iter_mut()
                            .map(|next| {
                                (rng.random_range(0..4) != 0).then(|| {
                                    *next += 1;
                                    (*next - 1, rng.random_range(0..4usize))
                                })
                            })
                            .collect();
                        match writer.write(items).await {
                            QueueWrite::Queued => {}
                            QueueWrite::QueuedAfterStall => return Err("stalled".to_string()),
                            QueueWrite::Closed => return Ok(()),
                        }
                    }
                    writer.finish();
                    Ok(())
                });
                tokio::time::timeout(Duration::from_secs(10), async {
                    while let Some(result) = tasks.join_next().await {
                        result.map_err(|e| e.to_string())??;
                    }
                    writer.await.map_err(|e| e.to_string())?
                })
                .await
                .map_err(|_| "timeout".to_string())?
            });
            let outcome = outcome.and_then(|()| match memory.used() {
                0 => Ok(()),
                used => Err(format!("{used} bytes are left")),
            });
            if let Err(error) = outcome {
                return Err(
                    format!("seed {seed} (limit {limit}, ceiling {ceiling}): {error}").into(),
                );
            }
        }
        Ok(())
    }
}
