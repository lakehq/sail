//! A deterministic, seeded model test of the gate that the channels of one task share.
//!
//! Random task graphs are run on a single thread with a random scheduler: a task is only
//! polled again after one of its wakers has fired, so a lost wake-up or a wait that nothing
//! can end shows up as a stall instead of being hidden by a retry. Each graph mixes the ways
//! in which readers behave in a cluster:
//!
//! * reading all inputs as data arrives (shuffle readers merge their producers);
//! * reading the inputs one after another to the end (a hash join reads its build side
//!   first), including two inputs that come from the same task;
//! * reading the inputs in an order that depends on the data (an ordered merge);
//! * leaving after some rows (a limit), which drops all inputs;
//! * finishing without reading some inputs, either dropping them (the inputs are closed)
//!   or leaving them behind until every reader of the producing task is done (replicas
//!   that nobody reads are only removed by the cleanup of the stage);
//! * tasks in the middle of the graph that stop reading while they wait on their output.
//!
//! The test asserts that
//!
//! 1. the graph always runs to completion (no stall while work remains);
//! 2. each reader receives exactly what was sent to its queue, in order, up to the point
//!    where it stops reading;
//! 3. every send is admitted or made to wait exactly as the rule says: a task waits if and
//!    only if one of its open queues holds more than the bound and none of its readers
//!    waits for data on an empty queue.
//!
//! The module only needs `channels`, `send` and `recv` of the channel implementation.

use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll, Wake, Waker};

use super::channel::{DistributionReceiver, DistributionSender, channels};

/// (position in the queue, sort key)
type Item = (usize, u64);

/// The reader of a queue, as (task, input of the task), if the queue is read.
type QueueReader = Option<(usize, usize)>;

/// An input of a task, as (producer, queue of the producer, receiver), until it is opened.
type UnopenedInput = Option<(usize, usize, DistributionReceiver<Item>)>;

struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Self(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ 0xD1B5_4A32_D192_ED03)
    }

    fn next(&mut self) -> u64 {
        // splitmix64
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }

    fn range(&mut self, low: usize, high: usize) -> usize {
        low + self.below(high - low + 1)
    }

    fn chance(&mut self, percent: usize) -> bool {
        self.below(100) < percent
    }
}

struct Flag(AtomicBool);

impl Wake for Flag {
    fn wake(self: Arc<Self>) {
        self.0.store(true, Ordering::SeqCst);
    }
}

fn poll_once<F: Future + Unpin>(future: &mut F, waker: &Waker) -> Poll<F::Output> {
    Pin::new(future).poll(&mut Context::from_waker(waker))
}

/// What the rule says about one queue, tracked independently of the implementation.
#[derive(Clone, Default)]
struct QueueModel {
    len: usize,
    /// The receiver exists.
    open: bool,
    /// The sender exists.
    writable: bool,
    /// The receiver found the queue empty and nothing has been sent to it since.
    waiting: bool,
    sent: usize,
    max_len: usize,
}

/// The queues of one producing task.
#[derive(Default)]
struct GateModel {
    bound: usize,
    queues: Vec<QueueModel>,
}

impl GateModel {
    fn closed(&self) -> bool {
        let full = self.queues.iter().any(|q| q.open && q.len > self.bound);
        let waiting = self
            .queues
            .iter()
            .any(|q| q.open && q.writable && q.waiting);
        full && !waiting
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Policy {
    /// Read whatever arrives on any input.
    Any,
    /// Read the inputs one after another to the end.
    InOrder,
    /// Merge the inputs by the sort key.
    Merge,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Leave {
    Never,
    /// Leave after this number of rows.
    AfterRows(usize),
    /// Leave after this number of inputs have been read to the end (only for `InOrder`).
    AfterInputs(usize),
}

struct Input {
    producer: usize,
    queue: usize,
    receiver: Option<DistributionReceiver<Item>>,
    ended: bool,
    next: usize,
    head: Option<Item>,
}

struct Task {
    flag: Arc<Flag>,
    waker: Waker,
    /// Rows that a task without inputs generates.
    source: usize,
    inputs: Vec<Input>,
    policy: Policy,
    leave: Leave,
    /// Whether the inputs that are left unread are dropped or left behind.
    drops_unread: bool,
    rows: usize,
    current: usize,
    /// The senders of each output channel (one for each replica).
    senders: Vec<Option<DistributionSender<Item>>>,
    channels: Vec<Vec<usize>>,
    /// Queues that still have to receive the current row.
    pending: VecDeque<(usize, u64)>,
    done: bool,
    emitted: u64,
    /// The queue that the task waits to send to.
    blocked: Option<usize>,
}

struct Graph {
    tasks: Vec<Task>,
    gates: Vec<GateModel>,
    /// Receivers that nobody reads, kept until all readers of the producer are done.
    leftovers: Vec<Vec<(usize, DistributionReceiver<Item>)>>,
    rng: Rng,
    seed: u64,
    steps: usize,
    /// Whether a task that may continue must be woken at once (see `check_wake_ups`).
    strict_wake_ups: bool,
}

fn build(seed: u64) -> Graph {
    let mut rng = Rng::new(seed);
    let bound = [0, 1, 2, 5, 16, usize::MAX][rng.below(6)];
    let layers = rng.range(2, 4);
    let mut layer_of = vec![];
    for layer in 0..layers {
        for _ in 0..rng.range(1, 4) {
            layer_of.push(layer);
        }
    }
    let n = layer_of.len();
    // inputs of each task as (producer, channel)
    let mut wanted: Vec<Vec<(usize, usize)>> = vec![vec![]; n];
    let mut channel_count = vec![0usize; n];
    for task in 0..n {
        if layer_of[task] == 0 {
            continue;
        }
        let producers = (0..n)
            .filter(|p| layer_of[*p] < layer_of[task])
            .collect::<Vec<_>>();
        for _ in 0..rng.range(1, 3) {
            let producer = producers[rng.below(producers.len())];
            let channel = rng.below(4);
            channel_count[producer] = channel_count[producer].max(channel + 1);
            wanted[task].push((producer, channel));
        }
    }
    // queues of each producer: one replica for each reader of a channel, and sometimes
    // replicas that nobody reads
    let mut replicas: Vec<Vec<Vec<QueueReader>>> =
        (0..n).map(|p| vec![vec![]; channel_count[p]]).collect();
    for (task, inputs) in wanted.iter().enumerate() {
        for (index, (producer, channel)) in inputs.iter().enumerate() {
            replicas[*producer][*channel].push(Some((task, index)));
        }
    }
    for producer in replicas.iter_mut() {
        for channel in producer.iter_mut() {
            while rng.chance(15) {
                channel.push(None);
            }
        }
    }
    let mut tasks = vec![];
    let mut gates = vec![];
    let mut leftovers = vec![];
    let mut receivers: Vec<Vec<UnopenedInput>> = wanted
        .iter()
        .map(|x| x.iter().map(|_| None).collect())
        .collect();
    for producer in 0..n {
        let count = replicas[producer].iter().map(Vec::len).sum::<usize>();
        let (senders, queues) = channels::<Item>(count, bound);
        let mut queues = queues.into_iter().enumerate();
        let mut channel_queues = vec![];
        let mut left = vec![];
        for channel in &replicas[producer] {
            let mut ids = vec![];
            for reader in channel {
                if let Some((queue, receiver)) = queues.next() {
                    ids.push(queue);
                    match reader {
                        Some((task, index)) => {
                            receivers[*task][*index] = Some((producer, queue, receiver));
                        }
                        None => left.push((queue, receiver)),
                    }
                }
            }
            channel_queues.push(ids);
        }
        gates.push(GateModel {
            bound,
            queues: vec![
                QueueModel {
                    open: true,
                    writable: true,
                    ..Default::default()
                };
                count
            ],
        });
        leftovers.push(left);
        let flag = Arc::new(Flag(AtomicBool::new(true)));
        let policy = [Policy::Any, Policy::InOrder, Policy::InOrder, Policy::Merge][rng.below(4)];
        let leave = match rng.below(10) {
            0..=1 => Leave::AfterRows(rng.below(40)),
            2..=4 if policy == Policy::InOrder => Leave::AfterInputs(rng.below(3)),
            _ => Leave::Never,
        };
        tasks.push(Task {
            waker: Waker::from(flag.clone()),
            flag,
            source: if layer_of[producer] == 0 {
                rng.range(0, 300)
            } else {
                0
            },
            inputs: vec![],
            policy,
            leave,
            drops_unread: rng.chance(60),
            rows: 0,
            current: 0,
            senders: senders.into_iter().map(Some).collect(),
            channels: channel_queues,
            pending: VecDeque::new(),
            done: false,
            emitted: 0,
            blocked: None,
        });
    }
    for (task, inputs) in receivers.into_iter().enumerate() {
        for input in inputs.into_iter().flatten() {
            let (producer, queue, receiver) = input;
            tasks[task].inputs.push(Input {
                producer,
                queue,
                receiver: Some(receiver),
                ended: false,
                next: 0,
                head: None,
            });
        }
    }
    Graph {
        tasks,
        gates,
        leftovers,
        rng,
        seed,
        steps: 0,
        strict_wake_ups: false,
    }
}

enum Read {
    Row(u64),
    Wait,
    End,
}

impl Graph {
    /// Receives from one input, checking the result against the model.
    fn receive(&mut self, task: usize, index: usize) -> Poll<Option<Item>> {
        let waker = self.tasks[task].waker.clone();
        let input = &mut self.tasks[task].inputs[index];
        let Some(receiver) = input.receiver.as_mut() else {
            return Poll::Ready(None);
        };
        let queue = &mut self.gates[input.producer].queues[input.queue];
        let mut future = receiver.recv();
        match poll_once(&mut future, &waker) {
            Poll::Ready(Some((position, key))) => {
                assert!(
                    queue.len > 0,
                    "seed {}: received from an empty queue",
                    self.seed
                );
                assert_eq!(position, input.next, "seed {}: out of order", self.seed);
                queue.len -= 1;
                input.next += 1;
                Poll::Ready(Some((position, key)))
            }
            Poll::Ready(None) => {
                assert_eq!(queue.len, 0, "seed {}: ended with data left", self.seed);
                assert!(
                    !queue.writable,
                    "seed {}: ended before the writer",
                    self.seed
                );
                assert_eq!(
                    input.next, queue.sent,
                    "seed {}: rows are missing",
                    self.seed
                );
                input.ended = true;
                Poll::Ready(None)
            }
            Poll::Pending => {
                assert_eq!(
                    queue.len, 0,
                    "seed {}: waits although data is queued",
                    self.seed
                );
                assert!(
                    queue.writable,
                    "seed {}: waits for a writer that left",
                    self.seed
                );
                queue.waiting = true;
                Poll::Pending
            }
        }
    }

    fn drop_input(&mut self, task: usize, index: usize) {
        let input = &mut self.tasks[task].inputs[index];
        if input.receiver.take().is_some() {
            let queue = &mut self.gates[input.producer].queues[input.queue];
            queue.open = false;
        }
    }

    fn read(&mut self, task: usize) -> Read {
        let count = self.tasks[task].inputs.len();
        if count == 0 {
            if self.tasks[task].source == 0 {
                return Read::End;
            }
            self.tasks[task].source -= 1;
            let key = self.tasks[task].emitted;
            return Read::Row(key);
        }
        if let Leave::AfterRows(rows) = self.tasks[task].leave
            && self.tasks[task].rows >= rows
        {
            return Read::End;
        }
        match self.tasks[task].policy {
            Policy::Any => {
                let start = self.rng.below(count);
                let mut waits = false;
                for offset in 0..count {
                    let index = (start + offset) % count;
                    if self.tasks[task].inputs[index].ended {
                        continue;
                    }
                    match self.receive(task, index) {
                        Poll::Ready(Some((_, key))) => return Read::Row(key),
                        Poll::Ready(None) => {}
                        Poll::Pending => waits = true,
                    }
                }
                if waits { Read::Wait } else { Read::End }
            }
            Policy::InOrder => loop {
                let current = self.tasks[task].current;
                if current >= count {
                    return Read::End;
                }
                if let Leave::AfterInputs(inputs) = self.tasks[task].leave
                    && current >= inputs
                {
                    return Read::End;
                }
                match self.receive(task, current) {
                    Poll::Ready(Some((_, key))) => return Read::Row(key),
                    Poll::Ready(None) => self.tasks[task].current += 1,
                    Poll::Pending => return Read::Wait,
                }
            },
            Policy::Merge => {
                let mut waits = false;
                for index in 0..count {
                    let input = &self.tasks[task].inputs[index];
                    if input.ended || input.head.is_some() {
                        continue;
                    }
                    match self.receive(task, index) {
                        Poll::Ready(Some(item)) => self.tasks[task].inputs[index].head = Some(item),
                        Poll::Ready(None) => {}
                        Poll::Pending => waits = true,
                    }
                }
                if waits {
                    return Read::Wait;
                }
                let best = self.tasks[task]
                    .inputs
                    .iter()
                    .enumerate()
                    .filter_map(|(index, input)| input.head.map(|(_, key)| (key, index)))
                    .min();
                match best {
                    Some((key, index)) => {
                        self.tasks[task].inputs[index].head = None;
                        Read::Row(key)
                    }
                    None => Read::End,
                }
            }
        }
    }

    /// Sends the pending copies of the current row. Returns false if the task has to wait.
    fn write(&mut self, task: usize) -> bool {
        self.tasks[task].blocked = None;
        while let Some((queue, key)) = self.tasks[task].pending.front().copied() {
            let waker = self.tasks[task].waker.clone();
            let model = &mut self.gates[task];
            let Some(sender) = self.tasks[task].senders[queue].as_ref() else {
                self.tasks[task].pending.pop_front();
                continue;
            };
            let closed = model.closed();
            let position = model.queues[queue].sent;
            let mut future = sender.send((position, key));
            match poll_once(&mut future, &waker) {
                Poll::Ready(Ok(())) => {
                    assert!(
                        model.queues[queue].open,
                        "seed {}: sent to a reader that left",
                        self.seed
                    );
                    assert!(
                        !closed,
                        "seed {}: sent although the task has to wait",
                        self.seed
                    );
                    let q = &mut model.queues[queue];
                    q.len += 1;
                    q.sent += 1;
                    q.waiting = false;
                    q.max_len = q.max_len.max(q.len);
                }
                Poll::Ready(Err(_)) => {
                    assert!(
                        !model.queues[queue].open,
                        "seed {}: reader is still there",
                        self.seed
                    );
                    drop(future);
                    self.tasks[task].senders[queue] = None;
                    model.queues[queue].writable = false;
                }
                Poll::Pending => {
                    assert!(
                        model.queues[queue].open,
                        "seed {}: waits for a reader that left",
                        self.seed
                    );
                    assert!(
                        closed,
                        "seed {}: waits although the task may continue",
                        self.seed
                    );
                    self.tasks[task].blocked = Some(queue);
                    return false;
                }
            }
            self.tasks[task].pending.pop_front();
        }
        true
    }

    fn finish(&mut self, task: usize) {
        // the writer is committed: its readers see the end after the data that is queued
        for (queue, sender) in self.tasks[task].senders.iter_mut().enumerate() {
            if sender.take().is_some() {
                let q = &mut self.gates[task].queues[queue];
                q.writable = false;
                q.waiting = false;
            }
        }
        for index in 0..self.tasks[task].inputs.len() {
            let unread = !self.tasks[task].inputs[index].ended;
            if unread && !self.tasks[task].drops_unread && self.tasks[task].inputs[index].next == 0
            {
                // an input that was never read is left behind
                let input = &mut self.tasks[task].inputs[index];
                if let Some(receiver) = input.receiver.take() {
                    self.leftovers[input.producer].push((input.queue, receiver));
                }
            } else {
                self.drop_input(task, index);
            }
        }
        self.tasks[task].done = true;
    }

    fn step(&mut self, task: usize) {
        loop {
            if !self.write(task) {
                return;
            }
            let has_outputs = !self.tasks[task].senders.is_empty();
            if has_outputs && self.tasks[task].senders.iter().all(Option::is_none) {
                // all readers have left
                self.finish(task);
                return;
            }
            match self.read(task) {
                Read::Wait => return,
                Read::End => {
                    self.finish(task);
                    return;
                }
                Read::Row(key) => {
                    self.tasks[task].rows += 1;
                    self.tasks[task].emitted += 1;
                    if !self.tasks[task].channels.is_empty() {
                        let channel = self.rng.below(self.tasks[task].channels.len());
                        let key = if self.tasks[task].inputs.is_empty() {
                            key
                        } else {
                            // keep the output of a task sorted for the merges downstream
                            self.tasks[task].emitted
                        };
                        for queue in self.tasks[task].channels[channel].clone() {
                            self.tasks[task].pending.push_back((queue, key));
                        }
                    }
                }
            }
        }
    }

    /// Removes the queues that nobody reads once all readers of a task are done.
    fn clean_up(&mut self) -> bool {
        let mut changed = false;
        for producer in 0..self.tasks.len() {
            if self.leftovers[producer].is_empty() {
                continue;
            }
            let readers_done = self.tasks.iter().all(|task| {
                task.done || task.inputs.iter().all(|input| input.producer != producer)
            });
            if readers_done {
                for (queue, receiver) in self.leftovers[producer].drain(..) {
                    self.gates[producer].queues[queue].open = false;
                    drop(receiver);
                }
                changed = true;
            }
        }
        changed
    }

    /// A task that waits to send must have been woken as soon as it may continue,
    /// either because the rule lets it or because the reader it sends to has left.
    fn check_wake_ups(&self) -> Result<(), String> {
        if !self.strict_wake_ups {
            return Ok(());
        }
        for (id, task) in self.tasks.iter().enumerate() {
            let Some(queue) = task.blocked else {
                continue;
            };
            if task.done || task.flag.0.load(Ordering::SeqCst) {
                continue;
            }
            if !self.gates[id].closed() || !self.gates[id].queues[queue].open {
                return Err(format!(
                    "task {id} is not woken although it may continue\n{}",
                    self.describe()
                ));
            }
        }
        Ok(())
    }

    fn run(&mut self) -> Result<(), String> {
        loop {
            let ready = (0..self.tasks.len())
                .filter(|task| {
                    !self.tasks[*task].done && self.tasks[*task].flag.0.load(Ordering::SeqCst)
                })
                .collect::<Vec<_>>();
            if ready.is_empty() {
                if self.tasks.iter().all(|task| task.done) {
                    return Ok(());
                }
                if self.clean_up() {
                    continue;
                }
                return Err(self.describe());
            }
            let task = ready[self.rng.below(ready.len())];
            self.tasks[task].flag.0.store(false, Ordering::SeqCst);
            self.step(task);
            self.clean_up();
            self.check_wake_ups()?;
            self.steps += 1;
        }
    }

    fn describe(&self) -> String {
        let mut out = format!("seed {} stalls after {} steps\n", self.seed, self.steps);
        for (id, task) in self.tasks.iter().enumerate() {
            out += &format!(
                "task {id}: done={} {:?} {:?} rows={} pending={} inputs=[{}] queues=[{}] closed={}\n",
                task.done,
                task.policy,
                task.leave,
                task.rows,
                task.pending.len(),
                task.inputs
                    .iter()
                    .map(|input| format!(
                        "{}.{}{}",
                        input.producer,
                        input.queue,
                        if input.ended {
                            " ended"
                        } else if input.receiver.is_none() {
                            " gone"
                        } else {
                            ""
                        }
                    ))
                    .collect::<Vec<_>>()
                    .join(", "),
                self.gates[id]
                    .queues
                    .iter()
                    .map(|q| format!(
                        "len {}{}{}{}",
                        q.len,
                        if q.open { "" } else { " no-reader" },
                        if q.writable { "" } else { " no-writer" },
                        if q.waiting { " waiting" } else { "" }
                    ))
                    .collect::<Vec<_>>()
                    .join(", "),
                self.gates[id].closed(),
            );
        }
        out
    }
}

/// Statistics of the graphs that were run.
#[derive(Default, Debug)]
pub struct Summary {
    pub graphs: usize,
    pub tasks: usize,
    pub steps: usize,
    pub rows: usize,
    /// Graphs in which some queue grew beyond bound + 1.
    pub beyond_bound: usize,
    pub longest_queue_over_finite_bound: usize,
}

/// Runs the graphs of the given seeds. Returns the description of the first stall.
///
/// With `strict_wake_ups`, a task that waits to send must also be woken in the very step
/// that lets it continue. Without it, the test only requires that the graph completes,
/// which a late wake-up (for example when a reader runs out of data) also achieves.
pub fn run_seeds(seeds: std::ops::Range<u64>, strict_wake_ups: bool) -> Result<Summary, String> {
    let mut summary = Summary::default();
    for seed in seeds {
        let mut graph = build(seed);
        graph.strict_wake_ups = strict_wake_ups;
        graph.run()?;
        summary.graphs += 1;
        summary.tasks += graph.tasks.len();
        summary.steps += graph.steps;
        summary.rows += graph
            .gates
            .iter()
            .flat_map(|g| &g.queues)
            .map(|q| q.sent)
            .sum::<usize>();
        let mut beyond = false;
        for gate in &graph.gates {
            if gate.bound == usize::MAX {
                continue;
            }
            for queue in &gate.queues {
                if queue.max_len > gate.bound + 1 {
                    beyond = true;
                    summary.longest_queue_over_finite_bound =
                        summary.longest_queue_over_finite_bound.max(queue.max_len);
                }
            }
        }
        summary.beyond_bound += usize::from(beyond);
    }
    Ok(summary)
}

#[cfg(test)]
#[expect(clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn random_task_graphs_never_stall() {
        match run_seeds(0..5_000, true) {
            Ok(summary) => {
                assert_eq!(summary.graphs, 5_000);
                // The graphs are not trivial, and they cover queues that grow beyond the
                // bound because another reader of the same task waits for data.
                assert!(summary.tasks > summary.graphs, "{summary:?}");
                assert!(summary.steps > summary.tasks, "{summary:?}");
                assert!(summary.rows > summary.steps, "{summary:?}");
                assert!(summary.beyond_bound > 0, "{summary:?}");
                assert!(summary.longest_queue_over_finite_bound > 17, "{summary:?}");
            }
            Err(stall) => panic!("{stall}"),
        }
    }
}
