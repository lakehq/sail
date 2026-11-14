// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// [CREDIT]: https://github.com/apache/datafusion/blob/55.2.0/datafusion/physical-plan/src/repartition/distributor_channels.rs

//! Special channel construction to distribute data from various inputs into N outputs
//! minimizing buffering but preventing deadlocks when repartitioning
//!
//! # Design
//!
//! ```text
//! +----+      +------+
//! | TX |==||  | Gate |
//! +----+  ||  |      |  +--------+  +----+
//!         ====|      |==| Buffer |==| RX |
//! +----+  ||  |      |  +--------+  +----+
//! | TX |==||  |      |
//! +----+      |      |
//!             |      |
//! +----+      |      |  +--------+  +----+
//! | TX |======|      |==| Buffer |==| RX |
//! +----+      +------+  +--------+  +----+
//! ```
//!
//! There are `N` virtual MPSC (multi-producer, single consumer) channels with unbounded capacity. However, if some
//! buffer/channel holds more elements than a bound and no receiver waits for data, then a global gate will be closed
//! preventing new data from being written (the sender futures will be [pending](Poll::Pending)) until all channels
//! are within the bound again or a receiver finds its channel empty (and not closed).

#![expect(clippy::expect_used)]

use std::collections::VecDeque;
use std::future::Future;
use std::ops::DerefMut;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Waker};

use parking_lot::Mutex;

/// Create `n` empty channels.
///
/// The senders wait when a channel holds more than `bound` elements, unless a receiver waits for data.
pub fn channels<T>(
    n: usize,
    bound: usize,
) -> (Vec<DistributionSender<T>>, Vec<DistributionReceiver<T>>) {
    let channels = (0..n)
        .map(|id| Arc::new(Channel::new_with_one_sender(id)))
        .collect::<Vec<_>>();
    let gate = Arc::new(Gate {
        receivers: AtomicUsize::new(n),
        bound,
        full_channels: AtomicUsize::new(0),
        waiting_receivers: AtomicUsize::new(0),
        send_wakers: Mutex::new(Vec::new()),
    });
    let senders = channels
        .iter()
        .map(|channel| DistributionSender {
            channel: Arc::clone(channel),
            gate: Arc::clone(&gate),
        })
        .collect();
    let receivers = channels
        .into_iter()
        .map(|channel| DistributionReceiver {
            channel,
            gate: Arc::clone(&gate),
        })
        .collect();
    (senders, receivers)
}

/// Erroring during [send](DistributionSender::send).
///
/// This occurs when the [receiver](DistributionReceiver) is gone.
#[derive(PartialEq, Eq)]
pub struct SendError<T>(pub T);

impl<T> std::fmt::Debug for SendError<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("SendError").finish()
    }
}

impl<T> std::fmt::Display for SendError<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "cannot send data, receiver is gone")
    }
}

impl<T> std::error::Error for SendError<T> {}

/// Sender side of distribution [channels].
///
/// This handle can be cloned. All clones will write into the same channel. Dropping the last sender will close the
/// channel. In this case, the [receiver](DistributionReceiver) will still be able to poll the remaining data, but will
/// receive `None` afterwards.
#[derive(Debug)]
pub struct DistributionSender<T> {
    /// To prevent lock inversion / deadlock, channel lock is always acquired prior to gate lock
    channel: SharedChannel<T>,
    gate: SharedGate,
}

impl<T> DistributionSender<T> {
    /// Send data.
    ///
    /// This fails if the [receiver](DistributionReceiver) is gone.
    pub fn send(&self, element: T) -> SendFuture<'_, T> {
        SendFuture {
            channel: &self.channel,
            gate: &self.gate,
            element: Box::new(Some(element)),
        }
    }

    /// Whether a [receiver](DistributionReceiver) is left for any of the [channels].
    pub fn has_receivers(&self) -> bool {
        self.gate.receivers.load(Ordering::SeqCst) > 0
    }

    /// Discard the data that has not been received yet.
    pub fn clear(&self) {
        let open_gate = {
            let mut state = self.channel.state.lock();

            let Some(data) = state.data.as_mut() else {
                // receiver end dead
                return;
            };

            let was_full = data.len() > self.gate.bound;
            *data = VecDeque::default();

            was_full && self.gate.decr_full_channels()
        };

        // wake outside of lock scope
        if open_gate {
            self.gate.wake_senders();
        }
    }
}

impl<T> Clone for DistributionSender<T> {
    fn clone(&self) -> Self {
        self.channel.n_senders.fetch_add(1, Ordering::SeqCst);

        Self {
            channel: Arc::clone(&self.channel),
            gate: Arc::clone(&self.gate),
        }
    }
}

impl<T> Drop for DistributionSender<T> {
    fn drop(&mut self) {
        let n_senders_pre = self.channel.n_senders.fetch_sub(1, Ordering::SeqCst);
        // is the last copy of the sender side?
        if n_senders_pre > 1 {
            return;
        }

        let receivers = {
            let mut state = self.channel.state.lock();

            // make sure that nobody can add wakers anymore
            let receivers = state.recv_wakers.take().expect("not closed yet");

            // During the shutdown of a channel whose receiver waits for data, both the sender and the receiver side
            // will be dropped. However we only want to decrement the "waiting receivers" counter once.
            //
            // We are within a critical section here, so we can safely assume that either the last sender or the
            // receiver (there's only one) will be dropped first. Whichever side is dropped first removes the wakers
            // and decrements the counter, so the other side finds no wakers.
            if !receivers.is_empty() {
                // the receiver will see the end of the channel, so we need to clear our signal
                self.gate.waiting_receivers.fetch_sub(1, Ordering::SeqCst);
            }

            receivers
        };

        // wake outside of lock scope
        for recv in receivers {
            recv.wake();
        }
    }
}

/// Future backing [send](DistributionSender::send).
#[derive(Debug)]
pub struct SendFuture<'a, T> {
    channel: &'a SharedChannel<T>,
    gate: &'a SharedGate,
    // the additional Box is required for `Self: Unpin`
    element: Box<Option<T>>,
}

impl<T> Future for SendFuture<'_, T> {
    type Output = Result<(), SendError<T>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = &mut *self;
        assert!(this.element.is_some(), "polled ready future");

        // lock scope
        let to_wake = {
            let mut guard_channel_state = this.channel.state.lock();

            let Some(data) = guard_channel_state.data.as_mut() else {
                // receiver end dead
                return Poll::Ready(Err(SendError(this.element.take().expect("just checked"))));
            };

            // is ANY channel over the bound while NO receiver needs data?
            // if so, the sender has to wait
            if this.gate.is_closed() {
                let mut send_wakers = this.gate.send_wakers.lock();

                // check after lock to see if the gate has been opened in the meantime
                if this.gate.is_closed() {
                    send_wakers.push((cx.waker().clone(), this.channel.id));
                    return Poll::Pending;
                }
            }

            let was_empty = data.is_empty();
            data.push_back(this.element.take().expect("just checked"));

            if data.len() - 1 == this.gate.bound {
                this.gate.full_channels.fetch_add(1, Ordering::SeqCst);
            }

            if was_empty {
                let to_wake = guard_channel_state.take_recv_wakers();
                if !to_wake.is_empty() {
                    // the receiver no longer waits for data
                    this.gate.waiting_receivers.fetch_sub(1, Ordering::SeqCst);
                }
                to_wake
            } else {
                Vec::with_capacity(0)
            }
        };

        // wake outside of lock scope
        for receiver in to_wake {
            receiver.wake();
        }

        Poll::Ready(Ok(()))
    }
}

/// Receiver side of distribution [channels].
#[derive(Debug)]
pub struct DistributionReceiver<T> {
    channel: SharedChannel<T>,
    gate: SharedGate,
}

impl<T> DistributionReceiver<T> {
    /// Receive data from channel.
    ///
    /// Returns `None` if the channel is empty and no [senders](DistributionSender) are left.
    pub fn recv(&mut self) -> RecvFuture<'_, T> {
        RecvFuture {
            channel: &mut self.channel,
            gate: &mut self.gate,
            rdy: false,
        }
    }
}

impl<T> Drop for DistributionReceiver<T> {
    fn drop(&mut self) {
        self.gate.receivers.fetch_sub(1, Ordering::SeqCst);

        let open_gate = {
            let mut guard_channel_state = self.channel.state.lock();
            let data = guard_channel_state.data.take().expect("not dropped yet");

            // See `DistributedSender::drop` for an explanation of the drop order and when the "waiting receivers"
            // counter is decremented.
            if let Some(recv_wakers) = guard_channel_state.recv_wakers.as_mut()
                && !std::mem::take(recv_wakers).is_empty()
            {
                // channel is gone, so we need to clear our signal
                self.gate.waiting_receivers.fetch_sub(1, Ordering::SeqCst);
            }

            // channel is gone, so its data no longer counts
            data.len() > self.gate.bound && self.gate.decr_full_channels()
        };

        // senders may be waiting for gate to open but should error now that the channel is closed
        self.gate.wake_channel_senders(self.channel.id);

        if open_gate {
            self.gate.wake_senders();
        }
    }
}

/// Future backing [recv](DistributionReceiver::recv).
pub struct RecvFuture<'a, T> {
    channel: &'a mut SharedChannel<T>,
    gate: &'a mut SharedGate,
    rdy: bool,
}

impl<T> Future for RecvFuture<'_, T> {
    type Output = Option<T>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = &mut *self;
        assert!(!this.rdy, "polled ready future");

        let mut guard_channel_state = this.channel.state.lock();
        let channel_state = guard_channel_state.deref_mut();
        let data = channel_state.data.as_mut().expect("not dropped yet");

        match data.pop_front() {
            Some(element) => {
                // change "full" signal for this channel?
                let open_gate = data.len() == this.gate.bound && this.gate.decr_full_channels();

                drop(guard_channel_state);

                // wake outside of lock scope
                if open_gate {
                    this.gate.wake_senders();
                }

                this.rdy = true;
                Poll::Ready(Some(element))
            }
            None => {
                if let Some(recv_wakers) = channel_state.recv_wakers.as_mut() {
                    // change "waiting" signal for this channel?
                    let open_gate = recv_wakers.is_empty()
                        && this.gate.waiting_receivers.fetch_add(1, Ordering::SeqCst) == 0;
                    recv_wakers.push(cx.waker().clone());

                    drop(guard_channel_state);

                    // wake outside of lock scope
                    if open_gate {
                        this.gate.wake_senders();
                    }

                    Poll::Pending
                } else {
                    this.rdy = true;
                    Poll::Ready(None)
                }
            }
        }
    }
}

/// Links senders and receivers.
#[derive(Debug)]
struct Channel<T> {
    /// Reference counter for the sender side.
    n_senders: AtomicUsize,

    /// Channel ID.
    ///
    /// This is used to address [send wakers](Gate::send_wakers).
    id: usize,

    /// Mutable state.
    state: Mutex<ChannelState<T>>,
}

impl<T> Channel<T> {
    /// Create new channel with one sender (so we don't need to [fetch-add](AtomicUsize::fetch_add) directly afterwards).
    fn new_with_one_sender(id: usize) -> Self {
        Channel {
            n_senders: AtomicUsize::new(1),
            id,
            state: Mutex::new(ChannelState {
                data: Some(VecDeque::default()),
                recv_wakers: Some(Vec::default()),
            }),
        }
    }
}

#[derive(Debug)]
struct ChannelState<T> {
    /// Buffered data.
    ///
    /// This is [`None`] when the receiver is gone.
    data: Option<VecDeque<T>>,

    /// Wakers for the receiver side.
    ///
    /// The receiver will be pending if the [buffer](Self::data) is empty and
    /// there are senders left (otherwise this is set to [`None`]).
    recv_wakers: Option<Vec<Waker>>,
}

impl<T> ChannelState<T> {
    /// Get all [`recv_wakers`](Self::recv_wakers) and replace with identically-sized buffer.
    ///
    /// The wakers should be woken AFTER the lock to [this state](Self) was dropped.
    ///
    /// # Panics
    /// Assumes that channel is NOT closed yet, i.e. that [`recv_wakers`](Self::recv_wakers) is not [`None`].
    fn take_recv_wakers(&mut self) -> Vec<Waker> {
        let to_wake = self.recv_wakers.as_mut().expect("not closed");
        let mut tmp = Vec::with_capacity(to_wake.capacity());
        std::mem::swap(to_wake, &mut tmp);
        tmp
    }
}

/// Shared channel.
///
/// One or multiple senders and a single receiver will share a channel.
type SharedChannel<T> = Arc<Channel<T>>;

/// The "a channel is over the bound and no receiver waits for data" gate.
#[derive(Debug)]
struct Gate {
    /// Number of receivers that are not dropped yet.
    receivers: AtomicUsize,

    /// Number of elements that a channel can hold without closing the gate.
    bound: usize,

    /// Number of (still open) channels that currently hold more elements than the bound.
    full_channels: AtomicUsize,

    /// Number of receivers that currently wait for data on an empty (and still open) channel.
    waiting_receivers: AtomicUsize,

    /// Wakers for the sender side, including their channel IDs.
    send_wakers: Mutex<Vec<(Waker, usize)>>,
}

impl Gate {
    /// Whether the senders have to wait.
    fn is_closed(&self) -> bool {
        self.full_channels.load(Ordering::SeqCst) > 0
            && self.waiting_receivers.load(Ordering::SeqCst) == 0
    }

    /// Wake senders for a specific channel.
    ///
    /// This is helpful to signal that the receiver side is gone and the senders shall now error.
    fn wake_channel_senders(&self, id: usize) {
        // lock scope
        let to_wake = {
            let mut send_wakers = self.send_wakers.lock();

            // `drain_filter` is unstable, so implement our own
            let (wake, keep): (Vec<_>, Vec<_>) =
                send_wakers.drain(..).partition(|(_waker, id2)| id == *id2);

            *send_wakers = keep;

            wake
        };

        // wake outside of lock scope
        for (waker, _id) in to_wake {
            waker.wake();
        }
    }

    /// Wake all senders.
    ///
    /// This MUST be called after the gate may have been opened, i.e. after the last full channel is gone or the
    /// first receiver starts to wait. A sender only starts to wait if it finds the gate closed while it holds the
    /// lock of the wakers, so it is either woken here or it sees the change.
    fn wake_senders(&self) {
        let to_wake = std::mem::take(self.send_wakers.lock().deref_mut());

        for (waker, _id) in to_wake {
            waker.wake();
        }
    }

    /// Remove a channel from the full ones and return whether none is left.
    fn decr_full_channels(&self) -> bool {
        self.full_channels.fetch_sub(1, Ordering::SeqCst) == 1
    }
}

/// Gate shared by all senders and receivers.
type SharedGate = Arc<Gate>;

#[cfg(test)]
#[expect(clippy::unwrap_used, clippy::panic)]
mod tests {
    use std::sync::atomic::AtomicBool;
    use std::time::Duration;

    use futures::FutureExt;
    use futures::task::ArcWake;
    use rand::rngs::StdRng;
    use rand::{RngExt, SeedableRng};

    use super::*;

    #[test]
    fn test_single_channel_no_gate() {
        // use an unlimited bound so that the channel never hits the gate
        let (mut txs, mut rxs) = channels(2, usize::MAX);

        let mut recv_fut = rxs[0].recv();
        let waker = poll_pending(&mut recv_fut);

        poll_ready(&mut txs[0].send("foo")).unwrap();
        assert!(waker.woken());
        assert_eq!(poll_ready(&mut recv_fut), Some("foo"),);

        poll_ready(&mut txs[0].send("bar")).unwrap();
        poll_ready(&mut txs[0].send("baz")).unwrap();
        poll_ready(&mut txs[0].send("end")).unwrap();
        assert_eq!(poll_ready(&mut rxs[0].recv()), Some("bar"),);
        assert_eq!(poll_ready(&mut rxs[0].recv()), Some("baz"),);

        // close channel
        txs.remove(0);
        assert_eq!(poll_ready(&mut rxs[0].recv()), Some("end"),);
        assert_eq!(poll_ready(&mut rxs[0].recv()), None,);
        assert_eq!(poll_ready(&mut rxs[0].recv()), None,);
    }

    #[test]
    fn test_multi_sender() {
        // use an unlimited bound so that the channel never hits the gate
        let (txs, mut rxs) = channels(2, usize::MAX);

        let tx_clone = txs[0].clone();

        poll_ready(&mut txs[0].send("foo")).unwrap();
        poll_ready(&mut tx_clone.send("bar")).unwrap();

        assert_eq!(poll_ready(&mut rxs[0].recv()), Some("foo"),);
        assert_eq!(poll_ready(&mut rxs[0].recv()), Some("bar"),);
    }

    #[test]
    fn test_gate() {
        let (txs, mut rxs) = channels(2, 1);

        // gate initially open
        poll_ready(&mut txs[0].send("0_a")).unwrap();

        // gate still open because channel 0 is still within the bound
        poll_ready(&mut txs[1].send("1_a")).unwrap();

        // gate still open because channel 0 is still within the bound prior to this call, so this call still goes
        // through
        poll_ready(&mut txs[0].send("0_b")).unwrap();

        // channel 0 over the bound and no receiver waiting => gate closed

        let mut send_fut = txs[1].send("1_b");
        let waker = poll_pending(&mut send_fut);

        // drain channel 1
        assert_eq!(poll_ready(&mut rxs[1].recv()), Some("1_a"),);
        poll_pending(&mut send_fut);
        assert_eq!(poll_ready(&mut rxs[0].recv()), Some("0_a"),);

        // channel 0 within the bound => gate open
        assert!(waker.woken());
        poll_ready(&mut send_fut).unwrap();
    }

    #[test]
    fn test_gate_waiting_receiver() {
        let (txs, mut rxs) = channels(2, 0);

        let mut rx1 = rxs.remove(1);
        let mut recv_fut = rx1.recv();
        poll_pending(&mut recv_fut);

        // gate open because receiver 1 waits for data, although channel 0 goes over the bound
        poll_ready(&mut txs[0].send("0_a")).unwrap();
        poll_ready(&mut txs[0].send("0_b")).unwrap();

        // receiver 1 no longer waits after this call => gate closed
        poll_ready(&mut txs[1].send("1_a")).unwrap();

        let mut send_fut = txs[0].send("0_c");
        let waker = poll_pending(&mut send_fut);

        // channel 1 within the bound but channel 0 is not
        assert_eq!(poll_ready(&mut recv_fut), Some("1_a"),);
        poll_pending(&mut send_fut);
        assert!(!waker.woken());

        // receiver 1 waits for data again => gate open
        poll_pending(&mut rx1.recv());
        assert!(waker.woken());
        poll_ready(&mut send_fut).unwrap();
    }

    #[test]
    fn test_clear() {
        let (txs, mut rxs) = channels(2, 0);

        let obj = Arc::new(());
        let counter = Arc::downgrade(&obj);

        // channel 0 over the bound => gate closed
        poll_ready(&mut txs[0].send(obj)).unwrap();
        let mut send_fut = txs[1].send(Arc::new(()));
        let waker = poll_pending(&mut send_fut);

        // discard data => gate open
        txs[0].clear();
        assert_eq!(counter.strong_count(), 0);
        assert!(waker.woken());
        poll_ready(&mut send_fut).unwrap();

        // channel 0 can still be used
        poll_pending(&mut rxs[0].recv());
        poll_ready(&mut txs[0].send(Arc::new(()))).unwrap();
        assert!(poll_ready(&mut rxs[0].recv()).is_some());
    }

    #[test]
    fn test_close_channel_by_dropping_tx() {
        let (mut txs, mut rxs) = channels(2, 0);

        let tx0 = txs.remove(0);
        let tx1 = txs.remove(0);
        let tx0_clone = tx0.clone();

        let mut recv_fut = rxs[0].recv();

        poll_ready(&mut tx1.send("a")).unwrap();
        let recv_waker = poll_pending(&mut recv_fut);

        // drop original sender
        drop(tx0);

        // not yet closed (there's a clone left)
        assert!(!recv_waker.woken());
        poll_ready(&mut tx1.send("b")).unwrap();
        let recv_waker = poll_pending(&mut recv_fut);

        // create new clone
        let tx0_clone2 = tx0_clone.clone();
        assert!(!recv_waker.woken());
        poll_ready(&mut tx1.send("c")).unwrap();
        let recv_waker = poll_pending(&mut recv_fut);

        // drop first clone
        drop(tx0_clone);
        assert!(!recv_waker.woken());
        poll_ready(&mut tx1.send("d")).unwrap();
        let recv_waker = poll_pending(&mut recv_fut);

        // drop last clone
        drop(tx0_clone2);

        // channel closed => receiver no longer waits => also close gate
        poll_pending(&mut tx1.send("e"));
        assert!(recv_waker.woken());
        assert_eq!(poll_ready(&mut recv_fut), None,);
    }

    #[test]
    fn test_close_channel_by_dropping_rx_on_open_gate() {
        let (txs, mut rxs) = channels(2, 0);

        let mut rx0 = rxs.remove(0);
        let _rx1 = rxs.remove(0);

        // gate open because receiver 0 waits for data
        poll_pending(&mut rx0.recv());
        poll_ready(&mut txs[1].send("a")).unwrap();

        // drop receiver => also close gate
        drop(rx0);

        poll_pending(&mut txs[1].send("b"));
        assert_eq!(poll_ready(&mut txs[0].send("foo")), Err(SendError("foo")),);
    }

    #[test]
    fn test_close_channel_by_dropping_rx_on_closed_gate() {
        let (txs, mut rxs) = channels(2, 1);

        let rx0 = rxs.remove(0);
        let mut rx1 = rxs.remove(0);

        // fill both channels, channel 1 over the bound
        poll_ready(&mut txs[0].send("0_a")).unwrap();
        poll_ready(&mut txs[1].send("1_a")).unwrap();
        poll_ready(&mut txs[1].send("1_b")).unwrap();

        let mut send_fut0 = txs[0].send("0_b");
        let mut send_fut1 = txs[1].send("1_c");
        let waker0 = poll_pending(&mut send_fut0);
        let waker1 = poll_pending(&mut send_fut1);

        // drop receiver
        drop(rx0);

        assert!(waker0.woken());
        assert!(!waker1.woken());
        assert_eq!(poll_ready(&mut send_fut0), Err(SendError("0_b")),);

        // gate still closed, so cannot send on channel 1
        poll_pending(&mut send_fut1);

        // channel 1 can still receive data
        assert_eq!(poll_ready(&mut rx1.recv()), Some("1_a"),);
    }

    #[test]
    fn test_drop_rx_three_channels() {
        let (mut txs, mut rxs) = channels(3, 1);

        let tx0 = txs.remove(0);
        let tx1 = txs.remove(0);
        let tx2 = txs.remove(0);
        let mut rx0 = rxs.remove(0);
        let rx1 = rxs.remove(0);
        let _rx2 = rxs.remove(0);

        // fill channels, channel 1 over the bound
        poll_ready(&mut tx0.send("0_a")).unwrap();
        poll_ready(&mut tx1.send("1_a")).unwrap();
        poll_ready(&mut tx2.send("2_a")).unwrap();
        poll_ready(&mut tx1.send("1_b")).unwrap();

        let mut send_fut = tx2.send("2_b");
        let waker = poll_pending(&mut send_fut);

        // drop / close the channel that is over the bound => gate open
        drop(rx1);
        assert!(waker.woken());

        // receive data
        assert_eq!(poll_ready(&mut rx0.recv()), Some("0_a"),);

        // use senders again
        poll_ready(&mut tx0.send("0_b")).unwrap();
        assert_eq!(poll_ready(&mut tx1.send("1_c")), Err(SendError("1_c")),);
        poll_ready(&mut send_fut).unwrap();
        poll_pending(&mut tx0.send("0_c"));
    }

    #[test]
    fn test_has_receivers() {
        let (txs, mut rxs) = channels::<u8>(2, 0);

        let rx0 = rxs.remove(0);
        let rx1 = rxs.remove(0);

        // every sender sees the receivers of all channels
        drop(rx0);
        assert!(txs[0].has_receivers());
        drop(rx1);
        assert!(!txs[0].has_receivers());
        assert!(!txs[1].has_receivers());
    }

    #[test]
    fn test_close_channel_by_dropping_rx_clears_data() {
        let (txs, rxs) = channels(1, 0);

        let obj = Arc::new(());
        let counter = Arc::downgrade(&obj);
        assert_eq!(counter.strong_count(), 1);

        // add object to channel
        poll_ready(&mut txs[0].send(obj)).unwrap();
        assert_eq!(counter.strong_count(), 1);

        // drop receiver
        drop(rxs);

        assert_eq!(counter.strong_count(), 0);
    }

    /// How a receiver behaves in [`test_concurrent_senders_and_receivers`].
    #[derive(Clone, Copy, PartialEq, Eq)]
    enum Receive {
        /// Receive all data.
        All,
        /// Receive all data, starting after a delay.
        Late,
        /// Receive all data, yielding to other tasks regularly.
        Slow,
        /// Receive all data, starting after the senders are gone.
        Last,
        /// Drop the receiver after some data.
        Some(usize),
        /// Drop the receiver once the channel is over the bound.
        UntilFull,
    }

    /// Ensure that senders are woken whenever the gate opens, with senders and receivers on multiple threads.
    ///
    /// In one kind of round, the receivers only receive data that is known to be in the channel. They never
    /// wait, so the senders depend on being woken when data is received or a receiver is dropped. In the
    /// other kind of round, one receiver does not receive anything before the senders are gone, so the
    /// senders depend on being woken when the other receivers wait.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_concurrent_senders_and_receivers() {
        for seed in 0..120 {
            tokio::time::timeout(Duration::from_secs(20), concurrent_round(seed))
                .await
                .unwrap_or_else(|_| panic!("round {seed} does not finish"));
        }
    }

    async fn concurrent_round(seed: u64) {
        const ELEMENTS: usize = 400;

        let mut rng = StdRng::seed_from_u64(seed);
        let waiting = rng.random_bool(0.5);
        let clear = waiting && rng.random_bool(0.5);
        let bound = [0, 1, 2, 5][rng.random_range(0..4)];
        let n = rng.random_range(2..=6);
        let (txs, rxs) = channels::<(usize, usize)>(n, bound);
        let sent = Arc::new((0..n).map(|_| AtomicUsize::new(0)).collect::<Vec<_>>());
        let done = Arc::new(AtomicBool::new(false));

        let mut receivers = vec![];
        for (i, mut rx) in rxs.into_iter().enumerate() {
            let receive = match (i, rng.random_range(0..6)) {
                // the senders can always continue when this receiver waits
                (0, _) if waiting => Receive::All,
                (1, _) if waiting => Receive::Last,
                (_, 0) => Receive::Late,
                (_, 1) => Receive::Slow,
                (_, 2) => Receive::Some(rng.random_range(0..ELEMENTS / n)),
                (_, 3) => Receive::UntilFull,
                _ => Receive::All,
            };
            let (sent, done) = (Arc::clone(&sent), Arc::clone(&done));
            receivers.push(tokio::spawn(async move {
                if receive == Receive::Late {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
                let mut count = 0;
                let mut next = [0; 2];
                loop {
                    // an element can be received before the sender has counted it
                    let available = sent[i].load(Ordering::SeqCst).saturating_sub(count);
                    if receive == Receive::Some(count)
                        || (receive == Receive::UntilFull && available > bound)
                    {
                        return None;
                    }
                    let done = done.load(Ordering::SeqCst);
                    if (receive == Receive::Last && !done) || (!waiting && available == 0) {
                        if done && sent[i].load(Ordering::SeqCst) == count {
                            break;
                        }
                        tokio::task::yield_now().await;
                        continue;
                    }
                    let Some((sender, element)) = rx.recv().await else {
                        break;
                    };
                    // elements of each sender are received in order
                    assert!(element >= next[sender]);
                    next[sender] = element + 1;
                    count += 1;
                    if receive == Receive::Slow && count % 16 == 0 {
                        tokio::task::yield_now().await;
                    }
                }
                Some(count)
            }));
        }

        let mut senders = vec![];
        for (sender, txs) in [txs.clone(), txs].into_iter().enumerate() {
            let mut rng = StdRng::seed_from_u64(rng.random());
            let sent = Arc::clone(&sent);
            senders.push(tokio::spawn(async move {
                let mut txs = txs.into_iter().map(Some).collect::<Vec<_>>();
                for element in 0..ELEMENTS {
                    // some channels get more data than others
                    let i = if rng.random_bool(0.3) {
                        n - 1
                    } else {
                        rng.random_range(0..n)
                    };
                    let Some(tx) = &txs[i] else {
                        continue;
                    };
                    if clear && rng.random_ratio(1, 50) {
                        tx.clear();
                    }
                    if tx.send((sender, element)).await.is_ok() {
                        sent[i].fetch_add(1, Ordering::SeqCst);
                    } else {
                        txs[i] = None;
                    }
                }
            }));
        }

        for sender in senders {
            sender.await.unwrap();
        }
        done.store(true, Ordering::SeqCst);
        for (i, receiver) in receivers.into_iter().enumerate() {
            let Some(count) = receiver.await.unwrap() else {
                continue;
            };
            let sent = sent[i].load(Ordering::SeqCst);
            if clear {
                assert!(count <= sent);
            } else {
                assert_eq!(count, sent);
            }
        }
    }

    /// Ensure that polling "pending" futures work even when you poll them too often (which happens under some circumstances).
    #[test]
    fn test_poll_empty_channel_twice() {
        let (txs, mut rxs) = channels(1, 0);

        let mut recv_fut = rxs[0].recv();
        let waker_1a = poll_pending(&mut recv_fut);
        let waker_1b = poll_pending(&mut recv_fut);

        let mut recv_fut = rxs[0].recv();
        let waker_2 = poll_pending(&mut recv_fut);

        poll_ready(&mut txs[0].send("a")).unwrap();
        assert!(waker_1a.woken());
        assert!(waker_1b.woken());
        assert!(waker_2.woken());
        assert_eq!(poll_ready(&mut recv_fut), Some("a"),);

        poll_ready(&mut txs[0].send("b")).unwrap();
        let mut send_fut = txs[0].send("c");
        let waker_3 = poll_pending(&mut send_fut);
        assert_eq!(poll_ready(&mut rxs[0].recv()), Some("b"),);
        assert!(waker_3.woken());
        poll_ready(&mut send_fut).unwrap();
        assert_eq!(poll_ready(&mut rxs[0].recv()), Some("c"));

        let mut recv_fut = rxs[0].recv();
        let waker_4 = poll_pending(&mut recv_fut);

        let mut recv_fut = rxs[0].recv();
        let waker_5 = poll_pending(&mut recv_fut);

        poll_ready(&mut txs[0].send("d")).unwrap();
        let mut send_fut = txs[0].send("e");
        let waker_6a = poll_pending(&mut send_fut);
        let waker_6b = poll_pending(&mut send_fut);

        assert!(waker_4.woken());
        assert!(waker_5.woken());
        assert_eq!(poll_ready(&mut recv_fut), Some("d"),);

        assert!(waker_6a.woken());
        assert!(waker_6b.woken());
        poll_ready(&mut send_fut).unwrap();
    }

    #[test]
    #[should_panic(expected = "polled ready future")]
    fn test_panic_poll_send_future_after_ready_ok() {
        let (txs, _rxs) = channels(1, 0);
        let mut fut = txs[0].send("foo");
        poll_ready(&mut fut).unwrap();
        poll_ready(&mut fut).ok();
    }

    #[test]
    #[should_panic(expected = "polled ready future")]
    fn test_panic_poll_send_future_after_ready_err() {
        let (txs, rxs) = channels(1, 0);

        drop(rxs);

        let mut fut = txs[0].send("foo");
        poll_ready(&mut fut).unwrap_err();
        poll_ready(&mut fut).ok();
    }

    #[test]
    #[should_panic(expected = "polled ready future")]
    fn test_panic_poll_recv_future_after_ready_some() {
        let (txs, mut rxs) = channels(1, 0);

        poll_ready(&mut txs[0].send("foo")).unwrap();

        let mut fut = rxs[0].recv();
        poll_ready(&mut fut).unwrap();
        poll_ready(&mut fut);
    }

    #[test]
    #[should_panic(expected = "polled ready future")]
    fn test_panic_poll_recv_future_after_ready_none() {
        let (txs, mut rxs) = channels::<u8>(1, 0);

        drop(txs);

        let mut fut = rxs[0].recv();
        assert!(poll_ready(&mut fut).is_none());
        poll_ready(&mut fut);
    }

    #[test]
    #[should_panic(expected = "future is pending")]
    fn test_meta_poll_ready_wrong_state() {
        let mut fut = futures::future::pending::<u8>();
        poll_ready(&mut fut);
    }

    #[test]
    #[should_panic(expected = "future is ready")]
    fn test_meta_poll_pending_wrong_state() {
        let mut fut = futures::future::ready(1);
        poll_pending(&mut fut);
    }

    /// Test [`poll_pending`] (i.e. the testing utils, not the actual library code).
    #[test]
    fn test_meta_poll_pending_waker() {
        let (tx, mut rx) = futures::channel::oneshot::channel();
        let waker = poll_pending(&mut rx);
        assert!(!waker.woken());
        tx.send(1).unwrap();
        assert!(waker.woken());
    }

    /// Poll a given [`Future`] and ensure it is [ready](Poll::Ready).
    #[track_caller]
    fn poll_ready<F>(fut: &mut F) -> F::Output
    where
        F: Future + Unpin,
    {
        match poll(fut).0 {
            Poll::Ready(x) => x,
            Poll::Pending => panic!("future is pending"),
        }
    }

    /// Poll a given [`Future`] and ensure it is [pending](Poll::Pending).
    ///
    /// Returns a waker that can later be checked.
    #[track_caller]
    fn poll_pending<F>(fut: &mut F) -> Arc<TestWaker>
    where
        F: Future + Unpin,
    {
        let (res, waker) = poll(fut);
        match res {
            Poll::Ready(_) => panic!("future is ready"),
            Poll::Pending => waker,
        }
    }

    fn poll<F>(fut: &mut F) -> (Poll<F::Output>, Arc<TestWaker>)
    where
        F: Future + Unpin,
    {
        let test_waker = Arc::new(TestWaker::default());
        let waker = futures::task::waker(Arc::clone(&test_waker));
        let mut cx = Context::from_waker(&waker);
        let res = fut.poll_unpin(&mut cx);
        (res, test_waker)
    }

    /// A test [`Waker`] that signal if [`wake`](Waker::wake) was called.
    #[derive(Debug, Default)]
    struct TestWaker {
        woken: AtomicBool,
    }

    impl TestWaker {
        /// Was [`wake`](Waker::wake) called?
        fn woken(&self) -> bool {
            self.woken.load(Ordering::SeqCst)
        }
    }

    impl ArcWake for TestWaker {
        fn wake_by_ref(arc_self: &Arc<Self>) {
            arc_self.woken.store(true, Ordering::SeqCst);
        }
    }
}
