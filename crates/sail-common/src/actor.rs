use std::fmt::Debug;
use std::future::Future;
use std::marker::PhantomData;
use std::num::NonZeroUsize;

use fastrace::Span;
use fastrace::future::FutureExt;
use fastrace::prelude::SpanContext;
use log::{error, warn};
use tokio::sync::mpsc;
use tokio::task::{AbortHandle, JoinSet};

use crate::telemetry::{SpanAssociation, SpanAttribute, SpanKind};

const ACTOR_CHANNEL_SIZE: usize = 8;

pub struct BoundedMailbox;
pub struct UnboundedMailbox;

mod private {
    pub trait Sealed {}
}

impl private::Sealed for BoundedMailbox {}
impl private::Sealed for UnboundedMailbox {}

pub trait Mailbox<M>: private::Sealed + Send + Sync + 'static
where
    M: Send + 'static,
{
    type Sender: Clone + Send + Sync + 'static;
    type Receiver: Send + 'static;

    fn recv(receiver: &mut Self::Receiver) -> impl Future<Output = Option<M>> + Send + '_;

    fn close(receiver: &mut Self::Receiver);
}

impl<M: Send + 'static> Mailbox<M> for BoundedMailbox {
    type Sender = mpsc::Sender<M>;
    type Receiver = mpsc::Receiver<M>;

    fn recv(receiver: &mut Self::Receiver) -> impl Future<Output = Option<M>> + Send + '_ {
        receiver.recv()
    }

    fn close(receiver: &mut Self::Receiver) {
        receiver.close();
    }
}

impl<M: Send + 'static> Mailbox<M> for UnboundedMailbox {
    type Sender = mpsc::UnboundedSender<M>;
    type Receiver = mpsc::UnboundedReceiver<M>;

    fn recv(receiver: &mut Self::Receiver) -> impl Future<Output = Option<M>> + Send + '_ {
        receiver.recv()
    }

    fn close(receiver: &mut Self::Receiver) {
        receiver.close();
    }
}

pub trait Actor<Q = BoundedMailbox>: Sized + Send + 'static
where
    Q: Mailbox<MessageEnvelope<Self::Message>>,
{
    type Message: Send + SpanAssociation + 'static;
    type Options;

    fn name() -> &'static str;
    fn new(options: Self::Options) -> Self;

    fn start(&mut self, _: &mut ActorContext<Self, Q>) -> impl Future<Output = ()> + Send {
        std::future::ready(())
    }

    /// Process one message and return the next action.
    ///
    /// This method should handle errors internally, for example by sending an error message for
    /// later processing. It may await blocking tasks that run inside the message handler. But
    /// note that while it is running, the event loop waits and processes no other messages, so any
    /// blocking operation should be designed carefully to avoid blocking the actor for too long.
    /// If the actor needs to perform async operations that do not need to happen sequentially,
    /// it should spawn tasks via [ActorContext::spawn].
    fn receive(
        &mut self,
        ctx: &mut ActorContext<Self, Q>,
        message: Self::Message,
    ) -> impl Future<Output = ActorAction> + Send;

    fn stop(self, _: &mut ActorContext<Self, Q>) -> impl Future<Output = ()> + Send {
        std::future::ready(())
    }
}

pub enum ActorAction {
    Continue,
    Stop,
}

pub struct ActorContext<T, Q = BoundedMailbox>
where
    T: Actor<Q>,
    Q: Mailbox<MessageEnvelope<T::Message>>,
{
    handle: ActorHandle<T, Q>,
    /// A set of tasks spawned by the actor when processing messages.
    /// All these tasks will be aborted when the context is dropped.
    tasks: JoinSet<()>,
    /// Actors owned by this actor. All child actors will be aborted when the context is dropped.
    children: ActorSystem,
}

impl<T, Q> ActorContext<T, Q>
where
    T: Actor<Q>,
    Q: Mailbox<MessageEnvelope<T::Message>>,
{
    pub fn new(handle: &ActorHandle<T, Q>) -> Self {
        Self {
            handle: handle.clone(),
            tasks: JoinSet::new(),
            children: ActorSystem::new(),
        }
    }

    pub fn handle(&self) -> &ActorHandle<T, Q> {
        &self.handle
    }

    /// Spawn a task and save the handle in the context.
    /// The task should handle errors internally.
    pub fn spawn(&mut self, task: impl Future<Output = ()> + Send + 'static) -> AbortHandle {
        let span = Span::enter_with_local_parent("ActorContext::spawn");
        self.tasks.spawn(task.in_span(span))
    }

    /// Return the system that owns the children of this actor.
    pub fn children_mut(&mut self) -> &mut ActorSystem {
        &mut self.children
    }

    /// Join tasks that have completed.
    ///
    /// Any unhandled errors will be logged here.
    pub fn reap(&mut self) {
        while let Some(result) = self.tasks.try_join_next() {
            match result {
                Ok(()) => {}
                Err(e) => error!("failed to join task spawned by actor: {e}"),
            }
        }
        self.children.reap();
    }
}

impl<T> ActorContext<T, BoundedMailbox>
where
    T: Actor,
{
    /// Spawn a task to send a message to the actor itself.
    ///
    /// This does not await capacity in the actor's own mailbox, which could deadlock if it is
    /// full.
    pub fn send(&mut self, message: T::Message) {
        let handle = self.handle.clone();
        self.spawn(async move {
            let _ = handle.send(message).await;
        });
    }

    /// Spawn a task to send a message to the actor itself after a delay.
    pub fn send_with_delay(&mut self, message: T::Message, delay: std::time::Duration) {
        let handle = self.handle.clone();
        self.spawn(async move {
            tokio::time::sleep(delay).await;
            let _ = handle.send(message).await;
        });
    }
}

impl<T> ActorContext<T, UnboundedMailbox>
where
    T: Actor<UnboundedMailbox>,
{
    /// Send a message to the actor itself.
    pub fn send(&mut self, message: T::Message) {
        let _ = self.handle.send(message);
    }

    /// Spawn a task to send a message to the actor itself after a delay.
    pub fn send_with_delay(&mut self, message: T::Message, delay: std::time::Duration) {
        let handle = self.handle.clone();
        self.spawn(async move {
            tokio::time::sleep(delay).await;
            let _ = handle.send(message);
        });
    }
}

/// An actor system that manages a set of actors.
/// All actors will be aborted when the system is dropped.
pub struct ActorSystem {
    tasks: JoinSet<()>,
}

impl Default for ActorSystem {
    fn default() -> Self {
        Self::new()
    }
}

impl ActorSystem {
    pub fn new() -> Self {
        Self {
            tasks: JoinSet::new(),
        }
    }

    pub fn spawn<T>(&mut self, options: T::Options) -> ActorHandle<T>
    where
        T: Actor,
    {
        let (sender, receiver) = mpsc::channel(ACTOR_CHANNEL_SIZE);
        self.spawn_with(options, sender, receiver)
    }

    pub fn spawn_bounded<T>(
        &mut self,
        options: T::Options,
        capacity: NonZeroUsize,
    ) -> ActorHandle<T>
    where
        T: Actor,
    {
        let (sender, receiver) = mpsc::channel(capacity.get());
        self.spawn_with(options, sender, receiver)
    }

    pub fn spawn_unbounded<T>(&mut self, options: T::Options) -> ActorHandle<T, UnboundedMailbox>
    where
        T: Actor<UnboundedMailbox>,
    {
        let (sender, receiver) = mpsc::unbounded_channel();
        self.spawn_with(options, sender, receiver)
    }

    fn spawn_with<T, Q>(
        &mut self,
        options: T::Options,
        sender: Q::Sender,
        receiver: Q::Receiver,
    ) -> ActorHandle<T, Q>
    where
        T: Actor<Q>,
        Q: Mailbox<MessageEnvelope<T::Message>>,
    {
        let handle = ActorHandle {
            sender,
            _actor: PhantomData,
        };
        let runner = ActorRunner {
            actor: T::new(options),
            ctx: ActorContext::new(&handle),
            receiver,
            start: Some(Span::enter_with_local_parent("ActorRunner")),
        };
        self.tasks.spawn(runner.run());
        handle
    }

    /// Wait for all the spawned actors to stop.
    /// The system can still be used to spawn new actors after this method is called.
    ///
    /// Please note that the actors must have been sent a stop message before calling this method,
    /// otherwise this method will wait indefinitely, causing a deadlock.
    pub async fn join(&mut self) {
        while let Some(result) = self.tasks.join_next().await {
            match result {
                Ok(()) => {}
                Err(e) => error!("failed to join task spawned by actor system: {e}"),
            }
        }
    }

    /// Join actors that have already stopped.
    ///
    /// Any unhandled errors will be logged here.
    pub fn reap(&mut self) {
        while let Some(result) = self.tasks.try_join_next() {
            match result {
                Ok(()) => {}
                Err(e) => error!("failed to join actor: {e}"),
            }
        }
    }

    fn len(&self) -> usize {
        self.tasks.len()
    }
}

pub struct ActorHandle<T, Q = BoundedMailbox>
where
    T: Actor<Q>,
    Q: Mailbox<MessageEnvelope<T::Message>>,
{
    sender: Q::Sender,
    _actor: PhantomData<fn() -> T>,
}

impl<T, Q> Debug for ActorHandle<T, Q>
where
    T: Actor<Q>,
    Q: Mailbox<MessageEnvelope<T::Message>>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ActorHandle").finish()
    }
}

impl<T, Q> Clone for ActorHandle<T, Q>
where
    T: Actor<Q>,
    Q: Mailbox<MessageEnvelope<T::Message>>,
{
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
            _actor: PhantomData,
        }
    }
}

#[derive(Debug)]
pub struct ActorSendError<M> {
    pub message: M,
}

impl<M> std::fmt::Display for ActorSendError<M> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("failed to send message to actor")
    }
}

impl<M: Debug + 'static> std::error::Error for ActorSendError<M> {}

impl<T> ActorHandle<T, BoundedMailbox>
where
    T: Actor,
{
    pub async fn send(&self, message: T::Message) -> Result<(), ActorSendError<T::Message>> {
        let span = make_send_span::<T, BoundedMailbox>(&message);
        self.sender
            .send(MessageEnvelope {
                message,
                context: SpanContext::from_span(&span),
            })
            .in_span(span)
            .await
            .map_err(|mpsc::error::SendError(envelope)| ActorSendError {
                message: envelope.message,
            })
    }
}

impl<T> ActorHandle<T, UnboundedMailbox>
where
    T: Actor<UnboundedMailbox>,
{
    pub fn send(&self, message: T::Message) -> Result<(), ActorSendError<T::Message>> {
        let span = make_send_span::<T, UnboundedMailbox>(&message);
        self.sender
            .send(MessageEnvelope {
                message,
                context: SpanContext::from_span(&span),
            })
            .map_err(|mpsc::error::SendError(envelope)| ActorSendError {
                message: envelope.message,
            })
    }
}

fn make_send_span<T, Q>(message: &T::Message) -> Span
where
    T: Actor<Q>,
    Q: Mailbox<MessageEnvelope<T::Message>>,
{
    Span::enter_with_local_parent(format!("Send {}.{}", T::name(), message.name()))
        .with_properties(|| message.properties())
        .with_property(|| (SpanAttribute::SPAN_KIND, SpanKind::PRODUCER))
}

struct ActorRunner<T, Q = BoundedMailbox>
where
    T: Actor<Q>,
    Q: Mailbox<MessageEnvelope<T::Message>>,
{
    actor: T,
    ctx: ActorContext<T, Q>,
    receiver: Q::Receiver,
    start: Option<Span>,
}

impl<T, Q> ActorRunner<T, Q>
where
    T: Actor<Q>,
    Q: Mailbox<MessageEnvelope<T::Message>>,
{
    async fn run(mut self) {
        {
            let span = self.start.take().unwrap_or_default();
            self.actor.start(&mut self.ctx).in_span(span).await;
            // The span is dropped here to conclude the actor start phase.
        }
        while let Some(MessageEnvelope { message, context }) = Q::recv(&mut self.receiver).await {
            let span = if let Some(context) = context {
                Span::root(format!("Receive {}.{}", T::name(), message.name()), context)
                    .with_property(|| (SpanAttribute::SPAN_KIND, SpanKind::CONSUMER))
            } else {
                Span::noop()
            };
            let action = self
                .actor
                .receive(&mut self.ctx, message)
                .in_span(span)
                .await;
            match action {
                ActorAction::Continue => {}
                ActorAction::Stop => break,
            }
            self.ctx.reap();
        }
        // The receiver will be dropped at the end of this function call,
        // and the other end of the channel will then know that the actor is no longer running.
        // But here we explicitly close the receiver so that the other end knows sooner
        // that the actor is no longer running, since the actor may take some time to stop.
        Q::close(&mut self.receiver);
        self.actor.stop(&mut self.ctx).await;
        self.ctx.reap();
        // The remaining tasks will be aborted when the `ActorContext` is dropped.
        let n = self.ctx.tasks.len();
        if n > 0 {
            warn!("aborting {n} task(s) for {}", T::name());
        }
        let n = self.ctx.children.len();
        if n > 0 {
            warn!("aborting {n} child actor(s) for {}", T::name());
        }
    }
}

/// A wrapper for an actor message with a tracing span.
#[doc(hidden)]
pub struct MessageEnvelope<M> {
    message: M,
    context: Option<SpanContext>,
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;

    use tokio::sync::oneshot;

    use super::*;

    struct TestActor;
    struct UnboundedTestActor;

    struct ParentActor {
        child: Option<oneshot::Sender<ActorHandle<TestActor>>>,
    }

    enum TestMessage {
        Echo {
            value: String,
            reply: oneshot::Sender<String>,
        },
        Stop,
    }

    enum ParentMessage {
        Stop,
    }

    impl SpanAssociation for TestMessage {
        fn name(&self) -> Cow<'static, str> {
            match self {
                Self::Echo { .. } => "Echo".into(),
                Self::Stop => "Stop".into(),
            }
        }

        fn properties(&self) -> impl IntoIterator<Item = (Cow<'static, str>, Cow<'static, str>)> {
            match self {
                Self::Echo { value, .. } => vec![("value".into(), value.clone().into())],
                Self::Stop => vec![],
            }
        }
    }

    impl SpanAssociation for ParentMessage {
        fn name(&self) -> Cow<'static, str> {
            "Stop".into()
        }

        fn properties(&self) -> impl IntoIterator<Item = (Cow<'static, str>, Cow<'static, str>)> {
            vec![]
        }
    }

    impl Actor for TestActor {
        type Message = TestMessage;
        type Options = ();

        fn name() -> &'static str {
            "TestActor"
        }

        fn new(_: Self::Options) -> Self {
            Self
        }

        async fn receive(
            &mut self,
            _: &mut ActorContext<Self>,
            message: Self::Message,
        ) -> ActorAction {
            match message {
                TestMessage::Echo { value, reply } => {
                    let _ = reply.send(value.to_uppercase());
                    ActorAction::Continue
                }
                TestMessage::Stop => ActorAction::Stop,
            }
        }
    }

    impl Actor<UnboundedMailbox> for UnboundedTestActor {
        type Message = TestMessage;
        type Options = ();

        fn name() -> &'static str {
            "UnboundedTestActor"
        }

        fn new(_: Self::Options) -> Self {
            Self
        }

        async fn receive(
            &mut self,
            _: &mut ActorContext<Self, UnboundedMailbox>,
            message: Self::Message,
        ) -> ActorAction {
            match message {
                TestMessage::Echo { value, reply } => {
                    let _ = reply.send(value.to_uppercase());
                    ActorAction::Continue
                }
                TestMessage::Stop => ActorAction::Stop,
            }
        }
    }

    impl Actor for ParentActor {
        type Message = ParentMessage;
        type Options = oneshot::Sender<ActorHandle<TestActor>>;

        fn name() -> &'static str {
            "ParentActor"
        }

        fn new(child: Self::Options) -> Self {
            Self { child: Some(child) }
        }

        async fn start(&mut self, ctx: &mut ActorContext<Self>) {
            let child = ctx.children_mut().spawn::<TestActor>(());
            if let Some(sender) = self.child.take() {
                let _ = sender.send(child);
            }
        }

        async fn receive(&mut self, _: &mut ActorContext<Self>, _: Self::Message) -> ActorAction {
            ActorAction::Stop
        }
    }

    #[tokio::test]
    async fn test_bounded_actor_handle_send() {
        let mut system = ActorSystem::new();
        let handle = system.spawn::<TestActor>(());
        assert!(!handle.sender.is_closed());
        let (tx, rx) = oneshot::channel();
        assert!(
            handle
                .send(TestMessage::Echo {
                    value: "hello".to_string(),
                    reply: tx,
                })
                .await
                .is_ok()
        );
        assert_eq!(rx.await, Ok("HELLO".to_string()));
    }

    #[tokio::test]
    async fn test_unbounded_actor_handle_send() {
        let mut system = ActorSystem::new();
        let handle = system.spawn_unbounded::<UnboundedTestActor>(());
        assert!(!handle.sender.is_closed());
        let (tx, rx) = oneshot::channel();
        assert!(
            handle
                .send(TestMessage::Echo {
                    value: "hello".to_string(),
                    reply: tx,
                })
                .is_ok()
        );
        assert_eq!(rx.await, Ok("HELLO".to_string()));
    }

    #[tokio::test]
    async fn test_actor_handle_stop() {
        let mut system = ActorSystem::new();
        let handle = system.spawn::<TestActor>(());
        assert!(handle.send(TestMessage::Stop).await.is_ok());
        system.join().await;
    }

    #[tokio::test]
    async fn test_child_actor_stops_with_parent() {
        let mut system = ActorSystem::new();
        let (tx, rx) = oneshot::channel();
        let parent = system.spawn::<ParentActor>(tx);
        let child = rx.await;
        assert!(child.is_ok());
        let Ok(child) = child else {
            return;
        };

        assert!(parent.send(ParentMessage::Stop).await.is_ok());
        system.join().await;
        child.sender.closed().await;
        assert!(child.sender.is_closed());
    }
}
