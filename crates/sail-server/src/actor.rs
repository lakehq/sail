use std::fmt::Debug;

use fastrace::future::FutureExt;
use fastrace::prelude::SpanContext;
use fastrace::Span;
use log::{error, warn};
use sail_telemetry::common::{SpanAssociation, SpanAttribute, SpanKind};
use tokio::sync::mpsc;
use tokio::task::{AbortHandle, JoinSet};

const ACTOR_CHANNEL_SIZE: usize = 8;

#[tonic::async_trait]
pub trait Actor: Sized + Send + 'static {
    type Message: Send + SpanAssociation + 'static;
    type Options;

    fn name() -> &'static str;

    fn new(options: Self::Options) -> Self;
    #[allow(unused_variables)]
    async fn start(&mut self, ctx: &mut ActorContext<Self>) {}
    /// Process one message and return the next action.
    /// This method should handle errors internally (e.g. by sending itself an error message
    /// for further processing).
    /// This method should not invoke any blocking functions, otherwise the actor event loop
    /// would be blocked since all messages are processed sequentially in a single thread.
    /// If the actor needs to perform async operations, it should spawn tasks via
    /// [ActorContext::spawn].
    fn receive(&mut self, ctx: &mut ActorContext<Self>, message: Self::Message) -> ActorAction;
    #[allow(unused_variables)]
    async fn stop(self, ctx: &mut ActorContext<Self>) {}
}

pub enum ActorAction {
    Continue,
    Stop,
}

pub struct ActorContext<T: Actor> {
    handle: ActorHandle<T>,
    /// A set of tasks spawned by the actor when processing messages.
    /// All these tasks will be aborted when the context is dropped.
    tasks: JoinSet<()>,
}

impl<T: Actor> ActorContext<T> {
    pub fn new(handle: &ActorHandle<T>) -> Self {
        Self {
            handle: handle.clone(),
            tasks: JoinSet::new(),
        }
    }

    pub fn handle(&self) -> &ActorHandle<T> {
        &self.handle
    }

    /// Spawn a task to send a message to the actor itself.
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

    /// Spawn a task and save the handle in the context.
    /// The task should handle errors internally.
    pub fn spawn(
        &mut self,
        task: impl std::future::Future<Output = ()> + Send + 'static,
    ) -> AbortHandle {
        let span = Span::enter_with_local_parent("ActorContext::spawn");
        self.tasks.spawn(task.in_span(span))
    }

    /// Join tasks that have completed.
    /// Any unhandled errors will be logged here.
    pub fn reap(&mut self) {
        while let Some(result) = self.tasks.try_join_next() {
            match result {
                Ok(()) => {}
                Err(e) => {
                    error!("failed to join task spawned by actor: {e}");
                    continue;
                }
            }
        }
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

    pub fn spawn<T: Actor>(&mut self, options: T::Options) -> ActorHandle<T> {
        let (tx, rx) = mpsc::channel(ACTOR_CHANNEL_SIZE);
        let handle = ActorHandle { sender: tx };
        let runner = ActorRunner {
            actor: T::new(options),
            ctx: ActorContext::new(&handle),
            receiver: rx,
            start: Some(Span::enter_with_local_parent("ActorRunner")),
        };
        self.tasks.spawn(runner.run());
        handle
    }

    /// Wait for all the spawned actors to stop.
    /// The system can still be used to spawn new actors after this method is called.
    pub async fn join(&mut self) {
        while let Some(result) = self.tasks.join_next().await {
            match result {
                Ok(()) => {}
                Err(e) => {
                    error!("failed to join task spawned by actor system: {e}");
                }
            }
        }
    }
}

pub struct ActorHandle<T: Actor> {
    sender: mpsc::Sender<MessageEnvelop<T::Message>>,
}

impl<T: Actor> Debug for ActorHandle<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ActorHandle").finish()
    }
}

impl<T: Actor> Clone for ActorHandle<T> {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
        }
    }
}

impl<T: Actor> ActorHandle<T> {
    pub async fn send(
        &self,
        message: T::Message,
    ) -> Result<(), mpsc::error::SendError<T::Message>> {
        let span = Span::enter_with_local_parent(format!("Send {}.{}", T::name(), message.name()))
            .with_properties(|| message.properties())
            .with_property(|| (SpanAttribute::SPAN_KIND, SpanKind::PRODUCER));
        self.sender
            .send(MessageEnvelop {
                message,
                context: SpanContext::from_span(&span),
            })
            .in_span(span)
            .await
            .map_err(
                |mpsc::error::SendError(MessageEnvelop {
                     message,
                     context: _,
                 })| { mpsc::error::SendError(message) },
            )
    }
}

struct ActorRunner<T: Actor> {
    actor: T,
    ctx: ActorContext<T>,
    receiver: mpsc::Receiver<MessageEnvelop<T::Message>>,
    start: Option<Span>,
}

impl<T: Actor> ActorRunner<T> {
    async fn run(mut self) {
        {
            let span = self.start.take().unwrap_or_default();
            self.actor.start(&mut self.ctx).in_span(span).await;
            // The span is dropped here to conclude the actor start phase.
        }
        while let Some(MessageEnvelop { message, context }) = self.receiver.recv().await {
            let span = if let Some(context) = context {
                Span::root(format!("Receive {}.{}", T::name(), message.name()), context)
                    .with_property(|| (SpanAttribute::SPAN_KIND, SpanKind::CONSUMER))
            } else {
                Span::noop()
            };
            let _guard = span.set_local_parent();
            let action = self.actor.receive(&mut self.ctx, message);
            match action {
                ActorAction::Continue => {}
                ActorAction::Stop => {
                    break;
                }
            }
            self.ctx.reap();
        }
        // The receiver will be dropped at the end of this function call,
        // and the other end of the channel will then know that the actor is no longer running.
        // But here we explicitly close the receiver so that the other end knows sooner
        // that the actor is no longer running, since the actor may take some time to stop.
        self.receiver.close();
        self.actor.stop(&mut self.ctx).await;
        self.ctx.reap();
        // The remaining tasks will be aborted when the `ActorContext` is dropped.
        let n = self.ctx.tasks.len();
        if n > 0 {
            warn!("aborting {n} task(s) for {}", T::name());
        }
    }
}

/// A wrapper for an actor message with a tracing span.
struct MessageEnvelop<M> {
    message: M,
    context: Option<SpanContext>,
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;

    use tokio::sync::oneshot;

    use super::*;

    struct TestActor;

    enum TestMessage {
        Echo {
            value: String,
            reply: oneshot::Sender<String>,
        },
        Stop,
    }

    impl SpanAssociation for TestMessage {
        fn name(&self) -> Cow<'static, str> {
            match self {
                TestMessage::Echo { .. } => "Echo".into(),
                TestMessage::Stop => "Stop".into(),
            }
        }

        fn properties(&self) -> impl IntoIterator<Item = (Cow<'static, str>, Cow<'static, str>)> {
            match self {
                TestMessage::Echo { value, .. } => vec![("value".into(), value.clone().into())],
                TestMessage::Stop => vec![],
            }
        }
    }

    #[tonic::async_trait]
    impl Actor for TestActor {
        type Message = TestMessage;
        type Options = ();

        fn name() -> &'static str {
            "TestActor"
        }

        fn new(_options: Self::Options) -> Self {
            Self
        }

        fn receive(&mut self, _: &mut ActorContext<Self>, message: Self::Message) -> ActorAction {
            match message {
                TestMessage::Echo { value, reply } => {
                    let _ = reply.send(value.to_uppercase());
                    ActorAction::Continue
                }
                TestMessage::Stop => ActorAction::Stop,
            }
        }
    }

    #[tokio::test]
    async fn test_actor_handle_send() {
        let mut system = ActorSystem::new();
        let handle = system.spawn::<TestActor>(());
        assert!(!handle.sender.is_closed());
        let (tx, rx) = oneshot::channel();
        let result = handle
            .send(TestMessage::Echo {
                value: "hello".to_string(),
                reply: tx,
            })
            .await;
        assert!(matches!(result, Ok(())));
        assert_eq!(rx.await, Ok("HELLO".to_string()));
    }

    #[tokio::test]
    async fn test_actor_handle_stop() {
        let mut system = ActorSystem::new();
        let handle = system.spawn::<TestActor>(());
        let result = handle.send(TestMessage::Stop).await;
        assert!(matches!(result, Ok(())));
        system.join().await;
    }
}
