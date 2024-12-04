use std::fmt::Debug;

use log::error;
use tokio::sync::mpsc;
use tokio::task::{AbortHandle, JoinSet};

const ACTOR_CHANNEL_SIZE: usize = 8;

#[tonic::async_trait]
pub trait Actor: Sized + Send + 'static {
    type Message: Send + 'static;
    type Options;

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
    Warn(String),
    Fail(String),
    Stop,
}

impl ActorAction {
    pub fn warn(message: impl ToString) -> Self {
        Self::Warn(message.to_string())
    }

    pub fn fail(message: impl ToString) -> Self {
        Self::Fail(message.to_string())
    }
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
        self.tasks.spawn(task)
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
    sender: mpsc::Sender<T::Message>,
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
        self.sender.send(message).await
    }
}

struct ActorRunner<T: Actor> {
    actor: T,
    ctx: ActorContext<T>,
    receiver: mpsc::Receiver<T::Message>,
}

impl<T: Actor> ActorRunner<T> {
    async fn run(mut self) {
        self.actor.start(&mut self.ctx).await;
        while let Some(message) = self.receiver.recv().await {
            let action = self.actor.receive(&mut self.ctx, message);
            match action {
                ActorAction::Continue => {}
                ActorAction::Warn(message) => {
                    log::warn!("{message}");
                }
                ActorAction::Fail(message) => {
                    log::error!("{message}");
                    break;
                }
                ActorAction::Stop => {
                    break;
                }
            }
            self.ctx.reap();
        }
        self.actor.stop(&mut self.ctx).await;
    }
}

#[cfg(test)]
mod tests {
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

    #[tonic::async_trait]
    impl Actor for TestActor {
        type Message = TestMessage;
        type Options = ();

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
