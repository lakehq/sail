use log::error;
use tokio::sync::{mpsc, watch};
use tokio::task::{AbortHandle, JoinSet};

const ACTOR_CHANNEL_SIZE: usize = 8;

pub trait Actor: Sized + Send + 'static {
    type Message: Send + 'static;
    type Options;

    fn new(options: Self::Options) -> Self;
    fn start(&mut self, ctx: &mut ActorContext<Self>);
    /// Process one message and return the next action.
    /// This method should handle errors internally (e.g. by sending itself an error message
    /// for further processing).
    /// This method should not invoke any blocking functions, otherwise the actor event loop
    /// would be blocked since all messages are processed sequentially in a single thread.
    /// If the actor needs to perform async operations, it should spawn tasks via
    /// [ActorContext::spawn].
    fn receive(&mut self, ctx: &mut ActorContext<Self>, message: Self::Message) -> ActorAction;
    fn stop(self);
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

pub struct ActorHandle<T: Actor> {
    sender: mpsc::Sender<T::Message>,
    stopped: watch::Receiver<bool>,
}

impl<T: Actor> Clone for ActorHandle<T> {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
            stopped: self.stopped.clone(),
        }
    }
}

impl<T: Actor> ActorHandle<T> {
    pub fn new(options: T::Options) -> Self {
        let (tx, rx) = mpsc::channel(ACTOR_CHANNEL_SIZE);
        let (stopped_tx, stopped_rx) = watch::channel::<bool>(false);
        let handle = Self {
            sender: tx,
            stopped: stopped_rx,
        };
        let runner = ActorRunner {
            actor: T::new(options),
            ctx: ActorContext::new(&handle),
            receiver: rx,
            stopped: stopped_tx,
        };
        // Currently we do not save the handle to the actor event loop task.
        // So the actor runs "detached" and the event loop task will stop by itself
        // when the stop action is taken.
        tokio::spawn(runner.run());
        handle
    }

    pub async fn send(
        &self,
        message: T::Message,
    ) -> Result<(), mpsc::error::SendError<T::Message>> {
        self.sender.send(message).await
    }

    pub async fn wait_for_stop(mut self) {
        // We ignore the receiver error since the sender must have been dropped in this case,
        // which means the actor has stopped.
        let _ = self.stopped.wait_for(|x| *x).await;
    }
}

struct ActorRunner<T: Actor> {
    actor: T,
    ctx: ActorContext<T>,
    receiver: mpsc::Receiver<T::Message>,
    stopped: watch::Sender<bool>,
}

impl<T: Actor> ActorRunner<T> {
    async fn run(mut self) {
        self.actor.start(&mut self.ctx);
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
        self.actor.stop();
        let _ = self.stopped.send(true);
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

    impl Actor for TestActor {
        type Message = TestMessage;
        type Options = ();

        fn new(_options: Self::Options) -> Self {
            Self
        }

        fn start(&mut self, _: &mut ActorContext<Self>) {}

        fn receive(&mut self, _: &mut ActorContext<Self>, message: Self::Message) -> ActorAction {
            match message {
                TestMessage::Echo { value, reply } => {
                    let _ = reply.send(value.to_uppercase());
                    ActorAction::Continue
                }
                TestMessage::Stop => ActorAction::Stop,
            }
        }

        fn stop(self) {}
    }

    #[tokio::test]
    async fn test_actor_handle_send() {
        let handle = ActorHandle::<TestActor>::new(());
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
    async fn test_actor_handle_wait_for_stop() {
        let handle = ActorHandle::<TestActor>::new(());
        let result = handle.send(TestMessage::Stop).await;
        assert!(matches!(result, Ok(())));

        handle.clone().wait_for_stop().await;
        // Multiple handles should be able to wait for the actor to stop.
        handle.wait_for_stop().await;
    }
}
