use log::error;
use tokio::sync::{mpsc, watch};
use tokio::task::{AbortHandle, JoinSet};

const ACTOR_CHANNEL_SIZE: usize = 8;

pub trait Actor: Sized + Send + 'static {
    type Message: Send + 'static;
    type Options;
    type Error: From<mpsc::error::SendError<Self::Message>> + std::fmt::Display + Send;

    fn new(options: Self::Options) -> Self;
    fn start(&mut self, ctx: &mut ActorContext<Self>) -> Result<(), Self::Error>;
    /// Process one message and return the next action.
    /// This method should only return errors when they are not recoverable.
    /// In such a situation, the actor will be stopped.
    /// If the actor can recover from the error, it should handle it inside the method
    /// and return [Ok].
    /// This method should not invoke any blocking functions, otherwise the actor event loop
    /// would be blocked since all messages are processed sequentially in a single thread.
    /// If the actor needs to perform async operations, it should spawn tasks via
    /// [ActorContext::spawn].
    fn receive(
        &mut self,
        ctx: &mut ActorContext<Self>,
        message: Self::Message,
    ) -> Result<ActorAction, Self::Error>;
    fn stop(self) -> Result<(), Self::Error>;
}

pub enum ActorAction {
    Continue,
    Stop,
}

pub struct ActorContext<T: Actor> {
    handle: ActorHandle<T>,
    /// A set of tasks spawned by the actor when processing messages.
    /// All these tasks will be aborted when the context is dropped.
    tasks: JoinSet<Result<(), T::Error>>,
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
    pub fn spawn(
        &mut self,
        task: impl std::future::Future<Output = Result<(), T::Error>> + Send + 'static,
    ) -> AbortHandle {
        self.tasks.spawn(task)
    }

    /// Join tasks that have completed and log errors.
    /// When the actor expects to handle errors, it should add the logic
    /// inside the task (e.g. sending itself a message on error).
    /// Any unhandled errors will be logged here.
    pub fn reap(&mut self) {
        while let Some(result) = self.tasks.try_join_next() {
            let result = match result {
                Ok(x) => x,
                Err(e) => {
                    error!("failed to join task spawned by actor: {e}");
                    continue;
                }
            };
            match result {
                Ok(()) => {}
                Err(e) => {
                    error!("actor task failed: {e}");
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

    pub async fn send(&self, message: T::Message) -> Result<(), T::Error> {
        self.sender.send(message).await.map_err(T::Error::from)
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
        let out = async {
            self.actor.start(&mut self.ctx)?;
            while let Some(message) = self.receiver.recv().await {
                let action = self.actor.receive(&mut self.ctx, message)?;
                match action {
                    ActorAction::Continue => {}
                    ActorAction::Stop => {
                        break;
                    }
                }
                self.ctx.reap();
            }
            self.actor.stop()
        }
        .await;
        match out {
            Ok(()) => {}
            Err(e) => {
                error!("actor failed: {e}");
            }
        }
        let _ = self.stopped.send(true);
    }
}

#[cfg(test)]
mod tests {
    use tokio::sync::{mpsc, oneshot, watch};

    use super::*;

    struct TestActor;

    #[derive(Clone)]
    struct TestError;

    impl std::fmt::Display for TestError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "test error")
        }
    }

    impl<T> From<mpsc::error::SendError<T>> for TestError {
        fn from(_: mpsc::error::SendError<T>) -> Self {
            Self
        }
    }

    impl From<watch::error::RecvError> for TestError {
        fn from(_: watch::error::RecvError) -> Self {
            Self
        }
    }

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
        type Error = TestError;

        fn new(_options: Self::Options) -> Self {
            Self
        }

        fn start(&mut self, _: &mut ActorContext<Self>) -> Result<(), Self::Error> {
            Ok(())
        }

        fn receive(
            &mut self,
            _: &mut ActorContext<Self>,
            message: Self::Message,
        ) -> Result<ActorAction, Self::Error> {
            match message {
                TestMessage::Echo { value, reply } => {
                    let _ = reply.send(value.to_uppercase());
                    Ok(ActorAction::Continue)
                }
                TestMessage::Stop => Ok(ActorAction::Stop),
            }
        }

        fn stop(self) -> Result<(), Self::Error> {
            Ok(())
        }
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
