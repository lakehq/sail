use tokio::sync::{mpsc, watch};

const ACTOR_CHANNEL_SIZE: usize = 8;

pub trait Actor: Sized + Send + 'static {
    type Message: Send + 'static;
    type Options;
    type Error: From<mpsc::error::SendError<Self::Message>>;

    fn new(options: Self::Options) -> Self;
    fn start(&mut self, handle: &ActorHandle<Self>) -> Result<(), Self::Error>;
    fn receive(
        &mut self,
        message: Self::Message,
        handle: &ActorHandle<Self>,
    ) -> Result<ActorAction, Self::Error>;
    fn stop(self) -> Result<(), Self::Error>;
}

pub enum ActorAction {
    Continue,
    Stop,
}

pub struct ActorHandle<T>
where
    T: Actor,
{
    sender: mpsc::Sender<T::Message>,
    stopped: watch::Receiver<bool>,
}

impl<T> Clone for ActorHandle<T>
where
    T: Actor,
{
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
            stopped: self.stopped.clone(),
        }
    }
}

impl<T: Actor> ActorHandle<T> {
    pub fn new(options: T::Options) -> Self {
        let (tx, mut rx) = mpsc::channel(ACTOR_CHANNEL_SIZE);
        let (stopped_tx, stopped_rx) = watch::channel::<bool>(false);
        let mut actor = T::new(options);
        let out = Self {
            sender: tx,
            stopped: stopped_rx,
        };
        let handle = out.clone();
        tokio::spawn(async move {
            let _ = async {
                actor.start(&handle)?;
                while let Some(message) = rx.recv().await {
                    let action = actor.receive(message, &handle)?;
                    match action {
                        ActorAction::Continue => {}
                        ActorAction::Stop => {
                            break;
                        }
                    }
                }
                actor.stop()
            }
            .await;
            let _ = stopped_tx.send(true);
        });
        out
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

#[cfg(test)]
mod tests {
    use tokio::sync::{mpsc, oneshot, watch};

    use super::*;

    struct TestActor;

    #[derive(Clone)]
    struct TestError;

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

        fn start(&mut self, _: &ActorHandle<Self>) -> Result<(), Self::Error> {
            Ok(())
        }

        fn receive(
            &mut self,
            message: Self::Message,
            _: &ActorHandle<Self>,
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
        assert_eq!(handle.sender.is_closed(), false);
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
