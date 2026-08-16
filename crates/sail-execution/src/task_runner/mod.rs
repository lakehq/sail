use std::any::Any;

mod actor;

pub(crate) use actor::{TaskRunnerActor, TaskRunnerMessage};
mod monitor;

pub use actor::{TaskRunnerComponents, TaskRunnerExtensions, TaskRunnerPlacement};

fn panic_message(payload: Box<dyn Any + Send>) -> String {
    if let Some(message) = payload.downcast_ref::<&str>() {
        (*message).to_string()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else {
        "unknown panic".to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_panic_messages() {
        assert_eq!(panic_message(Box::new("literal panic")), "literal panic");
        assert_eq!(
            panic_message(Box::new("owned panic".to_string())),
            "owned panic"
        );
        assert_eq!(panic_message(Box::new(42)), "unknown panic");
    }
}
