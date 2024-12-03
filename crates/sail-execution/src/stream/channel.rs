use std::fmt::Display;

/// An opaque channel name that can be used to identify a channel
/// for streaming data between workers. It can also be used to determine
/// the file path when persisting stream data in the worker's local storage.
/// The channel name must be unique within the entire lifetime of a worker.
/// The driver is responsible for generating unique channel names.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ChannelName(String);

impl ChannelName {
    pub fn new(name: impl Into<String>) -> Self {
        Self(name.into())
    }

    pub fn has_prefix(&self, prefix: &str) -> bool {
        self.0.starts_with(prefix)
    }
}

impl From<String> for ChannelName {
    fn from(name: String) -> Self {
        Self(name)
    }
}

impl From<ChannelName> for String {
    fn from(channel: ChannelName) -> String {
        channel.0
    }
}

impl Display for ChannelName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
