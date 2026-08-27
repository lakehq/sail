mod core;
mod handler;
mod message;

pub use message::SystemEventActorMessage;

use super::store::SystemEventStore;

/// Owns the materialized system-table rows and applies system-event CRUD messages serially.
#[derive(Default)]
pub struct SystemEventActor {
    pub(super) store: SystemEventStore,
}
