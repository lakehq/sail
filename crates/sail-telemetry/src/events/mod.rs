// SPDX-License-Identifier: Apache-2.0

mod processor;
mod reporter;

pub const SYSTEM_EVENT_NAME: &str = "sail.system";

pub use processor::SystemEventLogProcessor;
pub use reporter::SystemEventReporter;
