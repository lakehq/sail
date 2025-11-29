use fastrace::local::LocalSpan;
use fastrace::Event;
use log::{Log, Metadata, Record};

/// A logger that logs the record as fastrace span events.
pub struct SpanEventLogger;

impl Log for SpanEventLogger {
    fn enabled(&self, _metadata: &Metadata) -> bool {
        true
    }

    fn log(&self, record: &Record) {
        let event = Event::new(record.level().as_str())
            .with_properties(|| [("message", record.args().to_string())]);
        LocalSpan::add_event(event);
    }

    fn flush(&self) {}
}
