use log::{Log, Metadata, Record};

/// A logger that delegates logging to a primary logger,
/// and if the primary logger is enabled, delegates to secondary loggers as well.
pub struct CompositeLogger {
    primary: Box<dyn Log>,
    secondary: Vec<Box<dyn Log>>,
}

impl CompositeLogger {
    pub fn new(primary: Box<dyn Log>, secondary: Vec<Box<dyn Log>>) -> Self {
        CompositeLogger { primary, secondary }
    }
}

impl Log for CompositeLogger {
    fn enabled(&self, metadata: &Metadata) -> bool {
        self.primary.enabled(metadata)
    }

    fn log(&self, record: &Record) {
        if self.primary.enabled(record.metadata()) {
            self.primary.log(record);
            for logger in &self.secondary {
                logger.log(record);
            }
        }
    }

    fn flush(&self) {
        self.primary.flush();
        for logger in &self.secondary {
            logger.flush();
        }
    }
}
