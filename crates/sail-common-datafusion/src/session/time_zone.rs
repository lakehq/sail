use datafusion_common::config::{ConfigExtension, ConfigOptions};
use datafusion_common::extensions_options;

extensions_options! {
    /// The Spark session time zone for the optimizer rules, which see only the DataFusion
    /// session options.
    pub struct SessionTimeZoneOptions {
        /// The session time zone (`spark.sql.session.timeZone`), spelled as Spark accepts it.
        pub session_time_zone: Option<String>, default = None
    }
}

impl ConfigExtension for SessionTimeZoneOptions {
    const PREFIX: &'static str = "sail";
}

impl SessionTimeZoneOptions {
    /// Sets the session time zone for the optimizer rules, and as `execution.time_zone`, which
    /// DataFusion and Sail's readers and writers use.
    pub fn set_session_time_zone(options: &mut ConfigOptions, time_zone: &str) {
        options.execution.time_zone = Some(time_zone.to_string());
        match options.extensions.get_mut::<Self>() {
            Some(extension) => extension.session_time_zone = Some(time_zone.to_string()),
            None => options.extensions.insert(Self {
                session_time_zone: Some(time_zone.to_string()),
            }),
        }
    }

    /// The session time zone, which `SET timezone` does not change, unlike `execution.time_zone`.
    pub fn session_time_zone(options: &ConfigOptions) -> Option<&str> {
        options
            .extensions
            .get::<Self>()?
            .session_time_zone
            .as_deref()
    }
}
