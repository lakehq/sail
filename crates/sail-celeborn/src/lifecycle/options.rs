use crate::master::MasterClientOptions;

/// Configuration owned by a lifecycle manager actor.
#[derive(Debug, Clone)]
pub struct LifecycleManagerOptions {
    pub application_id: String,
    pub master: MasterClientOptions,
    pub hostname: String,
    pub tenant_id: String,
    pub user_name: String,
}

impl LifecycleManagerOptions {
    pub fn new(application_id: impl Into<String>, master: MasterClientOptions) -> Self {
        let hostname = std::env::var("HOSTNAME").unwrap_or_else(|_| "localhost".to_string());
        Self {
            application_id: application_id.into(),
            master,
            hostname,
            // Match Celeborn's DefaultIdentityProvider defaults.
            tenant_id: "default".to_string(),
            user_name: "default".to_string(),
        }
    }
}
