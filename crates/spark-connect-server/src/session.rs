use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};
use tonic::Status;

pub(crate) struct Session {
    id: String,
    config: HashMap<String, String>,
}

impl Session {
    pub(crate) fn get_config(&self, key: &str) -> Option<&String> {
        self.config.get(key)
    }

    pub(crate) fn set_config(&mut self, key: &str, value: &str) {
        self.config.insert(key.to_string(), value.to_string());
    }

    pub(crate) fn unset_config(&mut self, key: &str) {
        self.config.remove(key);
    }

    pub(crate) fn iter_config<'a>(
        &'a self,
        prefix: &'a Option<String>,
    ) -> Box<dyn Iterator<Item = (&String, &String)> + 'a> {
        if let Some(prefix) = prefix {
            Box::new(
                self.config
                    .iter()
                    .filter(move |(k, _)| k.starts_with(prefix)),
            )
        } else {
            Box::new(self.config.iter())
        }
    }
}

type SessionStore = HashMap<String, Arc<Mutex<Session>>>;

pub(crate) struct SessionManager {
    sessions: Mutex<SessionStore>,
}

impl SessionManager {
    pub(crate) fn new() -> Self {
        Self {
            sessions: Mutex::new(SessionStore::new()),
        }
    }

    fn lock_sessions(&self) -> Result<MutexGuard<SessionStore>, Status> {
        self.sessions
            .lock()
            .or_else(|_| Err(Status::internal("failed to lock sessions")))
    }

    pub(crate) fn get_session(&self, id: &str) -> Result<Arc<Mutex<Session>>, Status> {
        let mut sessions = self.lock_sessions()?;
        let session = sessions.entry(id.to_string()).or_insert_with_key(|k| {
            Arc::new(Mutex::new(Session {
                id: k.clone(),
                config: HashMap::new(),
            }))
        });
        Ok(session.clone())
    }

    pub(crate) fn delete_session(&self, id: &str) -> Result<(), Status> {
        let mut sessions = self.lock_sessions()?;
        sessions.remove(id);
        Ok(())
    }
}
