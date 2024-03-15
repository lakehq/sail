use std::collections::HashMap;
use std::hash::Hash;
use std::sync::{Arc, Mutex, MutexGuard};

use datafusion::prelude::SessionContext;

use crate::error::SparkResult;
use crate::executor::Executor;

pub(crate) struct Session {
    user_id: Option<String>,
    session_id: String,
    context: SessionContext,
    state: Mutex<SessionState>,
}

pub(crate) struct SessionState {
    config: HashMap<String, String>,
    executors: HashMap<String, Executor>,
}

impl Session {
    pub(crate) fn new(user_id: Option<String>, session_id: String) -> Self {
        Self {
            user_id,
            session_id,
            context: SessionContext::new(),
            state: Mutex::new(SessionState {
                config: HashMap::new(),
                executors: HashMap::new(),
            }),
        }
    }

    pub(crate) fn session_id(&self) -> &str {
        &self.session_id
    }

    pub(crate) fn user_id(&self) -> Option<&str> {
        self.user_id.as_deref()
    }

    pub(crate) fn context(&self) -> &SessionContext {
        &self.context
    }

    pub(crate) fn lock(&self) -> SparkResult<MutexGuard<SessionState>> {
        Ok(self.state.lock()?)
    }
}

impl SessionState {
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

    pub(crate) fn add_executor(&mut self, executor: Executor) {
        let id = executor.metadata.operation_id.clone();
        self.executors.insert(id, executor);
    }

    pub(crate) fn remove_executor(&mut self, id: &str) -> Option<Executor> {
        self.executors.remove(id)
    }

    pub(crate) fn remove_all_executors(&mut self) -> Vec<Executor> {
        let mut out = Vec::new();
        for (_, executor) in self.executors.drain() {
            out.push(executor);
        }
        out
    }

    pub(crate) fn remove_executors_by_tag(&mut self, tag: &str) -> Vec<Executor> {
        let tag = tag.to_string();
        let mut ids = Vec::new();
        let mut removed = Vec::new();
        for (key, executor) in &self.executors {
            if executor.metadata.tags.contains(&tag) {
                ids.push(key.clone());
            }
        }
        for key in ids {
            if let Some(executor) = self.executors.remove(&key) {
                removed.push(executor);
            }
        }
        removed
    }
}

#[derive(Eq, PartialEq, Hash, Clone)]
pub(crate) struct SessionKey {
    pub user_id: Option<String>,
    pub session_id: String,
}

type SessionStore = HashMap<SessionKey, Arc<Session>>;

pub(crate) struct SessionManager {
    sessions: Mutex<SessionStore>,
}

impl SessionManager {
    pub(crate) fn new() -> Self {
        Self {
            sessions: Mutex::new(SessionStore::new()),
        }
    }

    pub(crate) fn get_session(&self, key: SessionKey) -> SparkResult<Arc<Session>> {
        let mut sessions = self.sessions.lock()?;
        let session = sessions.entry(key).or_insert_with_key(|k| {
            Arc::new(Session::new(k.user_id.clone(), k.session_id.clone()))
        });
        Ok(session.clone())
    }

    pub(crate) fn delete_session(&self, key: &SessionKey) -> SparkResult<()> {
        self.sessions.lock()?.remove(key);
        Ok(())
    }
}
