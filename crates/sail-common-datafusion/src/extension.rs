// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use datafusion::catalog::Session;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::execution::{SessionState, TaskContext};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_common::{Result, internal_datafusion_err};

pub trait SessionExtension: Send + Sync + 'static {
    fn name() -> &'static str;
}

pub trait SessionExtensionAccessor {
    fn extension<T: SessionExtension>(&self) -> Result<Arc<T>>;
    fn session_config(&self) -> SessionConfig;
    fn runtime_env(&self) -> Arc<RuntimeEnv>;
}

impl SessionExtensionAccessor for SessionContext {
    fn extension<T: SessionExtension>(&self) -> Result<Arc<T>> {
        self.state_ref()
            .read()
            .config()
            .get_extension::<T>()
            .ok_or_else(|| internal_datafusion_err!("session extension not found: {}", T::name()))
    }

    fn session_config(&self) -> SessionConfig {
        self.state_ref().read().config().clone()
    }

    fn runtime_env(&self) -> Arc<RuntimeEnv> {
        self.state_ref().read().runtime_env().clone()
    }
}

impl SessionExtensionAccessor for SessionState {
    fn extension<T: SessionExtension>(&self) -> Result<Arc<T>> {
        self.config()
            .get_extension::<T>()
            .ok_or_else(|| internal_datafusion_err!("session extension not found: {}", T::name()))
    }

    fn session_config(&self) -> SessionConfig {
        self.config().clone()
    }

    fn runtime_env(&self) -> Arc<RuntimeEnv> {
        self.runtime_env().clone()
    }
}

impl SessionExtensionAccessor for &dyn Session {
    fn extension<T: SessionExtension>(&self) -> Result<Arc<T>> {
        self.config()
            .get_extension::<T>()
            .ok_or_else(|| internal_datafusion_err!("session extension not found: {}", T::name()))
    }

    fn session_config(&self) -> SessionConfig {
        self.config().clone()
    }

    fn runtime_env(&self) -> Arc<RuntimeEnv> {
        Session::runtime_env(*self).clone()
    }
}

impl SessionExtensionAccessor for TaskContext {
    fn extension<T: SessionExtension>(&self) -> Result<Arc<T>> {
        self.session_config()
            .get_extension::<T>()
            .ok_or_else(|| internal_datafusion_err!("session extension not found: {}", T::name()))
    }

    fn session_config(&self) -> SessionConfig {
        self.session_config().clone()
    }

    fn runtime_env(&self) -> Arc<RuntimeEnv> {
        self.runtime_env().clone()
    }
}

/// Returns a task context that has the extension in its session configuration
/// and that is the given task context otherwise.
pub fn task_context_with_extension<T: Send + Sync + 'static>(
    context: &TaskContext,
    extension: Arc<T>,
) -> Arc<TaskContext> {
    Arc::new(TaskContext::new(
        context.task_id(),
        context.session_id(),
        context.session_config().clone().with_extension(extension),
        context.scalar_functions().clone(),
        context.higher_order_functions().clone(),
        context.aggregate_functions().clone(),
        context.window_functions().clone(),
        context.runtime_env(),
    ))
}

/// The number of the attempt of the task that a plan is executed for, starting at 0.
///
/// A job runner that can attempt a task again puts this in the session configuration
/// of the task context of each attempt. A plan that is executed in another way is
/// attempted once and does not have it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TaskAttempt(pub usize);

impl TaskAttempt {
    /// Returns the number of the attempt of the task that the task context is for.
    pub fn of(context: &TaskContext) -> Option<usize> {
        let attempt = context.session_config().get_extension::<Self>()?;
        Some(attempt.0)
    }
}
