use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};

use sail_common_datafusion::extension::SessionExtension;

use crate::error::{SparkError, SparkResult};

pub(super) struct SparkSessionProcessAdmission {
    max_sessions: usize,
    active_sessions: AtomicUsize,
}

impl SparkSessionProcessAdmission {
    fn new(max_sessions: usize) -> Self {
        Self {
            max_sessions,
            active_sessions: AtomicUsize::new(0),
        }
    }

    pub(super) fn reserve(self: &Arc<Self>) -> Result<SparkSessionProcessReservation, String> {
        let mut active_sessions = self.active_sessions.load(Ordering::Acquire);
        loop {
            if active_sessions >= self.max_sessions {
                return Err(format!(
                    "cannot create Spark session: process-wide maximum number of sessions ({}) reached",
                    self.max_sessions
                ));
            }
            match self.active_sessions.compare_exchange_weak(
                active_sessions,
                active_sessions + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    return Ok(SparkSessionProcessReservation {
                        admission: Arc::clone(self),
                    });
                }
                Err(observed) => active_sessions = observed,
            }
        }
    }

    fn release(&self) {
        let result = self.active_sessions.fetch_update(
            Ordering::AcqRel,
            Ordering::Acquire,
            |active_sessions| active_sessions.checked_sub(1),
        );
        debug_assert!(result.is_ok(), "Spark session admission underflow");
    }

    fn configured_limit(&self) -> usize {
        self.max_sessions
    }
}

pub(super) struct SparkSessionProcessReservation {
    admission: Arc<SparkSessionProcessAdmission>,
}

impl SessionExtension for SparkSessionProcessReservation {
    fn name() -> &'static str {
        "Spark process session admission"
    }
}

impl Drop for SparkSessionProcessReservation {
    fn drop(&mut self) {
        self.admission.release();
    }
}

pub(super) fn resolve_session_process_admission(
    max_sessions: usize,
    admission_slot: &OnceLock<Arc<SparkSessionProcessAdmission>>,
) -> SparkResult<Arc<SparkSessionProcessAdmission>> {
    let admission =
        admission_slot.get_or_init(|| Arc::new(SparkSessionProcessAdmission::new(max_sessions)));
    let configured_limit = admission.configured_limit();
    if configured_limit != max_sessions {
        return Err(SparkError::invalid(format!(
            "spark.session_max_count conflicts with the process-wide configuration: configured spark.session_max_count={configured_limit}; requested spark.session_max_count={max_sessions}"
        )));
    }
    Ok(Arc::clone(admission))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_session_limit_reuses_process_admission() -> SparkResult<()> {
        let admission_slot = OnceLock::new();
        let first = resolve_session_process_admission(2, &admission_slot)?;
        let second = resolve_session_process_admission(2, &admission_slot)?;

        assert!(Arc::ptr_eq(&first, &second));
        Ok(())
    }

    #[test]
    fn conflicting_session_limit_is_rejected() -> SparkResult<()> {
        let admission_slot = OnceLock::new();
        let configured = resolve_session_process_admission(1, &admission_slot)?;
        let error = resolve_session_process_admission(2, &admission_slot)
            .err()
            .ok_or_else(|| SparkError::internal("conflicting session limit was accepted"))?;

        assert!(
            error.to_string().contains(
                "configured spark.session_max_count=1; requested spark.session_max_count=2"
            )
        );
        let configured_again = resolve_session_process_admission(1, &admission_slot)?;
        assert!(Arc::ptr_eq(&configured, &configured_again));
        Ok(())
    }
}
