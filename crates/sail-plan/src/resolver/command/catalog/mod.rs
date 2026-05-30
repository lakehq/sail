pub(super) mod database;
pub(super) mod table;
pub(super) mod view;

use crate::error::{PlanError, PlanResult};

fn validate_location_identifier(name: &str, kind: &str) -> PlanResult<()> {
    if name
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
    {
        Ok(())
    } else {
        Err(PlanError::invalid(format!(
            "invalid {kind} name for location derivation: {name:?}"
        )))
    }
}
