pub(super) mod database;
pub(super) mod table;
pub(super) mod view;

use crate::error::{PlanError, PlanResult};

fn validate_location_identifier(name: &str, kind: &str) -> PlanResult<()> {
    if name.is_empty()
        || name == "."
        || name == ".."
        || name.contains('/')
        || name.contains('\\')
        || name.contains('\0')
    {
        Err(PlanError::invalid(format!(
            "invalid {kind} name for location derivation: {name:?}"
        )))
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::validate_location_identifier;

    #[test]
    fn validate_location_identifier_allows_filename_safe_special_characters() {
        assert!(validate_location_identifier("my@table", "table").is_ok());
    }

    #[test]
    fn validate_location_identifier_rejects_path_traversal() {
        assert!(validate_location_identifier("../escaped", "table")
            .is_err_and(|error| error.to_string().contains("invalid table name")));
    }
}
