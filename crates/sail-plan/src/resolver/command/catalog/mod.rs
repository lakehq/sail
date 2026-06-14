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
        || name
            .chars()
            .any(|c| !c.is_alphanumeric() && c != '-' && c != '_')
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
    fn validate_location_identifier_allows_safe_identifier_characters() {
        assert!(validate_location_identifier("my-table", "table").is_ok());
        assert!(validate_location_identifier("my_table", "table").is_ok());
        assert!(validate_location_identifier("myTable123", "table").is_ok());
    }

    #[test]
    fn validate_location_identifier_rejects_special_characters() {
        assert!(validate_location_identifier("my@table", "table").is_err());
        assert!(validate_location_identifier("my table", "table").is_err());
        assert!(validate_location_identifier("my.table", "table").is_err());
    }

    #[test]
    fn validate_location_identifier_rejects_path_traversal() {
        assert!(validate_location_identifier("../escaped", "table")
            .is_err_and(|error| error.to_string().contains("invalid table name")));
    }
}
