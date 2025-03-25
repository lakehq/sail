use std::collections::HashMap;

use crate::error::{PlanError, PlanResult};
use crate::resolver::PlanResolver;

impl PlanResolver<'_> {
    fn resolve_data_reader_option(
        format: &str,
        key: &str,
        value: &str,
    ) -> PlanResult<(String, String)> {
        let format = format.to_lowercase();
        let key = key.to_lowercase();
        let (key, value) = match (format.as_str(), key.as_str()) {
            ("csv", "header") => ("format.has_header", value),
            ("csv", "sep") => ("format.delimiter", value),
            ("csv", "linesep") => ("format.terminator", value),
            _ => return Err(PlanError::unsupported(format!("data reader option: {key}"))),
        };
        Ok((key.to_string(), value.to_string()))
    }

    pub(crate) fn resolve_data_reader_options(
        format: &str,
        options: Vec<(String, String)>,
    ) -> PlanResult<HashMap<String, String>> {
        let mut output = HashMap::new();
        for (key, value) in options {
            let (k, v) = PlanResolver::resolve_data_reader_option(format, &key, &value)?;
            if output.insert(k, v).is_some() {
                return Err(PlanError::invalid(format!(
                    "duplicated data reader option key: {key}"
                )));
            }
        }
        Ok(output)
    }

    fn resolve_data_writer_option(
        format: &str,
        key: &str,
        value: &str,
    ) -> PlanResult<(String, String)> {
        let format = format.to_lowercase();
        let key = key.to_lowercase();
        let (key, value) = match (format.as_str(), key.as_str()) {
            ("csv", "header") => ("format.has_header", value),
            ("csv", "sep") => ("format.delimiter", value),
            ("csv", "linesep") => return Err(PlanError::todo("CSV writer line seperator")),
            _ => return Err(PlanError::unsupported(format!("data writer option: {key}"))),
        };
        Ok((key.to_string(), value.to_string()))
    }

    pub(crate) fn resolve_data_writer_options(
        format: &str,
        options: Vec<(String, String)>,
    ) -> PlanResult<HashMap<String, String>> {
        let mut output = HashMap::new();
        for (key, value) in options {
            let (k, v) = PlanResolver::resolve_data_writer_option(format, &key, &value)?;
            if output.insert(k, v).is_some() {
                return Err(PlanError::invalid(format!(
                    "duplicated data writer option key: {key}"
                )));
            }
        }
        Ok(output)
    }
}
