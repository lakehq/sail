// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Table location handling.

use datafusion_common::{Result, plan_datafusion_err, plan_err};
use sail_common_datafusion::datasource::{OptionLayer, find_path_in_options};

/// Resolves the single location a Lance dataset lives at.
pub fn resolve(paths: &[String], options: &[OptionLayer]) -> Result<String> {
    let path = match paths {
        [] => find_path_in_options(options)
            .ok_or_else(|| plan_datafusion_err!("missing path for the Lance table format"))?,
        [path] => path.clone(),
        paths => {
            return plan_err!(
                "the Lance table format reads one dataset at a time, got {} paths: [{}]",
                paths.len(),
                paths.join(", ")
            );
        }
    };
    normalize(&path)
}

/// Turns a table location into something Lance can open.
///
/// Lance resolves object store URIs itself, so only a local path has to be made
/// absolute, which is what the working directory of a Sail worker would
/// otherwise decide.
pub fn normalize(path: &str) -> Result<String> {
    if has_object_store_scheme(path) {
        return Ok(path.to_string());
    }
    let absolute = std::path::absolute(path).map_err(|e| {
        plan_datafusion_err!("invalid path for the Lance table format '{path}': {e}")
    })?;
    absolute.to_str().map(str::to_string).ok_or_else(|| {
        plan_datafusion_err!("path for the Lance table format is not valid UTF-8: '{path}'")
    })
}

fn has_object_store_scheme(path: &str) -> bool {
    match url::Url::parse(path) {
        // A single letter scheme is a Windows drive letter, not a URI scheme.
        Ok(url) => url.scheme().len() > 1,
        Err(_) => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error_message;

    #[test]
    fn object_store_uris_are_left_alone() -> Result<()> {
        assert_eq!(
            normalize("s3://bucket/data.lance")?,
            "s3://bucket/data.lance"
        );
        assert_eq!(normalize("file:///data/x.lance")?, "file:///data/x.lance");
        Ok(())
    }

    #[test]
    fn local_paths_are_made_absolute() -> Result<()> {
        assert!(normalize("relative/x.lance")?.starts_with('/'));
        assert_eq!(normalize("/data/x.lance")?, "/data/x.lance");
        Ok(())
    }

    #[test]
    fn a_single_location_is_required() {
        let paths = vec!["a.lance".to_string(), "b.lance".to_string()];
        let error = error_message(resolve(&paths, &[]));
        assert!(error.contains("one dataset at a time"), "{error}");
        let missing = error_message(resolve(&[], &[]));
        assert!(missing.contains("missing path"), "{missing}");
    }

    #[test]
    fn the_location_falls_back_to_the_options() -> Result<()> {
        let options = vec![OptionLayer::OptionList {
            items: vec![("path".to_string(), "/data/x.lance".to_string())],
        }];
        assert_eq!(resolve(&[], &options)?, "/data/x.lance");
        Ok(())
    }
}
