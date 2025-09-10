use std::collections::HashMap;

use crate::formats::binary::TableBinaryOptions;
use crate::options::{load_default_options, load_options, BinaryReadOptions};

fn apply_binary_read_options(
    from: BinaryReadOptions,
    to: &mut TableBinaryOptions,
) -> datafusion_common::Result<()> {
    if let Some(path_glob_filter) = from.path_glob_filter {
        if !path_glob_filter.is_empty() {
            to.path_glob_filter = Some(path_glob_filter);
        }
    }
    Ok(())
}

pub fn resolve_binary_read_options(
    options: Vec<HashMap<String, String>>,
) -> datafusion_common::Result<TableBinaryOptions> {
    let mut text_options = TableBinaryOptions::default();
    apply_binary_read_options(load_default_options()?, &mut text_options)?;
    for opt in options {
        apply_binary_read_options(load_options(opt)?, &mut text_options)?;
    }
    Ok(text_options)
}

#[cfg(test)]
mod tests {
    use crate::formats::binary::options::resolve_binary_read_options;
    use crate::options::build_options;

    #[test]
    fn test_resolve_binary_read_options() -> datafusion_common::Result<()> {
        let kv = build_options(&[]);
        let options = resolve_binary_read_options(vec![kv])?;
        assert_eq!(options.path_glob_filter, None);

        let kv = build_options(&[("path_glob_filter", "*.png")]);
        let options = resolve_binary_read_options(vec![kv])?;
        assert_eq!(options.path_glob_filter, Some("*.png".to_string()));

        let kv = build_options(&[("pathGlobFilter", "*.pdf")]);
        let options = resolve_binary_read_options(vec![kv])?;
        assert_eq!(options.path_glob_filter, Some("*.pdf".to_string()));

        Ok(())
    }
}
