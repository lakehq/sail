use sail_common_datafusion::datasource::OptionLayer;

use crate::formats::binary::TableBinaryOptions;
use crate::options::gen::{BinaryReadOptions, BinaryReadPartialOptions};
use crate::options::{BuildPartialOptions, PartialOptions};

fn apply_binary_read_options(
    from: BinaryReadOptions,
    to: &mut TableBinaryOptions,
) -> datafusion_common::Result<()> {
    let BinaryReadOptions { path_glob_filter } = from;
    if let Some(path_glob_filter) = path_glob_filter {
        to.path_glob_filter = Some(path_glob_filter);
    }
    Ok(())
}

pub fn resolve_binary_read_options(
    options: Vec<OptionLayer>,
) -> datafusion_common::Result<TableBinaryOptions> {
    let mut partial = BinaryReadPartialOptions::initialize();
    for layer in options {
        partial.merge(layer.build_partial_options()?);
    }
    let opts = partial.finalize()?;
    let mut binary_options = TableBinaryOptions::default();
    apply_binary_read_options(opts, &mut binary_options)?;
    Ok(binary_options)
}

#[cfg(test)]
mod tests {
    use crate::formats::binary::options::resolve_binary_read_options;
    use crate::options::build_option_layer;

    #[test]
    fn test_resolve_binary_read_options() -> datafusion_common::Result<()> {
        let kv = build_option_layer(&[]);
        let options = resolve_binary_read_options(vec![kv])?;
        assert_eq!(options.path_glob_filter, None);

        let kv = build_option_layer(&[("path_glob_filter", "*.png")]);
        let options = resolve_binary_read_options(vec![kv])?;
        assert_eq!(options.path_glob_filter, Some("*.png".to_string()));

        let kv = build_option_layer(&[("pathGlobFilter", "*.pdf")]);
        let options = resolve_binary_read_options(vec![kv])?;
        assert_eq!(options.path_glob_filter, Some("*.pdf".to_string()));

        Ok(())
    }
}
