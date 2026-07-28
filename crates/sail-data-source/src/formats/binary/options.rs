use datafusion::catalog::Session;
use sail_common_datafusion::datasource::OptionLayer;

use crate::error::DataSourceResult;
use crate::options::r#gen::{BinaryReadOptions, BinaryReadPartialOptions};
use crate::options::{BuildPartialOptions, PartialOptions, ResolveOptions};

impl ResolveOptions for BinaryReadOptions {
    fn resolve(_ctx: &dyn Session, options: Vec<OptionLayer>) -> DataSourceResult<Self> {
        let mut partial = BinaryReadPartialOptions::initialize();
        for layer in options {
            partial.merge(layer.build_partial_options()?);
        }
        partial.finalize()
    }
}

#[cfg(test)]
mod tests {
    use datafusion::prelude::SessionContext;

    use crate::options::r#gen::BinaryReadOptions;
    use crate::options::{ResolveOptions, option_list};

    #[test]
    fn test_resolve_binary_read_options() -> datafusion_common::Result<()> {
        let ctx = SessionContext::default();
        let state = ctx.state();

        let kv = option_list(&[]);
        let options = BinaryReadOptions::resolve(&state, vec![kv])
            .map_err(datafusion_common::DataFusionError::from)?;
        assert_eq!(options.path_glob_filter, None);

        let kv = option_list(&[("path_glob_filter", "*.png")]);
        let options = BinaryReadOptions::resolve(&state, vec![kv])
            .map_err(datafusion_common::DataFusionError::from)?;
        assert_eq!(options.path_glob_filter, Some("*.png".to_string()));

        let kv = option_list(&[("pathGlobFilter", "*.pdf")]);
        let options = BinaryReadOptions::resolve(&state, vec![kv])
            .map_err(datafusion_common::DataFusionError::from)?;
        assert_eq!(options.path_glob_filter, Some("*.pdf".to_string()));

        Ok(())
    }
}
