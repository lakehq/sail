use std::sync::Arc;

use datafusion_common::{Result, internal_datafusion_err};

use crate::extension::SessionExtension;

/// Provides effective session configuration without exposing the owning session.
pub trait SessionConfigProvider: Send + Sync + 'static {
    /// Reads the selected keys from one coherent configuration state.
    ///
    /// Returns owned values in input order, with exactly one entry per key.
    /// Keys need not belong to a predefined registry. Missing keys are `None`;
    /// empty strings remain `Some("")`. Defaults and aliases are resolved by
    /// the provider. Read failures must return errors, not missing values.
    /// Separate calls need not observe the same state.
    fn get_options(&self, keys: &[&str]) -> Result<Vec<Option<String>>>;
}

/// A session extension exposing a shared, live configuration provider.
/// Consumers decide when to capture values and how to interpret them.
pub struct SessionConfigService {
    provider: Arc<dyn SessionConfigProvider>,
}

impl SessionConfigService {
    pub fn new(provider: Arc<dyn SessionConfigProvider>) -> Self {
        Self { provider }
    }

    pub fn get_options(&self, keys: &[&str]) -> Result<Vec<Option<String>>> {
        let values = self.provider.get_options(keys)?;
        if values.len() != keys.len() {
            return Err(internal_datafusion_err!(
                "session configuration provider returned {} values for {} keys",
                values.len(),
                keys.len()
            ));
        }
        Ok(values)
    }

    pub fn get_option(&self, key: &str) -> Result<Option<String>> {
        self.get_options(&[key])?
            .pop()
            .ok_or_else(|| internal_datafusion_err!("missing session configuration result"))
    }
}

impl SessionExtension for SessionConfigService {
    fn name() -> &'static str {
        "SessionConfigService"
    }
}

#[cfg(test)]
mod tests {
    use datafusion_common::DataFusionError;

    use super::*;

    struct FixedProvider(Vec<Option<String>>);

    impl SessionConfigProvider for FixedProvider {
        fn get_options(&self, _keys: &[&str]) -> Result<Vec<Option<String>>> {
            Ok(self.0.clone())
        }
    }

    #[test]
    fn rejects_invalid_provider_cardinality() {
        for values in [vec![], vec![None, None]] {
            let service = SessionConfigService::new(Arc::new(FixedProvider(values)));
            assert!(matches!(
                service.get_option("key"),
                Err(DataFusionError::Internal(_))
            ));
        }
        let service = SessionConfigService::new(Arc::new(FixedProvider(vec![None])));
        assert!(matches!(
            service.get_options(&[]),
            Err(DataFusionError::Internal(_))
        ));
        assert!(matches!(
            service.get_options(&["a", "b"]),
            Err(DataFusionError::Internal(_))
        ));
    }

    #[test]
    fn accepts_empty_requests_and_missing_values() -> Result<()> {
        let service = SessionConfigService::new(Arc::new(FixedProvider(vec![])));
        assert!(service.get_options(&[])?.is_empty());
        let service = SessionConfigService::new(Arc::new(FixedProvider(vec![None])));
        assert_eq!(service.get_option("missing")?, None);
        Ok(())
    }
}
