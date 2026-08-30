use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use datafusion_common::{Result, plan_datafusion_err};
use serde::{Deserialize, Serialize};

use crate::extension::SessionExtension;
use crate::lakeprocedure::{LakeProcedureProvider, LakeProcedureResolution};
use crate::lakerelation::LakeRelationProvider;
use crate::lakesource::LakeTableFormat;

/// Stable identity of a trusted lake table-format plugin.
#[derive(Debug, Clone, Eq, PartialEq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct LakeFormatId(String);

impl LakeFormatId {
    pub fn try_new(name: impl AsRef<str>) -> Result<Self> {
        let name = name.as_ref().trim().to_ascii_lowercase();
        if name.is_empty() {
            return Err(plan_datafusion_err!("lake format identity cannot be empty"));
        }
        Ok(Self(name))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for LakeFormatId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Trusted implementation facets for one lake table format.
///
/// The engine constructs this value from built-ins during session creation.
/// User data-source registration cannot replace any of these facets.
#[derive(Clone)]
pub struct LakeFormatPlugin {
    id: LakeFormatId,
    table_format: Arc<dyn LakeTableFormat>,
    relation_provider: Option<Arc<dyn LakeRelationProvider>>,
    procedure_provider: Option<Arc<dyn LakeProcedureProvider>>,
}

impl fmt::Debug for LakeFormatPlugin {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LakeFormatPlugin")
            .field("id", &self.id)
            .field("has_relation_provider", &self.relation_provider.is_some())
            .field("has_procedure_provider", &self.procedure_provider.is_some())
            .finish()
    }
}

impl LakeFormatPlugin {
    pub fn try_new(table_format: Arc<dyn LakeTableFormat>) -> Result<Self> {
        let id = LakeFormatId::try_new(table_format.format_name())?;
        Ok(Self {
            id,
            table_format,
            relation_provider: None,
            procedure_provider: None,
        })
    }

    pub fn with_relation_provider(mut self, provider: Arc<dyn LakeRelationProvider>) -> Self {
        self.relation_provider = Some(provider);
        self
    }

    pub fn with_procedure_provider(mut self, provider: Arc<dyn LakeProcedureProvider>) -> Self {
        self.procedure_provider = Some(provider);
        self
    }

    pub fn id(&self) -> &LakeFormatId {
        &self.id
    }

    pub fn table_format(&self) -> Arc<dyn LakeTableFormat> {
        Arc::clone(&self.table_format)
    }

    pub fn relation_provider(&self) -> Option<Arc<dyn LakeRelationProvider>> {
        self.relation_provider.clone()
    }

    pub fn procedure_provider(&self) -> Option<Arc<dyn LakeProcedureProvider>> {
        self.procedure_provider.clone()
    }
}

/// Builder for the immutable per-session lake format registry.
#[derive(Default)]
pub struct LakeFormatRegistryBuilder {
    plugins: HashMap<LakeFormatId, Arc<LakeFormatPlugin>>,
}

impl LakeFormatRegistryBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&mut self, plugin: LakeFormatPlugin) -> Result<()> {
        let id = plugin.id.clone();
        if self.plugins.contains_key(&id) {
            return Err(plan_datafusion_err!(
                "lake format plugin is already registered: {id}"
            ));
        }
        self.plugins.insert(id, Arc::new(plugin));
        Ok(())
    }

    pub fn build(self) -> LakeFormatRegistry {
        LakeFormatRegistry {
            plugins: self.plugins,
        }
    }
}

/// Immutable registry of trusted lake table-format implementations.
pub struct LakeFormatRegistry {
    plugins: HashMap<LakeFormatId, Arc<LakeFormatPlugin>>,
}

impl LakeFormatRegistry {
    pub fn get(&self, id: &LakeFormatId) -> Result<Arc<LakeFormatPlugin>> {
        self.plugins
            .get(id)
            .cloned()
            .ok_or_else(|| plan_datafusion_err!("lake format plugin is not registered: {id}"))
    }

    pub fn get_by_name(&self, name: &str) -> Result<Arc<LakeFormatPlugin>> {
        self.get(&LakeFormatId::try_new(name)?)
    }

    pub fn get_if_registered(&self, name: &str) -> Result<Option<Arc<LakeFormatPlugin>>> {
        Ok(self.plugins.get(&LakeFormatId::try_new(name)?).cloned())
    }

    pub fn contains(&self, name: &str) -> bool {
        LakeFormatId::try_new(name)
            .ok()
            .is_some_and(|id| self.plugins.contains_key(&id))
    }

    /// Returns every trusted format's resolution for a catalog-scoped procedure name.
    pub fn resolve_procedures(
        &self,
        namespace: &[String],
        name: &str,
    ) -> Vec<(LakeFormatId, LakeProcedureResolution)> {
        let mut plugins = self.plugins.values().collect::<Vec<_>>();
        plugins.sort_by(|left, right| left.id.cmp(&right.id));
        plugins
            .into_iter()
            .filter_map(|plugin| {
                let provider = plugin.procedure_provider.as_ref()?;
                let resolution = provider.resolve_procedure(namespace, name);
                (!matches!(resolution, LakeProcedureResolution::Unrecognized))
                    .then(|| (plugin.id.clone(), resolution))
            })
            .collect()
    }
}

impl SessionExtension for LakeFormatRegistry {
    fn name() -> &'static str {
        "LakeFormatRegistry"
    }
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::catalog::Session;

    use super::*;
    use crate::datasource::SourceInfo;
    use crate::lakesource::LakeSourceMetadata;

    struct TestFormat;

    #[async_trait]
    impl LakeTableFormat for TestFormat {
        fn format_name(&self) -> &str {
            "test"
        }

        async fn infer_metadata(
            &self,
            _ctx: &dyn Session,
            _info: SourceInfo,
        ) -> Result<LakeSourceMetadata> {
            Ok(LakeSourceMetadata {
                schema: Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
                properties: vec![],
            })
        }
    }

    #[test]
    fn registry_is_typed_and_rejects_duplicate_plugins() -> Result<()> {
        let mut builder = LakeFormatRegistryBuilder::new();
        builder.register(LakeFormatPlugin::try_new(Arc::new(TestFormat))?)?;
        assert!(
            builder
                .register(LakeFormatPlugin::try_new(Arc::new(TestFormat))?)
                .is_err()
        );
        let registry = builder.build();
        assert_eq!(registry.get_by_name("TEST")?.id().as_str(), "test");
        assert!(registry.get_by_name("missing").is_err());
        Ok(())
    }
}
