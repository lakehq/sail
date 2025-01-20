use datafusion_common::{exec_err, DFSchema, DFSchemaRef, Result, SchemaReference};
use datafusion_expr::{CreateCatalogSchema, DdlStatement, DropCatalogSchema, LogicalPlan};
use sail_common::unwrap_or;
use serde::{Deserialize, Serialize};

use crate::catalog::utils::match_pattern;
use crate::catalog::CatalogManager;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct DatabaseMetadata {
    pub(crate) name: String,
    pub(crate) catalog: Option<String>,
    pub(crate) description: Option<String>,
    pub(crate) location_uri: Option<String>,
}

impl DatabaseMetadata {
    fn new(catalog_name: &str, database_name: &str) -> Self {
        Self {
            name: database_name.to_string(),
            catalog: Some(catalog_name.to_string()),
            description: None,  // TODO: Add actual description if available
            location_uri: None, // TODO: Add actual location URI if available
        }
    }
}

impl CatalogManager<'_> {
    pub(crate) fn default_database(&self) -> Result<String> {
        let state = self.ctx.state_ref();
        let state = state.read();
        Ok(state.config().options().catalog.default_schema.clone())
    }

    pub(crate) fn set_default_database(&self, database_name: String) -> Result<()> {
        // TODO: Race condition if catalog or database is deleted.
        let database = SchemaReference::Bare {
            schema: database_name.clone().into(),
        };
        let database = self.get_database(database)?;
        if database.is_none() {
            return exec_err!("Database not found: {database_name}");
        }
        let state = self.ctx.state_ref();
        let mut state = state.write();
        state.config_mut().options_mut().catalog.default_schema = database_name;
        Ok(())
    }

    pub(crate) fn get_database(
        &self,
        database: SchemaReference,
    ) -> Result<Option<DatabaseMetadata>> {
        let (catalog_name, database_name) = self.resolve_database_reference(Some(database))?;
        let catalog_provider = unwrap_or!(self.ctx.catalog(catalog_name.as_ref()), return Ok(None));
        let _ = unwrap_or!(
            catalog_provider.schema(database_name.as_ref()),
            return Ok(None)
        );
        Ok(Some(DatabaseMetadata::new(
            catalog_name.as_ref(),
            database_name.as_ref(),
        )))
    }

    pub(crate) fn list_databases(
        &self,
        catalog: Option<String>,
        database_pattern: Option<&str>,
    ) -> Result<Vec<DatabaseMetadata>> {
        let catalog_name = self.resolve_catalog_reference(catalog)?;
        let catalog_provider = unwrap_or!(
            self.ctx.catalog(catalog_name.as_ref()),
            return Ok(Vec::new())
        );
        Ok(catalog_provider
            .schema_names()
            .iter()
            .filter(|name| match_pattern(name.as_str(), database_pattern))
            .map(|name| DatabaseMetadata::new(catalog_name.as_ref(), name.as_str()))
            .collect())
    }

    pub(crate) async fn create_database(
        &self,
        database: SchemaReference,
        if_not_exists: bool,
        comment: Option<String>,
        location: Option<String>,
        properties: Vec<(String, String)>,
    ) -> Result<()> {
        let schema_name = match database {
            SchemaReference::Bare { schema } => schema.to_string(),
            SchemaReference::Full { .. } => {
                return exec_err!("catalog name is not supported in CREATE DATABASE")
            }
        };
        if comment.is_some() || location.is_some() || !properties.is_empty() {
            return exec_err!(
                "comment, location, or properties are not supported in CREATE DATABASE"
            );
        }
        let ddl = LogicalPlan::Ddl(DdlStatement::CreateCatalogSchema(CreateCatalogSchema {
            schema_name,
            if_not_exists,
            schema: DFSchemaRef::new(DFSchema::empty()),
        }));
        self.ctx.execute_logical_plan(ddl).await?;
        Ok(())
    }

    pub(crate) async fn drop_database(
        &self,
        database: SchemaReference,
        if_exists: bool,
        cascade: bool,
    ) -> Result<()> {
        let ddl = LogicalPlan::Ddl(DdlStatement::DropCatalogSchema(DropCatalogSchema {
            name: database,
            if_exists,
            cascade,
            schema: DFSchemaRef::new(DFSchema::empty()),
        }));
        self.ctx.execute_logical_plan(ddl).await?;
        Ok(())
    }
}
