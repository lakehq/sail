use datafusion::common::{DataFusionError, Result};

#[derive(Debug, Clone, Default)]
pub struct DuckLakeOptions {
    pub url: String,
    pub table: String,
    pub base_path: String,
    pub snapshot_id: Option<u64>,
    pub schema: Option<String>,
    pub case_sensitive: bool,
}

impl DuckLakeOptions {
    pub fn validate(&self) -> Result<()> {
        if self.url.is_empty() {
            return Err(DataFusionError::Plan(
                "Missing required option: url".to_string(),
            ));
        }
        Self::validate_url(&self.url)?;

        if self.table.is_empty() {
            return Err(DataFusionError::Plan(
                "Missing required option: table".to_string(),
            ));
        }
        Self::validate_table_name(&self.table)?;

        if self.base_path.is_empty() {
            return Err(DataFusionError::Plan(
                "Missing required option: base_path".to_string(),
            ));
        }
        Self::validate_base_path(&self.base_path)?;

        Ok(())
    }

    fn validate_url(url: &str) -> Result<()> {
        if !url.starts_with("sqlite://")
            && !url.starts_with("postgres://")
            && !url.starts_with("postgresql://")
        {
            return Err(DataFusionError::Plan(format!(
                "Invalid metadata URL scheme. Expected sqlite://, postgres://, or postgresql://, got: {}",
                url
            )));
        }
        Ok(())
    }

    fn validate_table_name(table: &str) -> Result<()> {
        if table.is_empty() {
            return Err(DataFusionError::Plan(
                "Table name cannot be empty".to_string(),
            ));
        }
        let parts: Vec<&str> = table.split('.').collect();
        if parts.len() > 2 {
            return Err(DataFusionError::Plan(format!(
                "Invalid table name format. Expected 'table' or 'schema.table', got: {}",
                table
            )));
        }
        Ok(())
    }

    fn validate_base_path(base_path: &str) -> Result<()> {
        if base_path.is_empty() {
            return Err(DataFusionError::Plan(
                "Base path cannot be empty".to_string(),
            ));
        }
        url::Url::parse(base_path).map_err(|e| {
            DataFusionError::Plan(format!("Invalid base_path URL: {}: {}", base_path, e))
        })?;
        Ok(())
    }
}
