pub mod catalog;
pub mod error;
pub mod formats;
pub mod optimizer;
pub mod planner;
pub mod runtime;
pub mod session_factory;
pub mod session_manager;

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::prelude::SessionContext;
    use sail_common_datafusion::system::catalog::SystemCatalog;
    use sail_plan::config::PlanConfig;
    use sail_plan::resolver::PlanResolver;
    use sail_sql_analyzer::data_type::from_ast_data_type;
    use sail_sql_analyzer::parser::parse_data_type;

    // This test is defined in this crate so that `sail-catalog-system` or `sail-common-datafusion`
    // does not need to depend on `sail-plan` or `sail-sql-analyzer`.
    #[test]
    fn test_system_table_schema_validity() -> Result<(), Box<dyn std::error::Error>> {
        let session = SessionContext::new();
        let resolver = PlanResolver::new(&session, Arc::new(PlanConfig::default()));
        for db in SystemCatalog::databases() {
            for t in db.tables() {
                let columns = t.columns();
                for col in columns.iter() {
                    // In the table definition YAML file, we have a SQL string for the data type
                    // of each column (for documentation purposes). Here we ensure that the SQL
                    // data type matches the Arrow data type used for table row struct serde.
                    let data_type = parse_data_type(col.sql_type).and_then(from_ast_data_type)?;
                    let data_type = resolver.resolve_data_type_for_plan(&data_type)?;
                    assert_eq!(data_type, col.arrow_type);
                }
            }
        }
        Ok(())
    }
}
