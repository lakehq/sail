use crate::error::CatalogResult;
use crate::manager::CatalogManager;

impl CatalogManager {
    pub fn cache_table<T: AsRef<str>>(&self, table: &[T]) -> CatalogResult<()> {
        let mut state = self.state()?;
        let key = state.resolve_cached_table_key(table)?;
        state.cached_tables.insert(key);
        Ok(())
    }

    pub fn uncache_table<T: AsRef<str>>(&self, table: &[T]) -> CatalogResult<()> {
        let mut state = self.state()?;
        let key = state.resolve_cached_table_key(table)?;
        state.cached_tables.remove(&key);
        Ok(())
    }

    pub fn clear_cache(&self) -> CatalogResult<()> {
        let mut state = self.state()?;
        state.cached_tables.clear();
        Ok(())
    }

    pub fn is_table_cached<T: AsRef<str>>(&self, table: &[T]) -> CatalogResult<bool> {
        let state = self.state()?;
        let key = state.resolve_cached_table_key(table)?;
        Ok(state.cached_tables.contains(&key))
    }
}
