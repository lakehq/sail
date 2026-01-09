use std::any::TypeId;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::{Arc, RwLock};

use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::{internal_datafusion_err, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FormatTag {
    Delta,
    Iceberg,
    Hudi,
    DuckLake,
    Custom(&'static str),
}

impl fmt::Display for FormatTag {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FormatTag::Delta => write!(f, "Delta"),
            FormatTag::Iceberg => write!(f, "Iceberg"),
            FormatTag::Hudi => write!(f, "Hudi"),
            FormatTag::DuckLake => write!(f, "DuckLake"),
            FormatTag::Custom(name) => write!(f, "{}", name),
        }
    }
}

lazy_static::lazy_static! {
    static ref FORMAT_TAG_REGISTRY: RwLock<HashMap<TypeId, FormatTag>> =
        RwLock::new(HashMap::new());
}

pub fn register_format_type<T: ExecutionPlan + 'static>(tag: FormatTag) -> Result<()> {
    let type_id = TypeId::of::<T>();
    let mut registry = FORMAT_TAG_REGISTRY
        .write()
        .map_err(|_| internal_datafusion_err!("format tag registry lock poisoned"))?;
    registry.insert(type_id, tag);
    Ok(())
}

pub fn get_format_tag(plan: &dyn ExecutionPlan) -> Result<Option<FormatTag>> {
    let type_id = (*plan.as_any()).type_id();
    let registry = FORMAT_TAG_REGISTRY
        .read()
        .map_err(|_| internal_datafusion_err!("format tag registry lock poisoned"))?;
    Ok(registry.get(&type_id).copied())
}

pub fn is_format_tag(plan: &dyn ExecutionPlan, tag: FormatTag) -> Result<bool> {
    Ok(get_format_tag(plan)? == Some(tag))
}

pub fn contains_format_tag(plan: &Arc<dyn ExecutionPlan>, tag: FormatTag) -> Result<bool> {
    if is_format_tag(plan.as_ref(), tag)? {
        return Ok(true);
    }
    for child in plan.children() {
        if contains_format_tag(child, tag)? {
            return Ok(true);
        }
    }
    Ok(false)
}

pub fn collect_format_tags(plan: &Arc<dyn ExecutionPlan>) -> Result<HashSet<FormatTag>> {
    let mut tags = HashSet::new();
    if let Some(tag) = get_format_tag(plan.as_ref())? {
        tags.insert(tag);
    }
    for child in plan.children() {
        tags.extend(collect_format_tags(child)?);
    }
    Ok(tags)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::physical_plan::empty::EmptyExec;

    use super::*;

    #[test]
    fn test_format_tag_display() {
        assert_eq!(FormatTag::Delta.to_string(), "Delta");
        assert_eq!(FormatTag::Iceberg.to_string(), "Iceberg");
        assert_eq!(FormatTag::Hudi.to_string(), "Hudi");
        assert_eq!(FormatTag::Custom("MyFormat").to_string(), "MyFormat");
    }

    #[test]
    fn test_register_and_get_tag() -> Result<()> {
        register_format_type::<EmptyExec>(FormatTag::Delta)?;

        let plan = Arc::new(EmptyExec::new(Arc::new(
            datafusion::arrow::datatypes::Schema::empty(),
        )));
        let tag = get_format_tag(plan.as_ref())?;

        // EmptyExec should be registered
        assert!(tag.is_some());
        Ok(())
    }

    #[test]
    fn test_is_format_tag() -> Result<()> {
        register_format_type::<EmptyExec>(FormatTag::Delta)?;

        let plan = Arc::new(EmptyExec::new(Arc::new(
            datafusion::arrow::datatypes::Schema::empty(),
        )));

        assert!(is_format_tag(plan.as_ref(), FormatTag::Delta)?);
        assert!(!is_format_tag(plan.as_ref(), FormatTag::Iceberg)?);
        Ok(())
    }

    #[test]
    fn test_unregistered_plan() -> Result<()> {
        let schema = Arc::new(datafusion::arrow::datatypes::Schema::empty());
        let plan = Arc::new(EmptyExec::new(schema));

        let _tag = get_format_tag(plan.as_ref())?;
        Ok(())
    }

    #[test]
    fn test_format_tag_equality() {
        assert_eq!(FormatTag::Delta, FormatTag::Delta);
        assert_ne!(FormatTag::Delta, FormatTag::Iceberg);
        assert_eq!(FormatTag::Custom("A"), FormatTag::Custom("A"));
        assert_ne!(FormatTag::Custom("A"), FormatTag::Custom("B"));
    }
}
