use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use super::DataType;

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Serialize, Deserialize)]
pub enum ColumnPosition {
    First,
    After(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Serialize, Deserialize)]
pub enum ColumnChange<T = DataType> {
    Add {
        path: Vec<String>,
        data_type: T,
        #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
        metadata: BTreeMap<String, String>,
        nullable: bool,
        comment: Option<String>,
        default: Option<String>,
        position: Option<ColumnPosition>,
    },
    Drop {
        path: Vec<String>,
        if_exists: bool,
    },
    Rename {
        path: Vec<String>,
        name: String,
    },
    Comment {
        path: Vec<String>,
        comment: Option<String>,
    },
    Nullability {
        path: Vec<String>,
        nullable: bool,
    },
    Position {
        path: Vec<String>,
        position: ColumnPosition,
    },
}

impl<T> ColumnChange<T> {
    pub fn path(&self) -> &[String] {
        match self {
            Self::Add { path, .. }
            | Self::Drop { path, .. }
            | Self::Rename { path, .. }
            | Self::Comment { path, .. }
            | Self::Nullability { path, .. }
            | Self::Position { path, .. } => path,
        }
    }

    pub fn try_map_type<U, E>(
        self,
        f: impl FnOnce(T) -> Result<U, E>,
    ) -> Result<ColumnChange<U>, E> {
        Ok(match self {
            Self::Add {
                path,
                data_type,
                metadata,
                nullable,
                comment,
                default,
                position,
            } => ColumnChange::Add {
                path,
                data_type: f(data_type)?,
                metadata,
                nullable,
                comment,
                default,
                position,
            },
            Self::Drop { path, if_exists } => ColumnChange::Drop { path, if_exists },
            Self::Rename { path, name } => ColumnChange::Rename { path, name },
            Self::Comment { path, comment } => ColumnChange::Comment { path, comment },
            Self::Nullability { path, nullable } => ColumnChange::Nullability { path, nullable },
            Self::Position { path, position } => ColumnChange::Position { path, position },
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn add_metadata_round_trips_and_maps_types() -> Result<(), Box<dyn std::error::Error>> {
        let change = ColumnChange::Add {
            path: vec!["u".into()],
            data_type: 16_u8,
            metadata: BTreeMap::from([("iceberg.type".into(), "uuid".into())]),
            nullable: true,
            comment: Some("UUID column".into()),
            default: None,
            position: Some(ColumnPosition::First),
        };
        let json = serde_json::to_string(&change)?;
        assert_eq!(serde_json::from_str::<ColumnChange<u8>>(&json)?, change);
        let mapped = change
            .clone()
            .try_map_type(|ty| Ok::<_, std::convert::Infallible>(u16::from(ty)))?;
        assert_eq!(serde_json::to_value(mapped)?, serde_json::to_value(change)?);
        Ok(())
    }

    #[test]
    fn add_without_metadata_remains_serde_compatible() -> serde_json::Result<()> {
        let legacy = serde_json::json!({"Add": {
            "path": ["x"], "data_type": 16, "nullable": true,
            "comment": null, "default": null, "position": null
        }});
        let change: ColumnChange<u8> = serde_json::from_value(legacy.clone())?;
        assert_eq!(
            change,
            ColumnChange::Add {
                path: vec!["x".into()],
                data_type: 16,
                metadata: BTreeMap::new(),
                nullable: true,
                comment: None,
                default: None,
                position: None,
            }
        );
        assert_eq!(serde_json::to_value(change)?, legacy);
        Ok(())
    }
}
