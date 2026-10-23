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
        comment: String,
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
                nullable,
                comment,
                default,
                position,
            } => ColumnChange::Add {
                path,
                data_type: f(data_type)?,
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
