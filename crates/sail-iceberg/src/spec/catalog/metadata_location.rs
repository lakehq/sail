use std::fmt::Display;
use std::str::FromStr;

use uuid::Uuid;

#[derive(Clone, Debug, PartialEq)]
pub struct MetadataLocation {
    table_location: String,
    version: i32,
    id: Uuid,
}

impl MetadataLocation {
    pub fn new_with_table_location(table_location: impl ToString) -> Self {
        Self {
            table_location: table_location.to_string(),
            version: 0,
            id: Uuid::new_v4(),
        }
    }
    pub fn with_next_version(&self) -> Self {
        Self {
            table_location: self.table_location.clone(),
            version: self.version + 1,
            id: Uuid::new_v4(),
        }
    }
    fn parse_metadata_path_prefix(path: &str) -> Result<String, String> {
        let prefix = path.strip_suffix("/metadata").ok_or_else(|| {
            format!(
                "Metadata location not under \"/metadata\" subdirectory: {}",
                path
            )
        })?;
        Ok(prefix.to_string())
    }
    fn parse_file_name(file_name: &str) -> Result<(i32, Uuid), String> {
        let (version, id) = file_name
            .strip_suffix(".metadata.json")
            .ok_or_else(|| format!("Invalid metadata file ending: {}", file_name))?
            .split_once('-')
            .ok_or_else(|| format!("Invalid metadata file name format: {}", file_name))?;
        let v = version.parse::<i32>().map_err(|e| e.to_string())?;
        let u = Uuid::parse_str(id).map_err(|e| e.to_string())?;
        Ok((v, u))
    }
}

impl Display for MetadataLocation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}/metadata/{:0>5}-{}.metadata.json",
            self.table_location, self.version, self.id
        )
    }
}

impl FromStr for MetadataLocation {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (path, file_name) = s
            .rsplit_once('/')
            .ok_or_else(|| format!("Invalid metadata location: {}", s))?;
        let prefix = Self::parse_metadata_path_prefix(path)?;
        let (version, id) = Self::parse_file_name(file_name)?;
        Ok(Self {
            table_location: prefix,
            version,
            id,
        })
    }
}
