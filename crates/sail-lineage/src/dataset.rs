use datafusion::arrow::datatypes::SchemaRef;
use url::Url;

use crate::event::{
    DatasetFacetsBuilder, Facets, InputDataset, LifecycleStateChange, OutputDataset,
};

/// The namespace used for datasets on the local file system.
const FILE_NAMESPACE: &str = "file";

/// A dataset read or written by a query, as observed during plan resolution.
#[derive(Debug, Clone, Default)]
pub struct LineageDataset {
    /// The dataset identifier, which takes precedence over the identifier
    /// derived from the location or the table name (e.g. for JDBC tables).
    pub identifier: Option<DatasetIdentifier>,
    /// The physical location of the dataset (a path or a URL).
    pub location: Option<String>,
    /// The fully qualified catalog table name, if the dataset is a catalog table.
    pub table: Option<Vec<String>>,
    /// The data source format (e.g. `parquet` or `delta`).
    pub format: Option<String>,
    pub schema: Option<SchemaRef>,
    /// The lifecycle state change for output datasets.
    pub lifecycle: Option<LifecycleStateChange>,
}

/// The OpenLineage identity of a dataset.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DatasetIdentifier {
    pub namespace: String,
    pub name: String,
}

impl LineageDataset {
    /// Returns the primary identifier of the dataset.
    ///
    /// The physical location is preferred, following the OpenLineage naming conventions,
    /// so that the same data read through a path or through a catalog table
    /// resolves to the same dataset. The table name is reported as a symlink.
    pub fn identifier(&self, default_namespace: &str) -> Option<DatasetIdentifier> {
        self.identifier
            .clone()
            .or_else(|| self.location.as_deref().map(location_identifier))
            .or_else(|| self.table_identifier(default_namespace))
    }

    fn table_identifier(&self, default_namespace: &str) -> Option<DatasetIdentifier> {
        let table = self.table.as_ref().filter(|x| !x.is_empty())?;
        Some(DatasetIdentifier {
            namespace: default_namespace.to_string(),
            name: table.join("."),
        })
    }

    fn facets(&self, identifier: &DatasetIdentifier, default_namespace: &str) -> Facets {
        let mut builder = DatasetFacetsBuilder::default().data_source(&identifier.namespace);
        if let Some(schema) = &self.schema {
            builder = builder.schema(schema.fields());
        }
        if let Some(symlink) = self.table_identifier(default_namespace)
            && &symlink != identifier
        {
            builder = builder.symlink(&symlink.namespace, &symlink.name);
        }
        if let Some(format) = self.format.as_deref().filter(|x| !x.is_empty()) {
            builder = builder.storage(format);
        }
        if let Some(lifecycle) = self.lifecycle {
            builder = builder.lifecycle(lifecycle);
        }
        builder.build()
    }

    pub fn to_input(&self, default_namespace: &str) -> Option<InputDataset> {
        let identifier = self.identifier(default_namespace)?;
        Some(InputDataset {
            facets: self.facets(&identifier, default_namespace),
            namespace: identifier.namespace,
            name: identifier.name,
        })
    }

    pub fn to_output(&self, default_namespace: &str) -> Option<OutputDataset> {
        let identifier = self.identifier(default_namespace)?;
        Some(OutputDataset {
            facets: self.facets(&identifier, default_namespace),
            namespace: identifier.namespace,
            name: identifier.name,
        })
    }
}

/// Returns the dataset identifier for a physical location according to the
/// OpenLineage dataset naming conventions.
///
/// See also: <https://openlineage.io/docs/spec/naming>
pub fn location_identifier(location: &str) -> DatasetIdentifier {
    let location = location.trim();
    match Url::parse(location) {
        // A single-letter scheme is a Windows drive letter rather than a URL scheme.
        Ok(url) if url.scheme().len() > 1 && url.scheme() != "file" => url_identifier(&url),
        Ok(url) if url.scheme() == "file" => DatasetIdentifier {
            namespace: FILE_NAMESPACE.to_string(),
            name: normalize_path(url.path()),
        },
        _ => DatasetIdentifier {
            namespace: FILE_NAMESPACE.to_string(),
            name: normalize_path(&absolute_path(location)),
        },
    }
}

fn url_identifier(url: &Url) -> DatasetIdentifier {
    let scheme = match url.scheme() {
        "s3a" | "s3n" => "s3",
        "gcs" => "gs",
        x => x,
    };
    let authority = match (url.username(), url.host_str(), url.port()) {
        ("", Some(host), None) => host.to_string(),
        ("", Some(host), Some(port)) => format!("{host}:{port}"),
        (user, Some(host), None) => format!("{user}@{host}"),
        (user, Some(host), Some(port)) => format!("{user}@{host}:{port}"),
        (_, None, _) => String::new(),
    };
    let path = normalize_path(url.path());
    // Object stores use the object key (without the leading slash) as the name,
    // while file systems such as HDFS use the absolute path.
    let name = match scheme {
        "s3" | "gs" | "abfs" | "abfss" | "wasb" | "wasbs" | "az" | "azure" => {
            path.trim_start_matches('/').to_string()
        }
        _ => path,
    };
    DatasetIdentifier {
        namespace: format!("{scheme}://{authority}"),
        name,
    }
}

fn absolute_path(path: &str) -> String {
    std::path::absolute(path)
        .map(|x| x.to_string_lossy().into_owned())
        .unwrap_or_else(|_| path.to_string())
}

fn normalize_path(path: &str) -> String {
    let path = path.trim_end_matches('/');
    if path.is_empty() {
        "/".to_string()
    } else {
        path.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(namespace: &str, name: &str) -> DatasetIdentifier {
        DatasetIdentifier {
            namespace: namespace.to_string(),
            name: name.to_string(),
        }
    }

    #[test]
    fn test_location_identifier() {
        assert_eq!(
            location_identifier("s3://bucket/path/to/table/"),
            id("s3://bucket", "path/to/table")
        );
        assert_eq!(
            location_identifier("s3a://bucket/path"),
            id("s3://bucket", "path")
        );
        assert_eq!(
            location_identifier("gs://bucket/a/b"),
            id("gs://bucket", "a/b")
        );
        assert_eq!(
            location_identifier("abfss://container@account.dfs.core.windows.net/data/t"),
            id("abfss://container@account.dfs.core.windows.net", "data/t")
        );
        assert_eq!(
            location_identifier("hdfs://namenode:8020/warehouse/t"),
            id("hdfs://namenode:8020", "/warehouse/t")
        );
        assert_eq!(
            location_identifier("file:///tmp/data/"),
            id("file", "/tmp/data")
        );
        assert_eq!(location_identifier("/tmp/data"), id("file", "/tmp/data"));
    }

    #[test]
    fn test_relative_path_is_absolute() {
        let identifier = location_identifier("relative/data");
        assert_eq!(identifier.namespace, "file");
        assert!(std::path::Path::new(&identifier.name).is_absolute());
        assert!(identifier.name.ends_with("relative/data"));
    }

    #[test]
    fn test_table_symlink() {
        let dataset = LineageDataset {
            location: Some("s3://bucket/warehouse/db/t".to_string()),
            table: Some(vec!["spark_catalog".into(), "db".into(), "t".into()]),
            format: Some("delta".to_string()),
            ..Default::default()
        };
        let input = dataset.to_input("sail");
        let Some(input) = input else {
            unreachable!("dataset should have an identifier");
        };
        assert_eq!(input.namespace, "s3://bucket");
        assert_eq!(input.name, "warehouse/db/t");
        let symlinks = &input.facets["symlinks"]["identifiers"][0];
        assert_eq!(symlinks["namespace"], "sail");
        assert_eq!(symlinks["name"], "spark_catalog.db.t");
        assert_eq!(input.facets["storage"]["storageLayer"], "delta");
    }

    #[test]
    fn test_table_without_location() {
        let dataset = LineageDataset {
            table: Some(vec!["db".into(), "t".into()]),
            ..Default::default()
        };
        assert_eq!(dataset.identifier("sail"), Some(id("sail", "db.t")));
        let Some(output) = dataset.to_output("sail") else {
            unreachable!("dataset should have an identifier");
        };
        assert!(!output.facets.contains_key("symlinks"));
    }
}
