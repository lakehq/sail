use sail_python_udf::config::{PySparkArtifactKind, PySparkPythonArtifact};

use crate::error::{ExecutionError, ExecutionResult};
use crate::task::definition::{LocalRelationResource, TaskLaunchContext, TaskResources};
use crate::worker::r#gen;

impl From<TaskLaunchContext> for r#gen::TaskLaunchContext {
    fn from(value: TaskLaunchContext) -> Self {
        let TaskLaunchContext { resources } = value;
        r#gen::TaskLaunchContext {
            resources: Some(resources.into()),
        }
    }
}

impl TryFrom<r#gen::TaskLaunchContext> for TaskLaunchContext {
    type Error = ExecutionError;

    fn try_from(value: r#gen::TaskLaunchContext) -> Result<Self, Self::Error> {
        Ok(TaskLaunchContext {
            resources: value
                .resources
                .map(TaskResources::try_from)
                .transpose()?
                .unwrap_or_default(),
        })
    }
}

impl From<TaskResources> for r#gen::TaskResources {
    fn from(value: TaskResources) -> Self {
        let TaskResources {
            python_artifacts,
            local_relation_resources,
        } = value;
        r#gen::TaskResources {
            python_artifacts: python_artifacts.into_iter().map(|x| x.into()).collect(),
            local_relation_resources: local_relation_resources
                .into_iter()
                .map(|x| x.into())
                .collect(),
        }
    }
}

impl TryFrom<r#gen::TaskResources> for TaskResources {
    type Error = ExecutionError;

    fn try_from(value: r#gen::TaskResources) -> Result<Self, Self::Error> {
        Ok(TaskResources {
            python_artifacts: value
                .python_artifacts
                .into_iter()
                .map(|x| x.try_into())
                .collect::<ExecutionResult<Vec<_>>>()?,
            local_relation_resources: value
                .local_relation_resources
                .into_iter()
                .map(|x| x.try_into())
                .collect::<ExecutionResult<Vec<_>>>()?,
        })
    }
}

impl From<LocalRelationResource> for r#gen::LocalRelationResource {
    fn from(value: LocalRelationResource) -> Self {
        let LocalRelationResource {
            key,
            data,
            uri,
            sha256,
            size,
        } = value;
        r#gen::LocalRelationResource {
            key,
            data,
            uri,
            sha256,
            size,
        }
    }
}

impl TryFrom<r#gen::LocalRelationResource> for LocalRelationResource {
    type Error = ExecutionError;

    fn try_from(value: r#gen::LocalRelationResource) -> Result<Self, Self::Error> {
        validate_local_relation_resource(&value)?;
        Ok(Self {
            key: value.key,
            data: value.data,
            uri: value.uri,
            sha256: value.sha256,
            size: value.size,
        })
    }
}

impl From<PySparkPythonArtifact> for r#gen::PySparkPythonArtifact {
    fn from(value: PySparkPythonArtifact) -> Self {
        let PySparkPythonArtifact {
            name,
            python_path,
            data,
            uri,
            sha256,
            size,
            kind,
        } = value;
        r#gen::PySparkPythonArtifact {
            name,
            python_path,
            data,
            uri,
            sha256,
            size,
            kind: encode_pyspark_artifact_kind(kind) as i32,
        }
    }
}

impl TryFrom<r#gen::PySparkPythonArtifact> for PySparkPythonArtifact {
    type Error = ExecutionError;

    fn try_from(value: r#gen::PySparkPythonArtifact) -> Result<Self, Self::Error> {
        let kind = decode_pyspark_artifact_kind(value.kind)?;
        validate_pyspark_artifact(&value)?;
        Ok(Self {
            name: value.name,
            python_path: value.python_path,
            data: value.data,
            uri: value.uri,
            sha256: value.sha256,
            size: value.size,
            kind,
        })
    }
}

fn validate_pyspark_artifact(value: &r#gen::PySparkPythonArtifact) -> ExecutionResult<()> {
    if value.name.is_empty() {
        return Err(ExecutionError::InvalidArgument(
            "PySpark artifact name must not be empty".to_string(),
        ));
    }
    if value.python_path.is_empty() {
        return Err(ExecutionError::InvalidArgument(format!(
            "PySpark artifact {} must have a local Python path",
            value.name
        )));
    }
    if value.sha256.len() != 64 || !value.sha256.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(ExecutionError::InvalidArgument(format!(
            "PySpark artifact {} must have a SHA-256 hex digest",
            value.name
        )));
    }
    match (&value.data, value.uri.as_deref()) {
        (Some(_), Some(_)) => Err(ExecutionError::InvalidArgument(format!(
            "PySpark artifact {} must not have both inline data and an object-store URI",
            value.name
        ))),
        (None, None) => Err(ExecutionError::InvalidArgument(format!(
            "PySpark artifact {} must have inline data or an object-store URI",
            value.name
        ))),
        (Some(data), None) if data.len() as u64 != value.size => {
            Err(ExecutionError::InvalidArgument(format!(
                "PySpark artifact {} inline data size does not match declared size",
                value.name
            )))
        }
        (_, Some("")) => Err(ExecutionError::InvalidArgument(format!(
            "PySpark artifact {} must not have an empty object-store URI",
            value.name
        ))),
        _ => Ok(()),
    }
}

fn validate_local_relation_resource(value: &r#gen::LocalRelationResource) -> ExecutionResult<()> {
    if value.key.is_empty() {
        return Err(ExecutionError::InvalidArgument(
            "LocalRelation resource key must not be empty".to_string(),
        ));
    }
    if value.sha256.len() != 64 || !value.sha256.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(ExecutionError::InvalidArgument(format!(
            "LocalRelation resource {} must have a SHA-256 hex digest",
            value.key
        )));
    }
    if value.key != value.sha256 {
        return Err(ExecutionError::InvalidArgument(format!(
            "LocalRelation resource key {} must match its SHA-256 digest",
            value.key
        )));
    }
    match (&value.data, value.uri.as_deref()) {
        (Some(_), Some(_)) => Err(ExecutionError::InvalidArgument(format!(
            "LocalRelation resource {} must not have both inline data and an object-store URI",
            value.key
        ))),
        (None, None) => Err(ExecutionError::InvalidArgument(format!(
            "LocalRelation resource {} must have inline data or an object-store URI",
            value.key
        ))),
        (Some(data), None) if data.len() as u64 != value.size => {
            Err(ExecutionError::InvalidArgument(format!(
                "LocalRelation resource {} inline data size does not match declared size",
                value.key
            )))
        }
        (_, Some("")) => Err(ExecutionError::InvalidArgument(format!(
            "LocalRelation resource {} must not have an empty object-store URI",
            value.key
        ))),
        _ => Ok(()),
    }
}

fn decode_pyspark_artifact_kind(kind: i32) -> ExecutionResult<PySparkArtifactKind> {
    let kind = r#gen::PySparkArtifactKind::try_from(kind).map_err(|e| {
        ExecutionError::InvalidArgument(format!("invalid PySpark artifact kind: {e}"))
    })?;
    match kind {
        r#gen::PySparkArtifactKind::Unspecified => Err(ExecutionError::InvalidArgument(
            "PySpark artifact kind must not be unspecified".to_string(),
        )),
        r#gen::PySparkArtifactKind::PyFile => Ok(PySparkArtifactKind::PyFile),
        r#gen::PySparkArtifactKind::File => Ok(PySparkArtifactKind::File),
        r#gen::PySparkArtifactKind::Archive => Ok(PySparkArtifactKind::Archive),
    }
}

fn encode_pyspark_artifact_kind(kind: PySparkArtifactKind) -> r#gen::PySparkArtifactKind {
    match kind {
        PySparkArtifactKind::PyFile => r#gen::PySparkArtifactKind::PyFile,
        PySparkArtifactKind::File => r#gen::PySparkArtifactKind::File,
        PySparkArtifactKind::Archive => r#gen::PySparkArtifactKind::Archive,
    }
}
