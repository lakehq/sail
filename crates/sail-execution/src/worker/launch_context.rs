use sail_python_udf::config::{PySparkArtifactKind, PySparkPythonArtifact};

use crate::error::{ExecutionError, ExecutionResult};
use crate::task::definition::{TaskLaunchContext, TaskResources};
use crate::worker::gen;

impl From<TaskLaunchContext> for gen::TaskLaunchContext {
    fn from(value: TaskLaunchContext) -> Self {
        let TaskLaunchContext { resources } = value;
        gen::TaskLaunchContext {
            resources: Some(resources.into()),
        }
    }
}

impl TryFrom<gen::TaskLaunchContext> for TaskLaunchContext {
    type Error = ExecutionError;

    fn try_from(value: gen::TaskLaunchContext) -> Result<Self, Self::Error> {
        Ok(TaskLaunchContext {
            resources: value
                .resources
                .map(TaskResources::try_from)
                .transpose()?
                .unwrap_or_default(),
        })
    }
}

impl From<TaskResources> for gen::TaskResources {
    fn from(value: TaskResources) -> Self {
        let TaskResources { python_artifacts } = value;
        gen::TaskResources {
            python_artifacts: python_artifacts.into_iter().map(|x| x.into()).collect(),
        }
    }
}

impl TryFrom<gen::TaskResources> for TaskResources {
    type Error = ExecutionError;

    fn try_from(value: gen::TaskResources) -> Result<Self, Self::Error> {
        Ok(TaskResources {
            python_artifacts: value
                .python_artifacts
                .into_iter()
                .map(|x| x.try_into())
                .collect::<ExecutionResult<Vec<_>>>()?,
        })
    }
}

impl From<PySparkPythonArtifact> for gen::PySparkPythonArtifact {
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
        gen::PySparkPythonArtifact {
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

impl TryFrom<gen::PySparkPythonArtifact> for PySparkPythonArtifact {
    type Error = ExecutionError;

    fn try_from(value: gen::PySparkPythonArtifact) -> Result<Self, Self::Error> {
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

fn validate_pyspark_artifact(value: &gen::PySparkPythonArtifact) -> ExecutionResult<()> {
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

fn decode_pyspark_artifact_kind(kind: i32) -> ExecutionResult<PySparkArtifactKind> {
    let kind = gen::PySparkArtifactKind::try_from(kind).map_err(|e| {
        ExecutionError::InvalidArgument(format!("invalid PySpark artifact kind: {e}"))
    })?;
    match kind {
        gen::PySparkArtifactKind::Unspecified => Err(ExecutionError::InvalidArgument(
            "PySpark artifact kind must not be unspecified".to_string(),
        )),
        gen::PySparkArtifactKind::PyFile => Ok(PySparkArtifactKind::PyFile),
        gen::PySparkArtifactKind::File => Ok(PySparkArtifactKind::File),
        gen::PySparkArtifactKind::Archive => Ok(PySparkArtifactKind::Archive),
    }
}

fn encode_pyspark_artifact_kind(kind: PySparkArtifactKind) -> gen::PySparkArtifactKind {
    match kind {
        PySparkArtifactKind::PyFile => gen::PySparkArtifactKind::PyFile,
        PySparkArtifactKind::File => gen::PySparkArtifactKind::File,
        PySparkArtifactKind::Archive => gen::PySparkArtifactKind::Archive,
    }
}
