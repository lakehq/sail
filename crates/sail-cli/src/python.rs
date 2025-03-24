use std::ffi::CString;
use std::marker::PhantomData;

use log::{error, logger, Level, MetadataBuilder, Record};
use pyo3::prelude::{PyAnyMethods, PyModule, PyModuleMethods};
use pyo3::{pyclass, pymethods, Bound, PyAny, PyResult, Python};

pub struct Module<I> {
    name: &'static str,
    source: &'static str,
    _initializer: PhantomData<I>,
}

impl<I: ModuleInitializer> Module<I> {
    const fn new(name: &'static str, source: &'static str) -> Self {
        Self {
            name,
            source,
            _initializer: PhantomData,
        }
    }

    pub fn load<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyModule>> {
        let m = PyModule::from_code(
            py,
            CString::new(self.source)?.as_c_str(),
            CString::new("")?.as_c_str(),
            CString::new(self.name)?.as_c_str(),
        )?;
        I::init(&m)?;
        Ok(m)
    }
}

pub struct Modules;

impl Modules {
    pub const NATIVE_LOGGING: Module<NativeLogging> = Module::new(
        "_sail_cli_native_logging",
        include_str!("python/native_logging.py"),
    );
    pub const SPARK_SHELL: Module<()> = Module::new(
        "_sail_cli_spark_shell",
        include_str!("python/spark_shell.py"),
    );
    pub const SPARK_MCP_SERVER: Module<()> = Module::new(
        "_sail_cli_spark_mcp_server",
        include_str!("python/spark_mcp_server.py"),
    );
}

pub trait ModuleInitializer {
    fn init(m: &Bound<PyModule>) -> PyResult<()>;
}

impl ModuleInitializer for () {
    fn init(_: &Bound<PyModule>) -> PyResult<()> {
        Ok(())
    }
}

#[pyclass]
pub struct NativeLogging;

impl NativeLogging {
    /// Emit a Python log record as a Rust log record.
    fn try_emit(record: Bound<'_, PyAny>) -> PyResult<()> {
        let level = record.getattr("levelno")?.extract::<u8>()?;
        let message = record.getattr("getMessage")?.call0()?.extract::<String>()?;
        let pathname = record.getattr("pathname")?.extract::<String>()?;
        let line = record.getattr("lineno")?.extract::<u32>()?;
        let name = record.getattr("name")?.extract::<String>()?;

        let target = name.trim();
        let target = if target.is_empty() {
            "python"
        } else {
            // replace "." with module separator "::" in Rust-style targets
            &format!("python::{}", target.replace('.', "::"))
        };

        let metadata = {
            let mut builder = MetadataBuilder::new();
            builder.target(target);
            if level >= 40u8 {
                builder.level(Level::Error)
            } else if level >= 30u8 {
                builder.level(Level::Warn)
            } else if level >= 20u8 {
                builder.level(Level::Info)
            } else if level >= 10u8 {
                builder.level(Level::Debug)
            } else {
                builder.level(Level::Trace)
            };
            builder.build()
        };

        logger().log(
            &Record::builder()
                .metadata(metadata)
                .args(format_args!("{}", &message))
                .line(Some(line))
                .file(Some(&pathname))
                .module_path(Some(&pathname))
                .build(),
        );

        Ok(())
    }
}

#[pymethods]
impl NativeLogging {
    #[staticmethod]
    fn emit(record: Bound<'_, PyAny>) {
        match Self::try_emit(record) {
            Ok(()) => {}
            Err(e) => error!("failed to emit log record: {e}"),
        }
    }
}

impl ModuleInitializer for NativeLogging {
    fn init(m: &Bound<PyModule>) -> PyResult<()> {
        m.add_class::<Self>()?;
        Ok(())
    }
}
