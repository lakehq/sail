use std::ffi::CString;
use std::marker::PhantomData;

use pyo3::prelude::PyModule;
use pyo3::{Bound, PyResult, Python};

pub struct Module<I> {
    name: &'static str,
    source: &'static str,
    _initializer: PhantomData<I>,
}

impl<I: ModuleInitializer> Module<I> {
    pub const fn new(name: &'static str, source: &'static str) -> Self {
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

pub trait ModuleInitializer {
    fn init(m: &Bound<PyModule>) -> PyResult<()>;
}

impl ModuleInitializer for () {
    fn init(_: &Bound<PyModule>) -> PyResult<()> {
        Ok(())
    }
}

pub struct Modules;

impl Modules {
    pub const DUCKLAKE_METADATA: Module<()> = Module::new(
        "_sail_ducklake_metadata",
        include_str!("python/ducklake_metadata.py"),
    );
}
