// SPDX-License-Identifier: Apache-2.0

//! Python data source format - Generic bridge to Python-based data sources.
//!
//! This format allows users to implement data sources in Python and use them
//! with Lakesail's distributed query engine. Any Python class that returns
//! Arrow data can be used!

pub use sail_python_datasource::PythonDataSourceFormat;
