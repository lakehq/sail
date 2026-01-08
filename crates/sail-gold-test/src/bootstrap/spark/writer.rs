use std::path::Path;
use std::{fmt, fs};

use serde::{Deserialize, Serialize};

use crate::bootstrap::spark::common::TestData;

#[derive(Debug)]
struct DataError {
    source: Box<dyn std::error::Error>,
    data: String,
}

impl fmt::Display for DataError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.source, self.data)
    }
}

impl std::error::Error for DataError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.source.as_ref())
    }
}

pub struct TestSuiteWriter<P> {
    pub input_path: P,
    pub output_path: P,
}

impl<P> TestSuiteWriter<P>
where
    P: AsRef<Path>,
{
    fn read<T>(
        &self,
        input_file: impl AsRef<Path>,
    ) -> Result<Vec<TestData<T>>, Box<dyn std::error::Error>>
    where
        T: for<'de> Deserialize<'de>,
    {
        let content = fs::read_to_string(self.input_path.as_ref().join(input_file))?;
        let data: Vec<TestData<T>> = content
            .lines()
            .map(|x| {
                serde_json::from_str(x).map_err(|e| DataError {
                    source: Box::new(e),
                    data: x.to_string(),
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(data)
    }

    fn write<S>(
        &self,
        suite: S,
        output_file: impl AsRef<Path>,
    ) -> Result<(), Box<dyn std::error::Error>>
    where
        S: Serialize,
    {
        let mut output = serde_json::to_string_pretty(&suite)?;
        output.push('\n');
        let path = self.output_path.as_ref().join(output_file);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(path, output)?;
        Ok(())
    }

    pub fn write_one<T, B, S>(
        &self,
        input_file: impl AsRef<Path>,
        output_file: impl AsRef<Path>,
        builder: B,
    ) -> Result<(), Box<dyn std::error::Error>>
    where
        B: Fn(Vec<TestData<T>>) -> S,
        T: for<'de> Deserialize<'de>,
        S: Serialize,
    {
        let data = self.read(input_file)?;
        let suite = builder(data);
        self.write(suite, output_file)
    }

    pub fn write_many<T, B, O, S, OF, OP>(
        &self,
        input_file: impl AsRef<Path>,
        output_file: OF,
        builder: B,
    ) -> Result<(), Box<dyn std::error::Error>>
    where
        B: Fn(Vec<TestData<T>>) -> O,
        T: for<'de> Deserialize<'de>,
        O: IntoIterator<Item = (String, S)>,
        S: Serialize,
        OF: Fn(&str) -> OP,
        OP: AsRef<Path>,
    {
        let data = self.read(input_file)?;
        let suites = builder(data);
        for (name, suite) in suites {
            self.write(suite, output_file(&name))?;
        }
        Ok(())
    }
}
