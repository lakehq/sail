use serde::{Deserialize, Deserializer};

/// The raw test data written by Spark test suites.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TestData<T> {
    pub kind: String,
    pub data: T,
    pub exception: Option<String>,
}

/// A wrapper for parsing test data JSON value of different types.
#[derive(Debug, Clone, PartialEq)]
pub enum TestDataObject<T> {
    Object(T),
    String(String),
    Null,
}

impl<T> TestDataObject<T> {
    pub fn into_object(self) -> Option<T> {
        match self {
            TestDataObject::Object(obj) => Some(obj),
            _ => None,
        }
    }
}

impl<'de, T> Deserialize<'de> for TestDataObject<T>
where
    T: Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = serde_json::Value::deserialize(deserializer)?;

        if value.is_null() {
            Ok(TestDataObject::Null)
        } else if let Some(s) = value.as_str() {
            Ok(TestDataObject::String(s.to_string()))
        } else {
            let parsed = T::deserialize(value).map_err(serde::de::Error::custom)?;
            Ok(TestDataObject::Object(parsed))
        }
    }
}
