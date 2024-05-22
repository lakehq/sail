use std::collections::HashMap;

#[derive(Debug, Clone)]
pub(crate) struct CaseInsensitiveHashMap<T> {
    inner: HashMap<String, T>,
}

impl<T> CaseInsensitiveHashMap<T> {
    fn new(params: HashMap<String, T>) -> Self {
        let mut map = HashMap::new();
        for (key, value) in params {
            map.insert(key.to_lowercase(), value);
        }
        CaseInsensitiveHashMap { inner: map }
    }
}

impl<T> From<HashMap<String, T>> for CaseInsensitiveHashMap<T> {
    fn from(params: HashMap<String, T>) -> Self {
        CaseInsensitiveHashMap::new(params)
    }
}
