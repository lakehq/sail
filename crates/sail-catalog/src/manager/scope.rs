// SPDX-License-Identifier: Apache-2.0

use std::future::Future;
use std::sync::{Arc, Mutex};

use datafusion_expr::ScalarUDF;
use sail_common_datafusion::catalog::{TableStatus, TemporaryViewDefinition, TemporaryViewObjects};

use crate::error::{CatalogError, CatalogResult};
use crate::provider::Namespace;

tokio::task_local! {
    static VIEW_DEFINITION_SCOPE: Arc<ViewDefinitionScope>;
}

/// How the catalog manager resolves the names of the SQL query that defines a temporary view,
/// for the futures that [`ViewDefinitionScope::run`] runs. Spark analyzes such a query in the
/// catalog and the database that it was created in, and a name refers to a temporary view or
/// a temporary function only if it referred to one when the view was created.
#[derive(Debug)]
pub enum ViewDefinitionScope {
    /// The view is created, and the temporary views and the functions of the session that
    /// the query resolves are recorded.
    Create(Mutex<TemporaryViewObjects>),
    /// The view resolves its query again. A name that referred to a temporary view or a
    /// function of the session refers to the one that it referred to then, which the plan of
    /// the view reads as well, and any other name refers to neither.
    Resolve(Arc<TemporaryViewDefinition>),
}

impl ViewDefinitionScope {
    pub fn create() -> Self {
        Self::Create(Mutex::default())
    }

    /// Runs the future in this scope.
    pub async fn run<F: Future>(self: Arc<Self>, future: F) -> F::Output {
        VIEW_DEFINITION_SCOPE.scope(self, future).await
    }

    /// Returns the temporary views and the functions of the session that the query
    /// resolved when the view was created.
    pub fn recorded(&self) -> CatalogResult<TemporaryViewObjects> {
        match self {
            Self::Create(objects) => Ok(objects
                .lock()
                .map_err(|e| CatalogError::Internal(e.to_string()))?
                .clone()),
            Self::Resolve(_) => Ok(TemporaryViewObjects::default()),
        }
    }

    /// Records an object that the query resolves when the view is created.
    fn record(record: impl FnOnce(&mut TemporaryViewObjects)) {
        if let Some(scope) = Self::current()
            && let Self::Create(objects) = scope.as_ref()
            && let Ok(mut objects) = objects.lock()
        {
            record(&mut objects);
        }
    }

    /// Returns the objects that the query resolved when the view was created, while the
    /// query is resolved again.
    fn pinned() -> Option<Arc<TemporaryViewDefinition>> {
        match Self::current()?.as_ref() {
            Self::Resolve(definition) => Some(Arc::clone(definition)),
            Self::Create(_) => None,
        }
    }

    fn current() -> Option<Arc<Self>> {
        VIEW_DEFINITION_SCOPE.try_with(Arc::clone).ok()
    }

    /// The current catalog and database in which the query is resolved again, if it is.
    /// They are those of a session, so they form a valid namespace.
    pub(super) fn namespace() -> Option<(Arc<str>, Namespace)> {
        let definition = Self::pinned()?;
        let database = definition.database.clone().try_into().ok()?;
        Some((definition.catalog.as_str().into(), database))
    }

    /// Returns the temporary view that a reference names while the query is resolved again.
    pub(super) fn pinned_temporary_view<T: AsRef<str>>(
        reference: &[T],
    ) -> Option<Option<TableStatus>> {
        let reference = reference
            .iter()
            .map(|x| x.as_ref().to_string())
            .collect::<Vec<_>>();
        Some(
            Self::pinned()?
                .objects
                .temporary_views
                .get(&reference)
                .cloned(),
        )
    }

    /// Records the temporary view that a reference names when the view is created.
    pub(super) fn record_temporary_view<T: AsRef<str>>(reference: &[T], view: &TableStatus) {
        Self::record(|objects| {
            let reference = reference.iter().map(|x| x.as_ref().to_string()).collect();
            objects.temporary_views.insert(reference, view.clone());
        });
    }

    /// Returns the function of the session that a name names while the query is resolved
    /// again.
    pub(super) fn pinned_function(name: &str) -> Option<Option<ScalarUDF>> {
        Some(Self::pinned()?.objects.functions.get(name).cloned())
    }

    /// Records the function of the session that a name names when the view is created.
    pub(super) fn record_function(name: &str, function: &ScalarUDF) {
        Self::record(|objects| {
            objects.functions.insert(name.to_string(), function.clone());
        });
    }
}
