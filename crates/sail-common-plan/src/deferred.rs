use std::fmt::{self, Debug};

use datafusion_common::{Result as PlanResult, plan_datafusion_err};

/// A value whose construction can be requested before its dependencies are known.
pub trait Resolvable {
    type Request;
}

/// Unresolved is a request, not a missing value. In particular, a resolved Arrow
/// `Null` type is different from a type which has not been inferred yet.
pub enum Deferred<T: Resolvable> {
    Unresolved(Box<T::Request>),
    Resolved(T),
}

impl<T: Resolvable> Deferred<T> {
    pub fn unresolved(request: T::Request) -> Self {
        Self::Unresolved(Box::new(request))
    }

    pub fn resolved(&self, context: &str) -> PlanResult<&T> {
        match self {
            Self::Resolved(value) => Ok(value),
            Self::Unresolved(_) => Err(plan_datafusion_err!(
                "unresolved {context} at the plan analysis boundary"
            )),
        }
    }

    pub fn into_resolved(self, context: &str) -> PlanResult<T> {
        match self {
            Self::Resolved(value) => Ok(value),
            Self::Unresolved(_) => Err(plan_datafusion_err!(
                "unresolved {context} at the plan analysis boundary"
            )),
        }
    }
}

impl<T> Clone for Deferred<T>
where
    T: Resolvable + Clone,
    T::Request: Clone,
{
    fn clone(&self) -> Self {
        match self {
            Self::Unresolved(request) => Self::Unresolved(request.clone()),
            Self::Resolved(value) => Self::Resolved(value.clone()),
        }
    }
}

impl<T> Debug for Deferred<T>
where
    T: Resolvable + Debug,
    T::Request: Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unresolved(request) => f.debug_tuple("Unresolved").field(request).finish(),
            Self::Resolved(value) => f.debug_tuple("Resolved").field(value).finish(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Clone, PartialEq)]
    struct Value(Option<i32>);

    impl Resolvable for Value {
        type Request = String;
    }

    #[test]
    fn unresolved_is_not_a_resolved_null() -> PlanResult<()> {
        let missing = Deferred::<Value>::unresolved("input".into());
        assert!(missing.into_resolved("expression type").is_err());
        let null = Deferred::Resolved(Value(None));
        assert_eq!(null.into_resolved("expression type")?, Value(None));
        Ok(())
    }
}
