use std::any::Any;
use std::cmp::Ordering;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

/// Compare equality-only values without treating unequal values as equal.
pub fn partial_cmp_by_equality<T: PartialEq>(left: &T, right: &T) -> Option<Ordering> {
    left.eq(right).then_some(Ordering::Equal)
}

/// Opaque providers have instance identity; clones retain the same identity.
pub fn arc_ptr_eq<T: ?Sized>(left: &Arc<T>, right: &Arc<T>) -> bool {
    Arc::ptr_eq(left, right)
}

pub fn arc_ptr_hash<T: ?Sized, H: Hasher>(value: &Arc<T>, state: &mut H) {
    (Arc::as_ptr(value) as *const ()).hash(state);
}

pub fn arc_ptr_partial_cmp<T: ?Sized>(left: &Arc<T>, right: &Arc<T>) -> Option<Ordering> {
    (Arc::as_ptr(left) as *const ()).partial_cmp(&(Arc::as_ptr(right) as *const ()))
}

/// A trait that facilitates deriving `PartialEq`, `Eq`, `Hash` and 'PartialOrd' for `dyn` trait objects.
/// Since `DynObject` has a blanket implementation, all method names are prefixed with `dyn_object_`
/// to avoid conflicts with similar methods defined by other traits.
pub trait DynObject: Any {
    fn dyn_object_eq(&self, other: &dyn Any) -> bool;
    fn dyn_object_hash(&self, state: &mut dyn Hasher);
    fn dyn_object_partial_cmp(&self, other: &dyn Any) -> Option<Ordering>;
}

impl<T: PartialEq + Eq + Hash + PartialOrd + 'static> DynObject for T {
    fn dyn_object_eq(&self, other: &dyn Any) -> bool {
        other.downcast_ref::<Self>() == Some(self)
    }

    fn dyn_object_hash(&self, mut state: &mut dyn Hasher) {
        self.hash(&mut state)
    }

    fn dyn_object_partial_cmp(&self, other: &dyn Any) -> Option<Ordering> {
        other
            .downcast_ref::<Self>()
            .and_then(|x| self.partial_cmp(x))
    }
}

#[macro_export]
macro_rules! impl_dyn_object_traits {
    ($t:ident) => {
        impl PartialEq<dyn $t> for dyn $t {
            fn eq(&self, other: &dyn $t) -> bool {
                self.dyn_object_eq(other)
            }
        }

        impl Eq for dyn $t {}

        impl Hash for dyn $t {
            fn hash<H: Hasher>(&self, state: &mut H) {
                self.dyn_object_hash(state)
            }
        }

        impl PartialOrd<dyn $t> for dyn $t {
            fn partial_cmp(&self, other: &dyn $t) -> Option<Ordering> {
                self.dyn_object_partial_cmp(other)
            }
        }
    };
}
