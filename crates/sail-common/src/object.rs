use std::any::Any;
use std::hash::{Hash, Hasher};

/// A trait that facilitates deriving `PartialEq`, `Eq`, and `Hash` for `dyn` trait objects.
/// Since `DynObject` has a blanket implementation, all method names are prefixed with `dyn_object_`
/// to avoid conflicts with similar methods defined by other traits.
/// Otherwise, for example, `x.as_any()` may unintentionally call the method defined by `DynObject`
/// when `DynObject` is in the scope.
pub trait DynObject: Any {
    fn dyn_object_as_any(&self) -> &dyn Any;
    fn dyn_object_eq(&self, other: &dyn Any) -> bool;
    fn dyn_object_hash(&self, state: &mut dyn Hasher);
}

impl<T: PartialEq + Eq + Hash + 'static> DynObject for T {
    fn dyn_object_as_any(&self) -> &dyn Any {
        self
    }

    fn dyn_object_eq(&self, other: &dyn Any) -> bool {
        other.downcast_ref::<Self>().map_or(false, |x| self == x)
    }

    fn dyn_object_hash(&self, mut state: &mut dyn Hasher) {
        self.hash(&mut state)
    }
}

#[macro_export]
macro_rules! impl_dyn_object_traits {
    ($t:ident) => {
        impl PartialEq<dyn $t> for dyn $t {
            fn eq(&self, other: &dyn $t) -> bool {
                self.dyn_object_eq(DynObject::dyn_object_as_any(other))
            }
        }

        impl Eq for dyn $t {}

        impl Hash for dyn $t {
            fn hash<H: Hasher>(&self, state: &mut H) {
                self.dyn_object_hash(state)
            }
        }
    };
}
