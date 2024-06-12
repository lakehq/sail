use std::any::Any;
use std::hash::{Hash, Hasher};

/// A trait that facilitates deriving `PartialEq`, `Eq`, and `Hash` for `dyn` trait objects.
pub trait DynObject: Any {
    fn as_any(&self) -> &dyn Any;
    fn dyn_eq(&self, other: &dyn Any) -> bool;
    fn dyn_hash(&self, state: &mut dyn Hasher);
}

impl<T: PartialEq + Eq + Hash + 'static> DynObject for T {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn dyn_eq(&self, other: &dyn Any) -> bool {
        other.downcast_ref::<Self>().map_or(false, |x| self == x)
    }

    fn dyn_hash(&self, mut state: &mut dyn Hasher) {
        self.hash(&mut state)
    }
}

#[macro_export]
macro_rules! impl_dyn_object_traits {
    ($t:ident) => {
        impl PartialEq<dyn $t> for dyn $t {
            fn eq(&self, other: &dyn $t) -> bool {
                self.dyn_eq(DynObject::as_any(other))
            }
        }

        impl Eq for dyn $t {}

        impl Hash for dyn $t {
            fn hash<H: Hasher>(&self, state: &mut H) {
                self.dyn_hash(state)
            }
        }
    };
}
