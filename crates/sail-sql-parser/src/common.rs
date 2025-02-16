/// A sequence of item type `T` and separator type `S`.
#[derive(Debug, Clone)]
pub struct Sequence<T, S> {
    pub head: Box<T>,
    pub tail: Vec<(S, T)>,
}

impl<T, S> Sequence<T, S> {
    pub fn items(&self) -> impl Iterator<Item = &T> {
        std::iter::once(&*self.head).chain(self.tail.iter().map(|(_, item)| item))
    }

    pub fn into_items(self) -> impl Iterator<Item = T> {
        std::iter::once(*self.head).chain(self.tail.into_iter().map(|(_, item)| item))
    }
}
