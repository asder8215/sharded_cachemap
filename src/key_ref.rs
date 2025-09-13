use std::hash::Hash;
use std::borrow::Borrow;

#[derive(Debug, Copy, Clone)]
pub(crate) struct KeyRef<K>(pub(crate) *const K);
impl<K> PartialEq for KeyRef<K>
where K: PartialEq {
    fn eq(&self, other: &Self) -> bool {
        // SAFETY: the value K is pinned in the HashShard
        unsafe { &*self.0 }.eq(unsafe { &*other.0 })
    }
}
impl<K> Eq for KeyRef<K>
where K: Eq {}
impl<K> Hash for KeyRef<K> 
where K: Hash {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        // SAFETY: the value K is pinned in the HashShard
        unsafe { &*self.0 }.hash(state);
    }
}
impl<K> Borrow<K> for KeyRef<K> {
    fn borrow(&self) -> &K {
        // SAFETY: the value K is pinned in the HashShard
        unsafe { &*self.0 }
    } 
}
impl<K> Borrow<*const K> for KeyRef<K> {
    fn borrow(&self) -> &*const K {
        &self.0
    } 
}
impl<K> From<&K> for KeyRef<K> {
    fn from(value: &K) -> Self {
        Self(value as *const K)
    }
}