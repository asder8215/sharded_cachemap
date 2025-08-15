use std::hash::Hash;
use std::{
    cell::Cell,
    collections::{HashMap, LinkedList},
    fmt::Debug,
};

// #[derive(Debug, Hash, Eq, PartialEq)]
// struct Key<K> {
//     key: K
// }

#[derive(Debug)]
struct KeyFreqNode<K, V> {
    key: K,
    val: V,
    freq: Cell<usize>,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct KeyFreqPtr<K, V>(pub *mut KeyFreqNode<K, V>);

#[derive(Debug)]
pub struct LRUCache<K, V> {
    cache: HashMap<K, (V, KeyFreqPtr<K, V>)>,
    freq_list: LinkedList<KeyFreqNode<K, V>>,
    capacity: usize,
}

// impl<K> Key<K> {
//     pub(crate) fn new(key: K) -> Self {
//         Self {
//             key
//         }
//     }
// }

impl<K, V> KeyFreqNode<K, V> {
    pub(crate) fn new(key: K, val: V) -> Self {
        Self {
            key,
            val,
            freq: Cell::new(1),
        }
    }
}

impl<K, V> KeyFreqPtr<K, V> {
    pub(crate) fn new(key_freq: &mut KeyFreqNode<K, V>) -> Self {
        Self(key_freq as *mut KeyFreqNode<K, V>)
    }
}

impl<K: Eq + Hash + Copy, V: Copy> LRUCache<K, V> {
    pub fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "Capacity must be positive");
        Self {
            cache: HashMap::with_capacity(capacity),
            freq_list: LinkedList::new(),
            capacity: 0,
        }
    }

    fn sort_list(&mut self) {
        let mut vec_list = Vec::with_capacity(self.freq_list.len());

        // I have the Key Frequency Linked List as a vector now
        for list_node in &self.freq_list {
            vec_list.push(list_node);
        }
    }

    pub fn put(&mut self, key: K, val: V) -> Option<V> {
        if let Some(value) = self.cache.get(&key) {
            unsafe {
                (*(value.1.0))
                    .freq
                    .set((*(value.1.0)).freq.get().saturating_add(1));
            }
            let item = self.cache.insert(key, (val, value.1.clone()));

            Some(item.unwrap().0)
        } else {
            if self.cache.len() == self.capacity {
                let back = self.freq_list.pop_back().unwrap();
                self.cache.remove(&back.key);
                self.freq_list.push_front(KeyFreqNode::new(key, val));
                let front = KeyFreqPtr::new(self.freq_list.front_mut().unwrap());
                self.cache.insert(key, (val, front));
                Some(back.val)
            } else {
                self.freq_list.push_front(KeyFreqNode::new(key, val));
                let front = KeyFreqPtr::new(self.freq_list.front_mut().unwrap());
                self.cache.insert(key, (val, front));
                None
            }
        }
        // todo!()
    }

    pub fn get(&self, key: K) -> Option<V> {
        if let Some(item) = self.cache.get(&key) {
            unsafe {
                (*(item.1.0))
                    .freq
                    .set((*(item.1.0)).freq.get().saturating_add(1));
            }
            return Some(item.0);
        }
        None
    }

    pub fn print_freq_list(&self)
    where
        K: Debug,
        V: Debug,
    {
        println!("{:?}", self.freq_list);
    }

    pub fn print_cache(&self)
    where
        K: Debug,
        V: Debug,
    {
    }
}
