use crate::common::PutResult;
use ahash::AHasher;
use std::fmt::Debug;
use std::fmt::Write;
use std::hash::Hash;
use std::hash::RandomState;
use std::{
    borrow::Borrow,
    cell::UnsafeCell,
    hash::{DefaultHasher, Hasher},
    mem::MaybeUninit,
    sync::atomic::{AtomicUsize, Ordering},
};
use tokio::sync::Notify;

// TODO: Need to do the following
//  - add comments/docs to all of the functions
//  - clean up any code (remove any redundant code if any)
//  - add more unit tests and benchmarking examples
//  - add code for users to provide a hasher function to use

/// DHShardedCacheMap uses Double Hashing, one for choosing a shard,
/// one for choosing a slot in the shard's bounded queue
/// It performs one shot gets, inserts, and evictions at this slot
#[derive(Debug)]
pub struct DoubleHashPolicy<S = RandomState> {
    first_hash: S,
    second_hash: S,
}

// pub enum PutResult<K, V> {
//     Eviction {
//         key: K,
//         val: V,
//     },
//     Update {
//         key: K,
//         val: V,
//     },
//     Insert
// }

/// An asynchronous cache data structure that operates with
/// m hash shards and n slots (within a bounded queue) per hash shard
#[derive(Debug)]
pub struct DHShardedCacheMap<K, V, S = RandomState> {
    /// bounded array of size n
    shards: Box<[HashShard<K, V>]>,
    /// num of shards
    shard_num: usize,
    /// num of slots in each shard
    slot_num: usize,
    /// determines the eviction policy to use for evicting
    /// a key out of the pair_list
    evict_policy: Option<DoubleHashPolicy<S>>,
}

#[derive(Debug, Default)]
struct HashShard<K, V> {
    /// bound queue of key-val pairs of size m
    pair_list: Box<[Slot<K, V>]>,
}

#[derive(Debug)]
struct Slot<K, V> {
    /// the key at this slot
    key: UnsafeCell<MaybeUninit<K>>,
    /// the val at this slot
    val: UnsafeCell<MaybeUninit<V>>,
    /// 0: Unclaimed, 1: Get, 2: Put
    state: AtomicUsize,
    /// 0: Empty, 1: Occupied
    occupied: AtomicUsize,
    /// incoming getters and putters wait their turn
    /// in this notify list if it's already claimed
    wait_notif: Notify,
}

impl<K, V> Slot<K, V> {
    fn new() -> Self {
        Self {
            key: UnsafeCell::new(MaybeUninit::uninit()),
            val: UnsafeCell::new(MaybeUninit::uninit()),
            state: AtomicUsize::new(0),
            occupied: AtomicUsize::new(0),
            wait_notif: Notify::new(),
        }
    }
}

impl<K, V> HashShard<K, V> {
    fn new(slots: usize) -> Self {
        Self {
            pair_list: {
                let mut vec = Vec::with_capacity(slots);
                for _ in 0..slots {
                    vec.push(Slot::new())
                }
                vec.into_boxed_slice()
            },
        }
    }
}

impl<K, V, S> DHShardedCacheMap<K, V, S> {
    /// Instatiates the DHShardedCacheMap object with non-zero user requested number of shards
    /// and slots (default 8 slots if none is provided) and optionally two hasher functions
    /// provided to determine which shard and slot to select
    pub fn new(
        shards: usize,
        slots: Option<usize>,
        evict_policy: Option<DoubleHashPolicy<S>>,
    ) -> Self {
        assert!(
            shards > 0,
            "The number of requested shards must be positive"
        );
        let slot_num = if let Some(slots) = slots {
            assert!(slots > 0, "The number of requested slots must be positive");
            slots
        } else {
            8
        };
        Self {
            shards: {
                let mut vec = Vec::with_capacity(shards);
                for _ in 0..shards {
                    vec.push(HashShard::new(slot_num));
                }
                vec.into_boxed_slice()
            },
            shard_num: shards,
            slot_num,
            evict_policy,
        }
    }

    pub fn print_cache(&self)
    where
        K: Debug,
        V: Debug,
    {
        let mut print_buffer = String::new();
        write!(print_buffer, "[").unwrap();
        writeln!(print_buffer).unwrap();
        for i in 0..self.shard_num {
            let shard = &self.shards[i];
            write!(print_buffer, "  [").unwrap();
            for j in 0..self.slot_num {
                let occupied = shard.pair_list[j].occupied.load(Ordering::Relaxed);
                if occupied != 0 {
                    let key_ref = unsafe { (*shard.pair_list[j].key.get()).assume_init_ref() };
                    let val_ref = unsafe { (*shard.pair_list[j].val.get()).assume_init_ref() };
                    if j == self.slot_num - 1 {
                        write!(print_buffer, "({key_ref:?}, {val_ref:?})").unwrap();
                    } else {
                        write!(print_buffer, "({key_ref:?}, {val_ref:?},) ").unwrap();
                    }
                } else if j == self.slot_num - 1 {
                    write!(print_buffer, "<uninit>").unwrap();
                } else {
                    write!(print_buffer, "<uninit>, ").unwrap();
                }
            }
            write!(print_buffer, "]").unwrap();
            writeln!(print_buffer).unwrap();
        }
        write!(print_buffer, "]").unwrap();
        writeln!(print_buffer).unwrap();
        print!("{print_buffer}");
    }
}

impl<K, V, S> DHShardedCacheMap<K, V, S>
where
    K: Hash + Eq + PartialEq,
{
    #[inline]
    fn first_hash_key<T>(&self, key: &T) -> u64
    where
        T: Hash + ?Sized,
    {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        hasher.finish()
    }

    #[inline]
    fn second_hash_key<T>(&self, key: &T) -> u64
    where
        T: Hash + ?Sized,
    {
        let mut hasher = AHasher::default();
        key.hash(&mut hasher);
        hasher.finish()
    }

    #[inline(always)]
    pub fn get_num_of_shards(&self) -> usize {
        self.shard_num
    }

    #[inline(always)]
    pub fn get_num_of_slots(&self) -> usize {
        self.slot_num
    }

    fn get_work<Q>(&self, key: &Q, hash_shard: &HashShard<K, V>, slot_ind: usize) -> Option<&V>
    where
        K: Borrow<Q>,
        Q: ?Sized + Eq,
    {
        if hash_shard.pair_list[slot_ind]
            .occupied
            .load(Ordering::Relaxed)
            == 0
        {
            return None;
        }
        // SAFETY: enq_counter in hash_shard is guaranteed to tell us how many
        // initialized items are in this shard
        // Moreover, we give references to the key-val pair in the slot
        // because the user should not own the key-val pair on get()
        let (k, v) = unsafe {
            (
                (*hash_shard.pair_list[slot_ind].key.get()).assume_init_ref(),
                (*hash_shard.pair_list[slot_ind].val.get()).assume_init_ref(),
            )
        };
        if k.borrow() == key {
            return Some(v);
        }
        None
    }

    pub async fn get<Q>(&self, key: &Q) -> Option<&V>
    where
        K: Borrow<Q>,
        Q: ?Sized + Hash + Eq,
    {
        let hash_shard_ind = (self.first_hash_key(key) % self.get_num_of_shards() as u64) as usize;
        let hash_slot_ind = (self.second_hash_key(key) % self.get_num_of_slots() as u64) as usize;
        let hash_shard = &self.shards[hash_shard_ind];

        loop {
            match hash_shard.pair_list[hash_slot_ind].state.compare_exchange(
                0,
                1,
                Ordering::Release,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    let item = self.get_work(key, hash_shard, hash_slot_ind);
                    hash_shard.pair_list[hash_slot_ind]
                        .state
                        .store(0, Ordering::Release);
                    hash_shard.pair_list[hash_slot_ind].wait_notif.notify_one();
                    return item;
                }
                Err(_) => {
                    hash_shard.pair_list[hash_slot_ind]
                        .wait_notif
                        .notified()
                        .await;
                }
            }
        }
    }

    fn put_work(&self, key: K, val: V, hash_shard_ind: usize, slot_ind: usize) -> PutResult<K, V>
    where
        K: Debug,
    {
        let hash_shard = &self.shards[hash_shard_ind];

        // occupied tells us if this cache slot was full or empty prior to claiming
        // it
        if hash_shard.pair_list[slot_ind]
            .occupied
            .load(Ordering::Relaxed)
            == 0
        {
            // SAFETY: MaybeUninit hasn't been initialized here, so it's completely
            // okay to write to this spot (no worry about data that hasn't been dropped)
            unsafe { (*hash_shard.pair_list[slot_ind].key.get()).write(key) };
            unsafe { (*hash_shard.pair_list[slot_ind].val.get()).write(val) };
            hash_shard.pair_list[slot_ind]
                .occupied
                .store(1, Ordering::Release);
            PutResult::Insert
        } else {
            let slot_key = unsafe { &*hash_shard.pair_list[slot_ind].key.get() };
            let stored_key = unsafe { &*slot_key.as_ptr() };
            if key == *stored_key {
                // SAFETY: since V is copied in old_val and dropping it is user's responsibility
                // we can safely override the value inside this MaybeUninit
                let old_val =
                    unsafe { (*hash_shard.pair_list[slot_ind].val.get()).assume_init_read() };
                unsafe { (*hash_shard.pair_list[slot_ind].val.get()).write(val) };
                PutResult::Update {
                    key,
                    val: old_val,
                }
            } else {
                // SAFETY: On eviction, it must be the case that the key-val pair must be overrided with whatever
                // key-val pair was provided by the user. The previous key-val pair stored will be returned and owned
                // by the user (user is responsible for dropping this owned data as a result)
                let slot = unsafe {
                    (
                        (*hash_shard.pair_list[slot_ind].key.get()).assume_init_read(),
                        (*hash_shard.pair_list[slot_ind].val.get()).assume_init_read(),
                    )
                };
                unsafe { (*hash_shard.pair_list[slot_ind].key.get()).write(key) };
                unsafe { (*hash_shard.pair_list[slot_ind].val.get()).write(val) };
                PutResult::Eviction {
                    key: slot.0,
                    val: slot.1,
                }
            }
        }
    }

    pub async fn put(&self, key: K, val: V) -> PutResult<K, V>
    where
        K: Debug,
    {
        let hash_shard_ind = (self.first_hash_key(&key) % self.get_num_of_shards() as u64) as usize;
        let hash_slot_ind = (self.second_hash_key(&key) % self.get_num_of_slots() as u64) as usize;
        let hash_shard = &self.shards[hash_shard_ind];

        loop {
            match hash_shard.pair_list[hash_slot_ind].state.compare_exchange(
                0,
                2,
                Ordering::Release,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    let item = self.put_work(key, val, hash_shard_ind, hash_slot_ind);
                    hash_shard.pair_list[hash_slot_ind]
                        .state
                        .store(0, Ordering::Release);
                    hash_shard.pair_list[hash_slot_ind].wait_notif.notify_one();
                    return item;
                }
                Err(_) => {
                    hash_shard.pair_list[hash_slot_ind]
                        .wait_notif
                        .notified()
                        .await;
                }
            }
        }
    }
}

// SAFETY: with all the safety regards mentioned put and get
// method, it should be sync and send safe
unsafe impl<K, V> Sync for Slot<K, V> {}
unsafe impl<K, V> Send for Slot<K, V> {}

// Destructor trait created for HashShard<T> to clean up
// memory allocated and initialized onto Slot<K, V> when
// ShardedCacheMap<K, V> goes out of scope
impl<K, V> Drop for HashShard<K, V> {
    fn drop(&mut self) {
        for slot in &self.pair_list {
            // SAFETY: the occupied status informs us for sure if there
            // is an item here at this slot which we can safely drop
            if slot.occupied.load(Ordering::Relaxed) == 1 {
                let key = slot.key.get();
                let val = slot.val.get();
                unsafe {
                    (*key).assume_init_drop();
                    (*val).assume_init_drop();
                }
                slot.occupied.store(1, Ordering::Relaxed);
            }
        }
    }
}
