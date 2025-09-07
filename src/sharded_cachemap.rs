use crate::PutResult;
use std::fmt::Debug;
use std::fmt::Write;
use std::hash::Hash;
use std::thread::sleep;
use std::time::Duration;
use std::{
    borrow::Borrow,
    cell::UnsafeCell,
    hash::{DefaultHasher, Hasher},
    mem::MaybeUninit,
    sync::atomic::{AtomicUsize, Ordering},
};
use tokio::sync::Notify;
// use crate::put_guard::PutGuard;

// TODO: Need to do the following
//  - add comments/docs to all of the functions
//  - clean up any code (remove any redundant code if any)
//  - add more unit tests and benchmarking examples

// bit 63 checked for whether putter bit is set
const PUTTER_BIT: usize = 1 << 63;
// bits 62-0 are used for getter count
const GETTER_MASK: usize = !PUTTER_BIT;

// This here below is just strategy notes on how this custom rwlock will work with atomics and notifs:
//
// What does getter have to check to increment state? state & WRITER_BIT == 0 (incoming get may not be added
// while put bit is set)
//
// What does putter have to check to set put bit in state? state & WRITER_BIT == 0 (only one putter allowed)
//
// So we have state like this: 1...1111. Getters claimed this spot before putter; what should happen?
// Putter must wait (place itself in a priority put notif) until getters are done. Getters must notify
// priority putter that it's done when state == WRITER_BIT. Putter on wake up will check firstly if there
// are for sure no getters working by checking if state & READER_MASK == 0, does its work and then notify
// all getters through notify_waiter(), then another time with notify_one() (since there's possibly a very,
// very small window for a getter to not register itself to the notify list when it sees a putter bit is set
// so we add a permit just in case), and then wake up one putter (since only one putter can really perform
// work at a time)
//
// Assume state is 0 (no getters or putters have set anything)
// If getter views this state ->
//      - loop compare exchange state to be state + 1 with Release publishing order, Acquire failure
// If putter views this state
//      - loop compare exchange state to be state | WRITER_BIT with Release publishing order, Acquire failure
// Incoming getters and putters will go into their respective getter and putter notify list (note not priority putter
// but a separate notify list)

/// Eviction for ShardedCacheMap is done on a Hash Shard level basis
/// meaning when the hash_shard itself is full and another key wants
/// to put itself into that specific shard, eviction will be prompted in
/// FIFO or LIFO order of the key-val pair queue.
#[derive(Debug, Clone, Copy)]
pub enum EvictionPolicy {
    LIFO,
    FIFO,
}

/// An asynchronous cache data structure that operates with
/// m hash shards and n slots (within a bounded queue) per hash shard
#[derive(Debug)]
pub struct ShardedCacheMap<K, V> {
    /// bounded array of size n
    shards: Box<[HashShard<K, V>]>,
    /// num of shards
    shard_num: usize,
    /// num of slots in each shard
    slot_num: usize,
    /// determines the eviction policy to use for evicting
    /// a key out of the pair_list
    evict_policy: EvictionPolicy,
}

#[derive(Debug, Default)]
struct HashShard<K, V> {
    /// bound queue of key-val pairs of size m
    pair_list: Box<[Slot<K, V>]>,
    /// puts are registered to this notify list
    put_notify: Notify,
    /// notif for the first put to set the bit 63 in state
    priority_put_notify: Notify,
    /// gets are registered to this notify list
    get_notify: Notify,
    /// bit 63: put count, 62-0: get count (acts like a rwlock)
    state: AtomicUsize,
    /// index to evict at if the queue is full
    evict_index: AtomicUsize,
    /// a counter informing about how many items have been
    /// initialized in this pair list (only put modifies)
    enq_counter: AtomicUsize,
}

#[derive(Debug)]
struct Slot<K, V> {
    /// the key at this slot
    key: UnsafeCell<MaybeUninit<K>>,
    /// the val at this slot
    val: UnsafeCell<MaybeUninit<V>>,
}

impl<K, V> Slot<K, V> {
    fn new() -> Self {
        Self {
            key: UnsafeCell::new(MaybeUninit::uninit()),
            val: UnsafeCell::new(MaybeUninit::uninit()),
        }
    }
}

impl<K, V> HashShard<K, V> {
    fn new(slots: usize, evict_policy: EvictionPolicy) -> Self {
        Self {
            pair_list: {
                let mut vec = Vec::with_capacity(slots);
                for _ in 0..slots {
                    vec.push(Slot::new())
                }
                vec.into_boxed_slice()
            },
            put_notify: Notify::new(),
            priority_put_notify: Notify::new(),
            get_notify: Notify::new(),
            state: AtomicUsize::new(0),
            evict_index: match evict_policy {
                EvictionPolicy::FIFO => AtomicUsize::new(0),
                EvictionPolicy::LIFO => AtomicUsize::new(slots - 1),
            },
            enq_counter: AtomicUsize::new(0),
        }
    }
}

impl<K, V> ShardedCacheMap<K, V> {
    /// Instatiates the ShardedCacheMap object with non-zero user requested number of shards
    /// and slots (default 8 slots if none is provided) and an eviction policy to follow (FIFO,
    /// LIFO)
    pub fn new(shards: usize, slots: Option<usize>, evict_policy: EvictionPolicy) -> Self {
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
                    vec.push(HashShard::new(slot_num, evict_policy));
                }
                vec.into_boxed_slice()
            },
            shard_num: shards,
            slot_num: slot_num,
            evict_policy: evict_policy,
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
            let enq_ctr = shard.enq_counter.load(Ordering::Relaxed);
            write!(print_buffer, "  [").unwrap();
            for j in 0..self.slot_num {
                if j < enq_ctr {
                    let key_ref = unsafe { (*shard.pair_list[j].key.get()).assume_init_ref() };
                    let val_ref = unsafe { (*shard.pair_list[j].val.get()).assume_init_ref() };
                    if j == self.slot_num - 1 {
                        write!(print_buffer, "({key_ref:?}, {val_ref:?})").unwrap();
                    } else {
                        write!(print_buffer, "({key_ref:?}, {val_ref:?},) ").unwrap();
                    }
                } else {
                    if j == self.slot_num - 1 {
                        write!(print_buffer, "<uninit>").unwrap();
                    } else {
                        write!(print_buffer, "<uninit>, ").unwrap();
                    }
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

impl<K, V> ShardedCacheMap<K, V>
where
    K: Hash + Eq + PartialEq,
{
    #[inline]
    fn hash_key<T>(&self, key: &T) -> u64
    where
        T: Hash + ?Sized,
    {
        let mut hasher = DefaultHasher::new();
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

    fn get_work<Q>(&self, key: &Q, hash_shard: &HashShard<K, V>) -> Option<&V>
    where
        K: Borrow<Q>,
        Q: ?Sized + Eq,
    {
        for i in 0..hash_shard.enq_counter.load(Ordering::Relaxed) {
            // SAFETY: enq_counter in hash_shard is guaranteed to tell us how many
            // initialized items are in this shard
            // Moreover, we give references to the key-val pair in the slot
            // because the user should not own the key-val pair on get()
            let (k, v) = unsafe {
                (
                    (*hash_shard.pair_list[i].key.get()).assume_init_ref(),
                    (*hash_shard.pair_list[i].val.get()).assume_init_ref(),
                )
            };
            if k.borrow() == key {
                return Some(v);
            }
        }
        return None;
    }

    pub async fn get<Q>(&self, key: &Q) -> Option<&V>
    where
        K: Borrow<Q>,
        Q: ?Sized + Hash + Eq,
    {
        let hash_shard_ind = (self.hash_key(key) % self.get_num_of_shards() as u64) as usize;
        let hash_shard = &self.shards[hash_shard_ind];

        let mut state = hash_shard.state.load(Ordering::Acquire);

        loop {
            // if the writer bit has not been set yet
            if state & PUTTER_BIT == 0 {
                // initially I thought getters should fetch_add BUT
                // compare exchange must occur for getters because
                // getters must observe the put bit being set here
                // (no incoming getters must be allowed when a
                // put bit is set!)
                match hash_shard.state.compare_exchange(
                    state,
                    state + 1,
                    Ordering::Release,
                    Ordering::Acquire,
                ) {
                    Ok(_) => {
                        let val = self.get_work(key, hash_shard);
                        let end_state = hash_shard.state.fetch_sub(1, Ordering::AcqRel) - 1;
                        if end_state == PUTTER_BIT {
                            hash_shard.priority_put_notify.notify_one();
                        }
                        return val;
                    }
                    Err(act_state) => {
                        let check_state = act_state & PUTTER_BIT;
                        // update state with the actual state
                        if check_state == 0 {
                            state = act_state;
                        }
                        // a writer has claimed the spot to write into this hash_shard
                        else {
                            hash_shard.get_notify.notified().await;
                            state = hash_shard.state.load(Ordering::Acquire);
                        }
                    }
                }

                // I thought about doing this initially, but this would allow
                // for more getters to come in when a putter bit is set
                // hash_shard.state.fetch_add(1, Ordering::SeqCst);
                // let val = self.get_work(key, hash_shard);
                // let end_state = hash_shard.state.fetch_sub(1,Ordering::Release);

                // if end_state == WRITER_BIT {
                //     hash_shard.priority_put_notify.notify_one();
                // }
                // return val;
            } else {
                hash_shard.get_notify.notified().await;
                state = hash_shard.state.load(Ordering::Acquire);
            }
        }
    }

    fn put_work(
        &self,
        key: K,
        val: V,
        num_slots: usize,
        evict_policy: EvictionPolicy,
        hash_shard_ind: usize,
        // ) -> Option<(K, V)>
    ) -> PutResult<K, V>
    where
        K: Debug,
    {
        let hash_shard = &self.shards[hash_shard_ind];
        let num_items = hash_shard.enq_counter.load(Ordering::Relaxed);
        for i in 0..num_items {
            // SAFETY: because num_items tells us how many key-val were initialized
            // it's guaranteed that there is a key here
            let slot_key = unsafe { &*hash_shard.pair_list[i].key.get() };
            let stored_key = unsafe { &*slot_key.as_ptr() };

            // compare to see if the provided user key is equal to stored key
            if key == *stored_key {
                // SAFETY: num_items safely tells us how many key-val pairs were initialized
                // and because the value will be replaced here, the user can
                // receive an owned version of V (the dropping of V will occur on
                // user's end), so it's okay to get a duplicate copy here
                let old_val = unsafe { (*hash_shard.pair_list[i].val.get()).assume_init_read() };

                // SAFETY: since V is copied in old_val and dropping it is user's responsibility
                // we can safely override the value inside this MaybeUninit
                unsafe { (*hash_shard.pair_list[i].val.get()).write(val) };

                return PutResult::Update {
                    key: key,
                    val: old_val,
                };
            }
        }

        if num_items != num_slots {
            // SAFETY: MaybeUninit hasn't been initialized here, so it's completely
            // okay to write to this spot (no worry about data that hasn't been dropped)
            unsafe { (*hash_shard.pair_list[num_items].key.get()).write(key) };
            unsafe { (*hash_shard.pair_list[num_items].val.get()).write(val) };
            hash_shard.enq_counter.fetch_add(1, Ordering::Relaxed);
        } else {
            let evict_ind: usize;
            match evict_policy {
                EvictionPolicy::LIFO => {
                    evict_ind = hash_shard.evict_index.fetch_sub(1, Ordering::Relaxed) % num_slots;
                }
                EvictionPolicy::FIFO => {
                    evict_ind = hash_shard.evict_index.fetch_add(1, Ordering::Relaxed) % num_slots;
                }
            }

            // SAFETY: On eviction, it must be the case that the key-val pair must be overrided with whatever
            // key-val pair was provided by the user. The previous key-val pair stored will be returned and owned
            // by the user (user is responsible for dropping this owned data as a result)
            let slot = unsafe {
                (
                    (*hash_shard.pair_list[evict_ind].key.get()).assume_init_read(),
                    (*hash_shard.pair_list[evict_ind].val.get()).assume_init_read(),
                )
            };
            unsafe { (*hash_shard.pair_list[evict_ind].key.get()).write(key) };
            unsafe { (*hash_shard.pair_list[evict_ind].val.get()).write(val) };
            return PutResult::Eviction {
                key: slot.0,
                val: slot.1,
            };
        }
        return PutResult::Insert;
    }

    pub async fn put(&self, key: K, val: V) -> PutResult<K, V>
    where
        K: Debug,
    {
        let hash_shard_ind = (self.hash_key(&key) % self.get_num_of_shards() as u64) as usize;
        let num_of_slots = self.get_num_of_slots();
        let evict_policy = self.evict_policy;
        let hash_shard = &self.shards[hash_shard_ind];

        let mut state = hash_shard.state.load(Ordering::Acquire);

        loop {
            // if the writer bit has not been set yet
            if state & PUTTER_BIT == 0 {
                // we are trying to set the writer bit right here
                let mut new_state = state | PUTTER_BIT;
                match hash_shard.state.compare_exchange(
                    state,
                    new_state,
                    Ordering::Release,
                    Ordering::Acquire,
                ) {
                    Ok(_) => {
                        // this put guard does not work to
                        // set the put bit to 0 for async :(
                        // let _ = PutGuard::create_guard(
                        //     &hash_shard.state,
                        //     &hash_shard.put_notify,
                        //     &hash_shard.get_notify,
                        // );
                        let mut sleep_time = 1;
                        loop {
                            // are there no getters working right now?
                            // if so we can proceed with doing our work
                            if new_state & GETTER_MASK == 0 {
                                let kv = self.put_work(
                                    key,
                                    val,
                                    num_of_slots,
                                    evict_policy,
                                    hash_shard_ind,
                                );

                                // set put bit to 0
                                hash_shard.state.fetch_and(GETTER_MASK, Ordering::Release);

                                // notify any waiting getters to get up
                                hash_shard.get_notify.notify_waiters();
                                // just in case an incoming getter sees that the put bit
                                // is 1 but didn't register itself to the Notify list on time
                                // add a permit to the Notify list
                                hash_shard.get_notify.notify_one();
                                // add a permit to wake up any waiting putters just in case
                                // (since only one can claim the put bit, it makes sense to
                                // just use notify_one() rather than notify_waiters())
                                hash_shard.put_notify.notify_one();
                                return kv;
                            }
                            // getters are working rn, sleep and wait until
                            // they are done
                            else {
                                // Cancel Safety issues: You can't safely use this notified.await
                                // across the put bit being set to 1 since on cancellation
                                // the put bit will not be set to 0 (and using a PutGuard
                                // as I had initially intended does not work for async)
                                // Until a good async guard or async drop is created, the
                                // priority put will stick to working in a loop
                                // hash_shard.priority_put_notify.notified().await;

                                // thread sleeps on failure to prevent high CPU usage
                                // max sleep time is stops at 100 ns
                                sleep(Duration::from_nanos(sleep_time));
                                new_state = hash_shard.state.load(Ordering::Acquire);
                                sleep_time = std::cmp::min(100, sleep_time << 1);
                            }
                        }
                    }
                    Err(act_state) => {
                        let check_state = act_state & PUTTER_BIT;
                        // if the writer bit has not been set yet, then
                        // we should try again to set it!
                        if check_state == 0 {
                            state = act_state;
                        }
                        // otherwise it's been claimed by another putter,
                        // so we need to wait our turn in the notify list
                        else {
                            hash_shard.put_notify.notified().await;
                            state = hash_shard.state.load(Ordering::Acquire);
                        }
                    }
                }
            } else {
                hash_shard.put_notify.notified().await;
                state = hash_shard.state.load(Ordering::Acquire);
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
        let items = self.enq_counter.load(Ordering::Relaxed);
        // SAFETY: the enq_counter informs us for sure if there
        // is an item here at this slot which we can safely drop
        for i in 0..items {
            let key = self.pair_list[i].key.get();
            let val = self.pair_list[i].val.get();
            unsafe {
                (*key).assume_init_drop();
                (*val).assume_init_drop();
            }
        }
    }
}
