use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::Notify;

/// This is a guard for the put request to drop
/// the writer bit on cancellation 
/// NOTE: This doesn't work unfortunately :(
/// because you can't hold a guard safely across
/// await points
#[allow(dead_code)]
pub(crate) struct PutGuard<'a> {
    state: &'a AtomicUsize,
    put_notify: &'a Notify,
    get_notify: &'a Notify,
}

impl<'a> PutGuard<'a> {
    #[inline(always)]
    #[allow(dead_code)]
    pub(crate) fn create_guard(
        state: &'a AtomicUsize,
        put_notify: &'a Notify,
        get_notify: &'a Notify,
    ) -> Self {
        Self {
            state,
            put_notify,
            get_notify,
        }
    }
}

impl Drop for PutGuard<'_> {
    #[inline(always)]
    fn drop(&mut self) {
        // bit 63 checked for whether write is set
        const WRITER_BIT: usize = 1 << 63;
        // bits 62-0 are used for reader count
        const READER_MASK: usize = !WRITER_BIT;
        let mut curr_state = self.state.load(Ordering::Acquire);
        loop {
            let drop_state = curr_state & READER_MASK;
            match self.state.compare_exchange_weak(
                curr_state,
                drop_state,
                Ordering::Release,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(act_state) => {
                    curr_state = act_state;
                }
            }
        }
        println!("Dropping!");
        self.get_notify.notify_waiters();
        self.get_notify.notify_one();
        self.put_notify.notify_one();
    }
}
