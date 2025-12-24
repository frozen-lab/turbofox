use crate::{error::InternalResult, linux::MMap};
use std::{
    marker::PhantomData,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    thread::{self, JoinHandle},
    time::Duration,
};

#[derive(Debug)]
pub(crate) struct TurboMMapView<'a, T> {
    ptr: *mut T,
    _pd: PhantomData<&'a T>,
}

impl<'a, T> TurboMMapView<'a, T> {
    #[inline]
    const fn new(ptr: *mut T) -> Self {
        Self { ptr, _pd: PhantomData }
    }

    #[inline]
    pub(crate) fn get(&self) -> &T {
        unsafe { &*self.ptr }
    }

    #[inline]
    pub(crate) fn update(&self, f: impl FnOnce(&mut T)) {
        unsafe { f(&mut *self.ptr) }
    }
}

#[derive(Debug)]
struct FlushState {
    write_epoch: AtomicU64,
    stop_signal: AtomicBool,
}

impl Default for FlushState {
    fn default() -> Self {
        Self {
            write_epoch: AtomicU64::new(0),
            stop_signal: AtomicBool::new(false),
        }
    }
}

#[derive(Debug)]
pub(crate) struct TurboMMap {
    #[cfg(target_os = "linux")]
    mmap: Arc<crate::linux::MMap>,

    f_st: Arc<FlushState>,
    f_tx: Option<JoinHandle<()>>,
}

impl TurboMMap {
    #[inline]
    pub(crate) fn new(fd: i32, len: usize, off: usize) -> InternalResult<Self> {
        #[cfg(not(target_os = "linux"))]
        unimplemented!();

        let mmap = Arc::new(unsafe { crate::linux::MMap::map(fd, len, off) }?);
        let f_st = Arc::new(FlushState::default());

        let tx_mmap = mmap.clone();
        let tx_state = f_st.clone();

        let tx = Self::spawn_tx(tx_mmap, tx_state);

        Ok(Self {
            mmap,
            f_st,
            f_tx: Some(tx),
        })
    }

    #[inline]
    pub(crate) fn len(&self) -> usize {
        #[cfg(target_os = "linux")]
        return self.mmap.len();

        #[cfg(not(target_os = "linux"))]
        unimplemented!()
    }

    #[inline]
    pub(crate) fn view<'a, T>(&self, off: usize) -> TurboMMapView<'a, T> {
        #[cfg(target_os = "linux")]
        unsafe {
            let ptr = self.mmap.read::<T>(off);
            TurboMMapView::new(ptr)
        }

        #[cfg(not(target_os = "linux"))]
        unimplemented!()
    }

    #[inline(always)]
    pub(crate) fn mark_write(&self) {
        self.f_st.write_epoch.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub(crate) fn unmap(mut self) -> InternalResult<()> {
        #[cfg(not(target_os = "linux"))]
        unimplemented!();

        #[cfg(target_os = "linux")]
        unsafe {
            self.f_st.stop_signal.store(true, Ordering::Release);

            if let Some(tx) = self.f_tx.take() {
                tx.thread().unpark();
                let _ = tx.join();
            }

            self.mmap.msync()?;
            self.mmap.unmap();
        }

        Ok(())
    }

    #[inline]
    fn flush(&self) -> InternalResult<()> {
        #[cfg(target_os = "linux")]
        unsafe {
            self.mmap.masync()
        }

        #[cfg(not(target_os = "linux"))]
        unimplemented!()
    }

    fn spawn_tx(tx_mmap: Arc<crate::linux::MMap>, tx_state: Arc<FlushState>) -> JoinHandle<()> {
        thread::spawn(move || {
            let mut last_seen = 0u64;

            loop {
                if tx_state.stop_signal.load(Ordering::Acquire) {
                    break;
                }

                // sleep (no busy spin)
                thread::park_timeout(Duration::from_secs(1));

                let current = tx_state.write_epoch.load(Ordering::Acquire);
                if current == last_seen {
                    continue;
                }

                unsafe {
                    let _ = tx_mmap.masync();
                }

                last_seen = current;
            }
        })
    }
}
