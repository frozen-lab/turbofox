use crate::error::InternalResult;
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
pub(crate) struct TurboMMap {
    #[cfg(target_os = "linux")]
    mmap: Arc<crate::linux::MMap>,

    f_st: Arc<FlushState>,
    f_tx: Option<JoinHandle<()>>,
}

unsafe impl Send for TurboMMap {}
unsafe impl Sync for TurboMMap {}

impl TurboMMap {
    #[cfg(target_os = "linux")]
    #[inline]
    pub(crate) fn new(fd: i32, len: usize, off: usize) -> InternalResult<Self> {
        let mmap = unsafe { Arc::new(crate::linux::MMap::map(fd, len, off)?) };
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

    #[allow(unused)]
    #[inline]
    pub(crate) fn len(&self) -> usize {
        #[cfg(target_os = "linux")]
        return self.mmap.len();

        #[cfg(not(target_os = "linux"))]
        unimplemented!()
    }

    pub(crate) fn write<'a, T>(&'a self, off: usize) -> TurboMMapWriter<'a, T> {
        #[cfg(not(target_os = "linux"))]
        unimplemented!();

        unsafe {
            let ptr = self.mmap.get_mut::<T>(off);
            TurboMMapWriter::new(ptr, &self.f_st)
        }
    }

    pub(crate) fn read<'a, T>(&'a self, off: usize) -> TurboMMapReader<'a, T> {
        #[cfg(not(target_os = "linux"))]
        unimplemented!();

        unsafe { TurboMMapReader::new(self.mmap.get::<T>(off)) }
    }

    #[inline]
    pub(crate) fn unmap(&self) -> InternalResult<()> {
        #[cfg(not(target_os = "linux"))]
        unimplemented!();

        #[cfg(target_os = "linux")]
        self.f_st.should_stop();
        if let Some(tx) = self.f_tx.as_ref() {
            tx.thread().unpark();
        }

        unsafe { self.mmap.unmap() }
    }

    #[inline]
    pub(crate) fn sync(&self) -> InternalResult<()> {
        #[cfg(not(target_os = "linux"))]
        unimplemented!();

        #[cfg(target_os = "linux")]
        unsafe {
            self.mmap.msync()
        }
    }

    fn spawn_tx(tx_mmap: Arc<crate::linux::MMap>, tx_state: Arc<FlushState>) -> JoinHandle<()> {
        thread::spawn(move || {
            let mut last_seen = 0u64;
            loop {
                if tx_state.get_signal() {
                    // TODO:
                    // Must not silently consume errors here!
                    let _ = unsafe { tx_mmap.msync() };
                    break;
                }

                // sleep (no busy spin)
                thread::park_timeout(Duration::from_secs(1));

                let current = tx_state.load_epoch();
                if current == last_seen {
                    continue;
                }
                last_seen = current;

                // TODO:
                // Must not silently consume errors here!
                let _ = unsafe { tx_mmap.msync() };
            }
        })
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

impl FlushState {
    #[inline]
    fn incr_epoch(&self) {
        self.write_epoch.fetch_add(1, Ordering::Release);
    }

    #[inline]
    fn should_stop(&self) {
        self.stop_signal.store(true, Ordering::Release);
    }

    #[inline]
    fn get_signal(&self) -> bool {
        self.stop_signal.load(Ordering::Acquire)
    }

    #[inline]
    fn load_epoch(&self) -> u64 {
        self.write_epoch.load(Ordering::Acquire)
    }
}

#[derive(Debug)]
pub(crate) struct TurboMMapWriter<'a, T> {
    ptr: *mut T,
    state: &'a FlushState,
}

impl<'a, T> TurboMMapWriter<'a, T> {
    #[inline]
    const fn new(ptr: *mut T, state: &'a FlushState) -> Self {
        Self { ptr, state }
    }

    #[inline]
    pub(crate) fn write(&self, f: impl FnOnce(&mut T)) {
        unsafe { f(&mut *self.ptr) }
    }
}

impl<'a, T> Drop for TurboMMapWriter<'a, T> {
    #[inline]
    fn drop(&mut self) {
        self.state.incr_epoch();
    }
}

#[derive(Debug)]
pub(crate) struct TurboMMapReader<'a, T> {
    ptr: *const T,
    _pd: PhantomData<&'a T>,
}

impl<'a, T> TurboMMapReader<'a, T> {
    #[inline]
    const fn new(ptr: *const T) -> Self {
        Self { ptr, _pd: PhantomData }
    }

    #[inline]
    pub(crate) fn read(&self) -> &T {
        unsafe { &*self.ptr }
    }
}
