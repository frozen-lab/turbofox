use crate::error::InternalResult;
use std::{
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    thread::{self, JoinHandle},
    time::Duration,
};

#[derive(Debug)]
pub(crate) struct TurboFile {
    #[cfg(target_os = "linux")]
    file: Arc<crate::linux::File>,

    f_st: Arc<FlushState>,
    f_tx: Option<JoinHandle<()>>,
}

unsafe impl Send for TurboFile {}
unsafe impl Sync for TurboFile {}

impl TurboFile {
    #[cfg(target_os = "linux")]
    #[inline]
    pub(crate) fn new(path: &std::path::Path) -> InternalResult<Self> {
        let file = unsafe { crate::linux::File::new(path) }?;
        Self::init(file)
    }

    #[cfg(target_os = "linux")]
    #[inline]
    pub(crate) fn open(path: &std::path::Path) -> InternalResult<Self> {
        let file = unsafe { crate::linux::File::open(path) }?;
        Self::init(file)
    }

    #[cfg(target_os = "linux")]
    #[inline]
    pub(crate) fn fd(&self) -> i32 {
        self.file.fd()
    }

    #[inline]
    pub(crate) fn close(&self) -> InternalResult<()> {
        #[cfg(not(target_os = "linux"))]
        unimplemented!();

        self.f_st.should_stop();
        if let Some(tx) = self.f_tx.as_ref() {
            tx.thread().unpark();
        }

        unsafe { self.file.close() }
    }

    #[inline]
    pub(crate) fn flush(&self) -> InternalResult<()> {
        #[cfg(not(target_os = "linux"))]
        unimplemented!();

        #[cfg(target_os = "linux")]
        unsafe {
            self.file.sync()
        }
    }

    #[inline]
    pub(crate) fn zero_extend(&self, new_len: usize) -> InternalResult<()> {
        #[cfg(not(target_os = "linux"))]
        unimplemented!();

        #[cfg(target_os = "linux")]
        unsafe {
            self.file.ftruncate(new_len)
        }
    }

    #[inline]
    pub(crate) fn len(&self) -> InternalResult<usize> {
        #[cfg(not(target_os = "linux"))]
        unimplemented!();

        #[cfg(target_os = "linux")]
        unsafe {
            self.file.len()
        }
    }

    #[inline]
    pub(crate) fn read(&self, off: usize, buf_size: usize) -> InternalResult<Vec<u8>> {
        #[cfg(not(target_os = "linux"))]
        unimplemented!();

        #[cfg(target_os = "linux")]
        unsafe {
            self.file.pread(off, buf_size)
        }
    }

    #[inline]
    pub(crate) fn write(&self, off: usize, buf: &[u8]) -> InternalResult<()> {
        #[cfg(not(target_os = "linux"))]
        unimplemented!();

        #[cfg(target_os = "linux")]
        unsafe {
            self.file.pwrite(off, buf)?;
        }

        self.f_st.incr_epoch();
        Ok(())
    }

    #[cfg(target_os = "linux")]
    fn init(file: crate::linux::File) -> InternalResult<Self> {
        let file = Arc::new(file);

        let f_st = Arc::new(FlushState::default());
        let tx = Self::spawn_tx(file.clone(), f_st.clone());

        Ok(Self {
            file,
            f_st,
            f_tx: Some(tx),
        })
    }

    fn spawn_tx(file: Arc<crate::linux::File>, state: Arc<FlushState>) -> JoinHandle<()> {
        thread::spawn(move || {
            let mut last_seen = 0u64;
            loop {
                if state.get_signal() {
                    // TODO:
                    // Must not silently consume errors here!
                    let _ = unsafe { file.sync() };
                    break;
                }

                // sleep (no busy spin)
                thread::park_timeout(Duration::from_secs(1));

                let current = state.load_epoch();
                if current == last_seen {
                    continue;
                }
                last_seen = current;

                // TODO:
                // Must not silently consume errors here!
                let _ = unsafe { file.sync() };
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
