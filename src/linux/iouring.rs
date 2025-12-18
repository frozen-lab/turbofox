use super::MMap;
use crate::{
    core::unlikely,
    error::{InternalError, InternalResult},
    file::TurboBuf,
};
use libc::{c_int, c_void, close, iovec, off_t, sigset_t, syscall, SYS_io_uring_enter, SYS_io_uring_setup};
use std::sync::{
    atomic::{AtomicBool, AtomicU32, Ordering},
    Arc, Condvar, Mutex,
};
use std::thread::{self, JoinHandle};

const IOURING_OFF_SQ_RING: off_t = 0x00;
const IOURING_OFF_CQ_RING: off_t = 0x8000000;
const IOURING_OFF_SQES: off_t = 0x10000000;

const IOURING_FEAT_SINGLE_MMAP: u32 = 0x01;
const IOURING_FYSNC_USER_DATA: u64 = 0xFFFF_FFFF_FFFF_FFFF;
const QUEUE_DEPTH: u32 = 0x80; // 128 * 64 bytes = 8 KiB for SQE entries

const TAG_READ: u64 = 1u64 << 63;
const TAG_WRITE: u64 = 0u64 << 63;
const TAG_MASK: u64 = !(1u64 << 63);

#[derive(Debug)]
pub(crate) struct IOUring {
    ring_fd: i32,
    file_fd: i32,
    rings: RingPtrs,
    params: IOUringParams,
    in_flight: Arc<AtomicU32>,
    cq_poll_tx: Option<JoinHandle<()>>,
    cq_poll_shutdown_flag: Arc<AtomicBool>,
}

#[repr(C)]
struct PendingWrite {
    iov: iovec,
    buf: Arc<TurboBuf>,
}

#[repr(C)]
struct PendingRead {
    iov: iovec,
    buf: Arc<Vec<u8>>,
    state: Arc<(Mutex<Option<i32>>, Condvar)>,
}

/// Maps the io_uring operation codes mirroring the original `io_uring_op` enum.
/// For reference, <https://github.com/torvalds/linux/blob/master/include/uapi/linux/io_uring.h#L240>
enum IOUringOP {
    READV = 1,
    WRITEV = 2,
    FSYNC = 3,
}

/// Maps the io_uring sqe flag bits mirroring original constants.
/// For reference, <https://github.com/torvalds/linux/blob/master/include/uapi/linux/io_uring.h#L141>
#[repr(u32)]
enum IOUringSQEFlags {
    IOLINK = 1 << 2,
}

#[allow(unused)]
#[derive(Debug, Copy, Clone)]
#[repr(C)]
struct SQringOffset {
    head: u32,
    tail: u32,
    ring_mask: u32,
    ring_entries: u32,
    flags: u32,
    dropped: u32,
    array: u32,
    user_addr: u64,
}

#[allow(unused)]
#[derive(Debug, Copy, Clone)]
#[repr(C)]
struct CQringOffset {
    head: u32,
    tail: u32,
    ring_mask: u32,
    ring_entries: u32,
    overflow: u32,
    cqes: u32,
    flags: u32,
    resv1: u32,
    user_addr: u64,
}

#[allow(unused)]
#[derive(Copy, Clone, Debug)]
#[repr(C)]
struct IOUringCQE {
    user_data: u64,
    res: i32,
    flags: u32,
}

#[allow(unused)]
#[derive(Copy, Clone)]
#[repr(C)]
union SQEOffUnion {
    off: u64,
    addr2: u64,
}

#[allow(unused)]
#[derive(Copy, Clone)]
#[repr(C)]
union SQEAddrUnion {
    addr: u64,
    splice_off_in: u64,
}

#[allow(unused)]
#[derive(Copy, Clone)]
#[repr(C)]
struct IOUringSQE {
    opcode: u8,
    flags: u8,
    ioprio: u16,
    fd: i32,
    off: SQEOffUnion,
    addr: SQEAddrUnion,
    len: u32,
    user_data: u64,
    union2: [u64; 0x03],
}

#[allow(unused)]
const IOURING_SQE_SIZE: usize = std::mem::size_of::<IOUringSQE>();

#[allow(unused)]
#[derive(Debug, Copy, Clone)]
#[repr(C)]
struct IOUringParams {
    sq_entries: u32,
    cq_entries: u32,
    flags: u32,
    sq_thread_cpu: u32,
    sq_thread_idle: u32,
    features: u32,
    resv: [u32; 0x04],
    sq_off: SQringOffset,
    cq_off: CQringOffset,
}

#[derive(Debug)]
struct RingPtrs {
    sq_ptr: MMap,
    cq_ptr: MMap,
    sqes_ptr: MMap,
}

impl RingPtrs {
    unsafe fn munmap(&self) -> InternalResult<()> {
        let sq_base = self.sq_ptr.ptr();
        let cq_base = self.cq_ptr.ptr();

        self.sq_ptr.m_unmap()?;
        if sq_base != cq_base {
            self.cq_ptr.m_unmap()?;
        }
        self.sqes_ptr.m_unmap()
    }
}

impl IOUring {
    /// Creates a new instance of `[IOUring]` via ring_fd
    pub(crate) unsafe fn new(file_fd: i32) -> InternalResult<Self> {
        let mut params: IOUringParams = std::mem::zeroed();

        params.flags = 0x00;
        params.sq_thread_idle = 0x00;
        params.sq_thread_cpu = 0x00;

        let ring_fd = Self::iouring_init(&mut params)?;
        let rings = Self::mmap_rings(ring_fd, &params).map_err(|e| {
            // NOTE:
            // As we are already in an errored state, we must ignore this error
            // as the preceeding error is more important
            let _ = Self::close(ring_fd);
            e
        })?;

        // sanity check
        #[cfg(debug_assertions)]
        {
            debug_assert!(
                params.sq_off.array != 0 && params.cq_off.cqes != 0,
                "Kernel did not fill SQ/CQ offsets"
            );
        }

        let (cq_poll_tx, cq_poll_shutdown_flag, in_flight) = Self::spawn_cq_poller_tx(&params, &rings);

        Ok(Self {
            ring_fd,
            file_fd,
            rings,
            params,
            in_flight,
            cq_poll_shutdown_flag,
            cq_poll_tx: Some(cq_poll_tx),
        })
    }

    /// Closes the `[IOUring]` via ring_fd
    pub(crate) unsafe fn close(ring_fd: i32) -> InternalResult<()> {
        let res = close(ring_fd);
        if unlikely(res != 0) {
            return Self::last_os_error();
        }

        Ok(())
    }

    pub(crate) unsafe fn write(&self, buf: TurboBuf, off: u64) -> InternalResult<()> {
        if unlikely(self.cq_poll_shutdown_flag.load(Ordering::Acquire)) {
            return Err(InternalError::IO("IOUring shutting down".into()));
        }

        let pending = Box::new(PendingWrite {
            iov: iovec {
                iov_base: buf.to_ptr() as *mut c_void,
                iov_len: 3,
            },
            buf: Arc::new(buf),
        });

        let pending_ptr = Box::into_raw(pending) as u64;
        let (tail, idx0) = self.next_sqe_index();

        //
        // prep for writev
        //
        let sqe0 = (self.rings.sqes_ptr.ptr_mut() as *mut IOUringSQE).add(idx0 as usize);
        std::ptr::write_bytes(sqe0 as *mut u8, 0, IOURING_SQE_SIZE);

        (*sqe0).opcode = IOUringOP::WRITEV as u8;
        (*sqe0).flags = IOUringSQEFlags::IOLINK as u8;
        (*sqe0).fd = self.file_fd;
        (*sqe0).len = 1; // iov count
        (*sqe0).addr = SQEAddrUnion { addr: pending_ptr };
        (*sqe0).off = SQEOffUnion { off };
        (*sqe0).user_data = (pending_ptr & TAG_MASK) | TAG_WRITE;

        //
        // prep for fsync
        //
        let tail2 = tail + 1;
        let idx1 = tail2 & self.sq_mask();
        let sqe1 = (self.rings.sqes_ptr.ptr_mut() as *mut IOUringSQE).add(idx1 as usize);
        std::ptr::write_bytes(sqe1 as *mut u8, 0, IOURING_SQE_SIZE);

        (*sqe1).opcode = IOUringOP::FSYNC as u8;
        (*sqe1).flags = 0u8;
        (*sqe1).fd = self.file_fd;
        (*sqe1).user_data = IOURING_FYSNC_USER_DATA;

        //
        // push into SQE
        //
        let sq_array = self.sq_array_ptr();
        let mask = self.sq_mask();

        std::ptr::write_volatile(sq_array.add((tail & mask) as usize), idx0);
        std::ptr::write_volatile(sq_array.add((tail2 & mask) as usize), idx1);

        std::sync::atomic::fence(Ordering::Release);
        std::ptr::write_volatile(self.sq_tail_ptr(), tail + 2);

        //
        // io_uring submit
        //
        self.in_flight.fetch_add(1, Ordering::Release);
        if syscall(SYS_io_uring_enter, self.ring_fd, 2, 0, 0, std::ptr::null::<sigset_t>()) < 0 {
            self.in_flight.fetch_sub(1, Ordering::Release);
            let _ = Box::from_raw(pending_ptr as *mut PendingWrite);
            return Self::last_os_error();
        }

        Ok(())
    }

    pub(crate) unsafe fn read(&self, off: u64, len: usize) -> InternalResult<Vec<u8>> {
        if unlikely(self.cq_poll_shutdown_flag.load(Ordering::Acquire)) {
            return Err(InternalError::IO("IOUring shutting down".into()));
        }

        let buf = Arc::new(vec![0u8; len]);
        let state = Arc::new((Mutex::new(None), Condvar::new()));
        let pending = Box::new(PendingRead {
            iov: iovec {
                iov_base: buf.as_ptr() as *mut c_void,
                iov_len: len,
            },
            buf: buf.clone(),
            state: state.clone(),
        });

        let pending_ptr = Box::into_raw(pending) as u64;
        let (tail, idx) = self.next_sqe_index();

        //
        // prep for readv
        //
        let sqe = (self.rings.sqes_ptr.ptr_mut() as *mut IOUringSQE).add(idx as usize);
        std::ptr::write_bytes(sqe as *mut u8, 0, IOURING_SQE_SIZE);

        (*sqe).opcode = IOUringOP::READV as u8;
        (*sqe).fd = self.file_fd;
        (*sqe).len = 1;
        (*sqe).addr = SQEAddrUnion { addr: pending_ptr };
        (*sqe).off = SQEOffUnion { off };
        (*sqe).user_data = (pending_ptr & TAG_MASK) | TAG_READ;

        //
        // push into SQE
        //
        let sq_array = self.sq_array_ptr();
        let mask = self.sq_mask();
        std::ptr::write_volatile(sq_array.add((tail & mask) as usize), idx);

        std::sync::atomic::fence(Ordering::Release);
        std::ptr::write_volatile(self.sq_tail_ptr(), tail + 1);

        //
        // io_uring submit
        //
        self.in_flight.fetch_add(1, Ordering::Release);
        if syscall(SYS_io_uring_enter, self.ring_fd, 1, 0, 0, std::ptr::null::<sigset_t>()) < 0 {
            self.in_flight.fetch_sub(1, Ordering::Release);
            let _ = Box::from_raw(pending_ptr as *mut PendingRead);
            return Self::last_os_error();
        }

        //
        // wait for completion
        //
        let (lock, cv) = &*state;
        let mut guard = match lock.lock() {
            Ok(g) => g,
            Err(e) => return Err(e.into()),
        };

        while guard.is_none() {
            guard = match cv.wait(guard) {
                Ok(g) => g,
                Err(e) => return Err(e.into()),
            };
        }

        if let Some(res) = guard.take() {
            if res < 0 {
                return Err(std::io::Error::from_raw_os_error(-res).into());
            }

            return match Arc::try_unwrap(buf) {
                Ok(v) => Ok(v),
                Err(_) => Err(InternalError::InvalidDbState(
                    "read buffer still has outstanding references".into(),
                )),
            };
        }

        Err(InternalError::IO("IOUring error! Unable to read from DB".into()))
    }

    pub(crate) unsafe fn drop(&mut self) {
        self.cq_poll_shutdown_flag.store(true, Ordering::Release);

        // stop polling thread
        if let Some(tx) = self.cq_poll_tx.take() {
            tx.thread().unpark();
            let _ = tx.join();
        }

        let _ = self.rings.munmap();
        let _ = Self::close(self.ring_fd);
    }

    unsafe fn spawn_cq_poller_tx(
        params: &IOUringParams,
        rings: &RingPtrs,
    ) -> (JoinHandle<()>, Arc<AtomicBool>, Arc<AtomicU32>) {
        let shutdown = Arc::new(AtomicBool::new(false));
        let in_flight = Arc::new(AtomicU32::new(0));

        let shutdown_clone = shutdown.clone();
        let in_flight_clone = in_flight.clone();

        let cq_head_addr = rings.cq_ptr.ptr_mut().add(params.cq_off.head as usize) as usize;
        let cq_tail_addr = rings.cq_ptr.ptr_mut().add(params.cq_off.tail as usize) as usize;
        let cqes_addr = rings.cq_ptr.ptr_mut().add(params.cq_off.cqes as usize) as usize;
        let cq_mask = *(rings.cq_ptr.ptr_mut().add(params.cq_off.ring_mask as usize) as *const u32);

        let handle = thread::spawn(move || {
            let cq_head_ptr = cq_head_addr as *mut u32;
            let cq_tail_ptr = cq_tail_addr as *mut u32;
            let cqes_ptr = cqes_addr as *mut IOUringCQE;

            loop {
                let head = core::ptr::read_volatile(cq_head_ptr);
                let tail = core::ptr::read_volatile(cq_tail_ptr);

                if head == tail {
                    // check shutdown
                    if shutdown_clone.load(Ordering::Acquire) && in_flight_clone.load(Ordering::Acquire) == 0 {
                        break;
                    }

                    // NOTE:
                    // if head == tail and shutdown == false, we must avoid busy loop spin for current
                    // thread, so we make it sleep for a while, hence just park the thread!
                    std::thread::park_timeout(std::time::Duration::from_micros(50));
                    continue;
                }

                let idx = head & cq_mask;
                let cqe = core::ptr::read_volatile(cqes_ptr.add(idx as usize));

                // NOTE:
                // we decr `in_flight` ops for both READ and WRITE!
                // For WRITE we free after `fsync` and for READ after `read` as there is no
                // fsync for reads!

                if cqe.user_data == IOURING_FYSNC_USER_DATA {
                    if cqe.res < 0 {
                        // NOTE:
                        // this states fsync has failed, we must reschedule the fsync to ensure
                        // durability of the data
                        eprintln!("ERROR: fsync failed: {}", std::io::Error::from_raw_os_error(-cqe.res));
                    }

                    in_flight_clone.fetch_sub(1, Ordering::Release);
                } else {
                    let tag = cqe.user_data & !TAG_MASK;
                    let ptr = (cqe.user_data & TAG_MASK) as *mut c_void;

                    match tag {
                        TAG_WRITE => {
                            let pending = Box::from_raw(ptr as *mut PendingWrite);

                            if cqe.res < 0 {
                                let err = std::io::Error::from_raw_os_error(-cqe.res);
                                eprintln!("ERROR: {err} for cqe={:?}", cqe);

                                // TODO:
                                // this states the syscall failed, for durability, we either must retry (reschedule the write)
                                // or esclate the error to the user!
                            } else {
                                if cqe.res != 3 {
                                    eprintln!("ERROR: Partial write on cqe={:?}", cqe);
                                }

                                let idx_to_free = pending.buf.idx();
                                println!("Freeup => {idx_to_free}");

                                // TODO:
                                // this states partial write, which is bad for accuracy, we must schedule the write
                                // and perform the write op again!
                            }
                        }
                        TAG_READ => {
                            let pending = Box::from_raw(ptr as *mut PendingRead);
                            in_flight_clone.fetch_sub(1, Ordering::Release);

                            let (lock, cv) = &*pending.state;

                            // we try to obtain lock few times, before giving up
                            for _ in 0..0x0A {
                                match lock.try_lock() {
                                    Ok(mut guard) => {
                                        *guard = Some(cqe.res);
                                        cv.notify_one();
                                        break;
                                    }
                                    Err(std::sync::TryLockError::WouldBlock) => {
                                        // wait a bit to be able to try again
                                        std::thread::sleep(std::time::Duration::from_micros(0x0A));
                                    }
                                    Err(std::sync::TryLockError::Poisoned(_)) => {
                                        eprintln!("ERROR: PendingRead lock poisoned");
                                        break;
                                    }
                                }
                            }
                        }
                        _ => unreachable!("Invalid tag"),
                    }
                }

                // advance CQ head
                core::ptr::write_volatile(cq_head_ptr, head.wrapping_add(1));
            }
        });

        (handle, shutdown, in_flight)
    }

    unsafe fn iouring_init(params: &mut IOUringParams) -> InternalResult<i32> {
        let ring_fd = libc::syscall(SYS_io_uring_setup, QUEUE_DEPTH, params as *mut IOUringParams) as c_int;
        if ring_fd < 0x00 {
            let errno = *libc::__errno_location();

            // NOTE:
            // `io_uring` is introduced in Linux Kernel 5.1 and after. Some environments,
            // including container images, decide to opt out of using `io_uring`.
            //
            // HACK:
            // We simply throw [`UnsupportedVersion`] error when lack of support is detected
            //
            // TODO:
            // In future, we must have a backup for I/O ops, in case `io_uring` is not supported
            if errno == libc::ENOSYS {
                return Err(InternalError::UnsupportedVersion("IOUring not supported".into()));
            }

            return Self::last_os_error();
        }

        Ok(ring_fd)
    }

    unsafe fn mmap_rings(ring_fd: i32, params: &IOUringParams) -> InternalResult<RingPtrs> {
        // TODO:
        // Perform checked arithmetic ops, and handle overflows gracefully
        let sq_ring_sz = params.sq_off.array as usize + params.sq_entries as usize * std::mem::size_of::<u32>();
        let cq_ring_sz = params.cq_off.cqes as usize + params.cq_entries as usize * std::mem::size_of::<u64>();

        // NOTE:
        // The kernel may place the SQ & CQ rings in one contiguous VMA, depends on the kernel version.
        // If it does, it sets `IOURING_FEAT_SINGLE_MMAP` in `params.features`, and we are allowed to mmap
        // both rings with a single `mmap`.
        //
        // This is purely a kernel-side optimization to reduce VMA and syscall overhead.
        //
        // If the flag is not set, we must mmap rings separately. Attempting a single mmap will
        // result in UB.
        let single_mmap = params.features & IOURING_FEAT_SINGLE_MMAP != 0;
        let ring_sz = std::cmp::max(sq_ring_sz, cq_ring_sz);

        // SQ ring map
        let sq_ptr = MMap::m_map(
            ring_fd,
            if single_mmap { ring_sz } else { sq_ring_sz },
            IOURING_OFF_SQ_RING as usize,
        )?;

        // CQ ring map
        let cq_ptr = if single_mmap {
            sq_ptr.clone()
        } else {
            match MMap::m_map(ring_fd, cq_ring_sz, IOURING_OFF_CQ_RING as usize) {
                Ok(ptr) => ptr,
                Err(e) => {
                    // NOTE:
                    // As we are already in an errored state, we must ignore this error
                    // as the preceeding error is more important
                    let _ = sq_ptr.m_unmap();
                    return Err(e);
                }
            }
        };

        // SQEs array map
        let sqes_sz = params.sq_entries as usize * std::mem::size_of::<IOUringSQE>();
        let sqes_ptr = match MMap::m_map(ring_fd, sqes_sz, IOURING_OFF_SQES as usize) {
            Ok(ptr) => ptr,
            Err(e) => {
                // NOTE:
                // As we are already in an errored state, we must ignore this error
                // as the preceeding error is more important
                let sq_base = sq_ptr.ptr();
                let cq_base = cq_ptr.ptr();

                let _ = sq_ptr.m_unmap();
                if sq_base != cq_base {
                    let _ = cq_ptr.m_unmap();
                }

                return Err(e);
            }
        };

        Ok(RingPtrs {
            sq_ptr,
            cq_ptr,
            sqes_ptr,
        })
    }

    #[inline(always)]
    unsafe fn next_sqe_index(&self) -> (u32, u32) {
        let tail_ptr = self.sq_tail_ptr();
        let tail = core::ptr::read_volatile(tail_ptr);
        let mask = self.sq_mask();
        let idx = tail & mask;

        (tail, idx)
    }

    #[inline(always)]
    unsafe fn sq_tail_ptr(&self) -> *mut u32 {
        self.rings.sq_ptr.ptr_mut().add(self.params.sq_off.tail as usize) as *mut u32
    }

    #[inline(always)]
    unsafe fn sq_mask(&self) -> u32 {
        *(self.rings.sq_ptr.ptr_mut().add(self.params.sq_off.ring_mask as usize) as *const u32)
    }

    #[inline(always)]
    unsafe fn sq_array_ptr(&self) -> *mut u32 {
        self.rings.sq_ptr.ptr_mut().add(self.params.sq_off.array as usize) as *mut u32
    }

    #[inline]
    fn last_os_error<T>() -> InternalResult<T> {
        Err(std::io::Error::last_os_error().into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::linux::File;
    use tempfile::TempDir;

    #[test]
    fn test_init_works_correctly() {
        unsafe {
            let dir = TempDir::new().expect("new temp dir");
            let path = dir.path().join("tmp.bin");

            let file = File::new(&path).expect("new file");
            let mut iouring = IOUring::new(file.fd()).expect("init works");

            assert!(iouring.ring_fd >= 0, "Ring fd must be non-negative");
            assert!(iouring.file_fd >= 0, "File fd must be non-negative");

            assert!(iouring.params.sq_off.array != 0);
            assert!(iouring.params.cq_off.cqes != 0);

            assert!(!iouring.rings.sq_ptr.ptr().is_null());
            assert!(!iouring.rings.cq_ptr.ptr().is_null());
            assert!(!iouring.rings.sqes_ptr.ptr().is_null());

            assert!(iouring.cq_poll_tx.is_some());
            assert!(iouring.cq_poll_shutdown_flag.load(Ordering::Relaxed) == false);

            iouring.drop();
        }
    }

    #[test]
    fn test_write_read_cycle() {
        unsafe {
            let dir = TempDir::new().expect("new temp dir");
            let path = dir.path().join("tmp.bin");

            let file = File::new(&path).expect("new file");
            let mut iouring = IOUring::new(file.fd()).expect("init works");

            let dummy_data: Vec<u8> = vec![0, 1, 2];
            let buf = TurboBuf::new(dummy_data.as_ptr() as *mut u8, 0);
            assert!(iouring.write(buf, 0).is_ok(), "Write should not fail");
            std::thread::sleep(std::time::Duration::from_millis(1)); // manual sleep so write could be finished

            let buf = iouring.read(0, dummy_data.len());
            assert!(buf.is_ok(), "Read should not fail");
            assert_eq!(buf.unwrap(), dummy_data);

            iouring.drop();
        }
    }
}
