use super::MMap;
use crate::{
    core::unlikely,
    error::{InternalError, InternalResult},
};
use libc::{c_int, c_uint, c_ulong, close, off_t, syscall, SYS_io_uring_register, SYS_io_uring_setup};

const IOURING_FEAT_SINGLE_MMAP: u32 = 0x01;

const IOURING_REGISTER_FILES: u32 = 0x02;

const IOURING_OFF_SQ_RING: off_t = 0x00;
const IOURING_OFF_CQ_RING: off_t = 0x8000000;
const IOURING_OFF_SQES: off_t = 0x10000000;

const IOURING_FYSNC_USER_DATA: u64 = 0xFFFF_FFFF_FFFF_FFFF;
const IOURING_FIXED_FILE_IDX: i32 = 0x00; // as we only register one file, it's stored at 0th index.

const QUEUE_DEPTH: u32 = 0x80; // 128 * 64 bytes = 8 KiB for SQE entries

#[derive(Debug)]
pub(crate) struct IOUring {
    ring_fd: i32,
    file_fd: i32,
    rings: RingPtrs,
    params: IOUringParams,
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
enum IOUringSQEFlags {
    FIXEDFILE = 0,
    IOLINK = 2,
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
    pub(crate) unsafe fn new(file_fd: i32) -> InternalResult<Self> {
        let mut params: IOUringParams = std::mem::zeroed();

        params.flags = 0x00;
        params.sq_thread_idle = 0x00;
        params.sq_thread_cpu = 0x00;

        let ring_fd = Self::iouring_init(&mut params)?;
        let rings = Self::mmap_rings(ring_fd, &params)?;
        Self::register_file(file_fd, ring_fd).map_err(|e| {
            // NOTE:
            // As we are already in an errored state, we must ignore this error
            // as the preceeding error is more important
            let _ = rings.munmap();
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

        Ok(Self {
            ring_fd,
            file_fd,
            rings,
            params,
        })
    }

    /// Closes the `[File]` via fd
    pub(crate) unsafe fn close(ring_fd: i32) -> InternalResult<()> {
        let res = close(ring_fd);
        if unlikely(res != 0) {
            return Err(std::io::Error::last_os_error().into());
        }

        Ok(())
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

            let err = std::io::Error::last_os_error();
            return Err(err.into());
        }

        Ok(ring_fd)
    }

    unsafe fn register_file(file_fd: i32, ring_fd: i32) -> InternalResult<()> {
        let fds = [file_fd];
        let res = syscall(
            SYS_io_uring_register,
            ring_fd,
            IOURING_REGISTER_FILES as c_ulong,
            fds.as_ptr(),
            fds.len() as c_uint,
        );

        if res < 0 {
            let err = std::io::Error::last_os_error();
            return Err(err.into());
        }

        Ok(())
    }

    unsafe fn mmap_rings(ring_fd: i32, params: &IOUringParams) -> InternalResult<RingPtrs> {
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
                let _ = sq_ptr.m_unmap();
                let _ = cq_ptr.m_unmap();
                return Err(e);
            }
        };

        Ok(RingPtrs {
            sq_ptr,
            cq_ptr,
            sqes_ptr,
        })
    }
}
