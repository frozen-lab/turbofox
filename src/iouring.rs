use crate::errors::InternalResult;
use libc::{c_void, mmap, munmap, syscall, SYS_io_uring_setup, MAP_SHARED, PROT_READ, PROT_WRITE};
use std::{
    fs::File,
    mem::zeroed,
    os::{fd::AsRawFd, raw::c_int},
    ptr::null_mut,
};

/// No. of page bufs we can store into mem for write syscalls
///
/// TODO: We must take `max_value` as config from user, and it
/// should match this, so we will not have race conditions, cause
/// we must block new writes (thread sleep, etc.) if no bufs are
/// available to write into
pub(super) const PAGE_BUF_SIZE: usize = 128;

const IOURING_SETUP_SQPOLL: u32 = 1 << 0;
const IOURING_SETUP_SQ_AFF: u32 = 1 << 1;
const IOURING_FEAT_SINGLE_MMAP: u32 = 1 << 0;

const QUEUE_DEPTH: u32 = 0x400; // 1024 i.e. ~80 KiB of memory overhead
const IOURING_POLL_THREAD_IDLE: u32 = 200; // 200 ms

const IOURING_OFF_SQ_RING: libc::off_t = 0;
const IOURING_OFF_CQ_RING: libc::off_t = 0x8000000;
const IOURING_OFF_SQES: libc::off_t = 0x10000000;

const IOURING_REGISTER_BUFFERS: u32 = 0;
const IOURING_UNREGISTER_BUFFERS: u32 = 1;
const IOURING_REGISTER_FILES: u32 = 2;
const IOURING_UNREGISTER_FILES: u32 = 3;

const IOURING_ENTER_SQ_WAKEUP: u32 = 1 << 3;
const IOURING_OP_WRITEV: u8 = 2;
const IOURING_OP_FSYNC: u8 = 3;

const IOSQE_IO_LINK: u8 = 1 << 2;
const IOSQE_FIXED_FILE: u8 = 1 << 0;

#[allow(unused)]
#[derive(Debug, Copy, Clone)]
#[repr(C)]
struct SqringOffsets {
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
struct CqringOffsets {
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

#[derive(Copy, Clone)]
#[repr(C)]
pub struct IoUringSqe {
    opcode: u8,
    flags: u8,
    ioprio: u16,
    fd: i32,
    off: OffUnion,
    addr: AddrUnion,
    len: u32,
    user_data: u64,
    union2: [u64; 3],
}

#[derive(Copy, Clone)]
#[repr(C)]
union OffUnion {
    off: u64,
    addr2: u64,
}

#[derive(Copy, Clone)]
#[repr(C)]
union AddrUnion {
    addr: u64,
    splice_off_in: u64,
}

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
    resv: [u32; 4],
    sq_off: SqringOffsets,
    cq_off: CqringOffsets,
}

struct IOUringRings {
    sq_ptr: *mut c_void,
    cq_ptr: *mut c_void,
    sqes_ptr: *mut c_void,
}

pub(super) struct IOUring {
    ring_fd: i32,
    file_fd: i32,
    buf_base: *mut u8,
    rings: IOUringRings,
    params: IOUringParams,
    buf_page_size: usize,
    active_buf_idx: usize,
    iovecs: Vec<libc::iovec>,
}

impl std::fmt::Debug for IOUring {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IOUring")
            .field("ring_fd", &self.ring_fd)
            .field("file_fd", &self.file_fd)
            .finish()
    }
}

impl IOUring {
    #[allow(unsafe_op_in_unsafe_fn)]
    pub(super) unsafe fn new(file: &File) -> InternalResult<Self> {
        let active_buf_idx = 0;
        let buf_page_size = libc::sysconf(libc::_SC_PAGESIZE) as usize;

        //
        // io_uring init
        //

        let mut params: IOUringParams = std::mem::zeroed();

        // params.flags = IOURING_SETUP_SQPOLL | IOURING_SETUP_SQ_AFF;
        params.flags = 0;
        params.sq_thread_idle = IOURING_POLL_THREAD_IDLE;
        params.sq_thread_cpu = 0x00;

        let ring_fd = syscall(SYS_io_uring_setup, QUEUE_DEPTH, &mut params as *mut IOUringParams) as c_int;
        let file_fd = file.as_raw_fd();

        eprintln!("FD's file={file_fd} ring={ring_fd}");

        if ring_fd < 0 || file_fd < 0 {
            eprintln!("Either ring or file fd's are invalid!");
            return Err(std::io::Error::last_os_error().into());
        }

        //
        // mmap shared rings (SQ, CQ, etc.)
        //

        let rings = Self::mmap_rings(ring_fd, &params).map_err(|e| {
            libc::close(ring_fd);
            e
        })?;

        //
        // Register file descriptor
        //

        if let Err(e) = Self::register_files(ring_fd, file_fd) {
            libc::close(ring_fd);
            return Err(e);
        }

        //
        // allocate (page align) and register buffer to be used for write syscalls
        //

        let (iovecs, buf_base) = Self::allocate_bufs(buf_page_size).map_err(|e| {
            libc::close(ring_fd);
            e
        })?;

        if let Err(e) = Self::register_buffers(ring_fd, iovecs.as_ptr(), iovecs.len()) {
            libc::close(ring_fd);
            return Err(e);
        }

        Ok(Self {
            ring_fd,
            file_fd,
            rings,
            params,
            buf_base,
            active_buf_idx,
            buf_page_size,
            iovecs,
        })
    }

    #[allow(unsafe_op_in_unsafe_fn)]
    pub(super) unsafe fn write(&mut self, data: &[u8], offset: u64) {
        debug_assert!(!data.is_empty(), "Input buffer must not be empty");

        let buf_idx = self.active_buf_idx;
        let buf_ptr = self.buf_base.add(buf_idx * self.buf_page_size);
        let copy_len = std::cmp::min(data.len(), self.buf_page_size);

        // copy data into registered buf
        std::ptr::copy_nonoverlapping(data.as_ptr(), buf_ptr, copy_len);

        self.submit_write_and_fsync(buf_idx, offset);
        self.active_buf_idx = (buf_idx + 1) % PAGE_BUF_SIZE;

        let res = libc::fsync(self.file_fd);

        if res < 0 {
            let err = std::io::Error::last_os_error();
            eprintln!("WRITE RES => {res}, err => {err}");
        }
    }

    #[allow(unsafe_op_in_unsafe_fn)]
    unsafe fn allocate_bufs(page_size: usize) -> InternalResult<(Vec<libc::iovec>, *mut u8)> {
        let total_size = page_size.checked_mul(PAGE_BUF_SIZE).ok_or_else(|| {
            eprintln!("Mult error while calculating total_size in alloc_bufs!");
            std::io::Error::from_raw_os_error(libc::EINVAL)
        })?;

        let layout = std::alloc::Layout::from_size_align(total_size, page_size).map_err(|_| {
            eprintln!("unable to create layout in alloc_bufs!");
            std::io::Error::from_raw_os_error(libc::EINVAL)
        })?;

        let base_ptr = std::alloc::alloc_zeroed(layout);

        if base_ptr.is_null() {
            eprintln!("base_ptr is null in alloc_bufs!");
            return Err(std::io::Error::last_os_error().into());
        }

        let mut iovecs = Vec::with_capacity(PAGE_BUF_SIZE);

        for i in 0..PAGE_BUF_SIZE {
            let ptr = base_ptr.add(i * page_size);

            iovecs.push(libc::iovec {
                iov_base: ptr as *mut c_void,
                iov_len: page_size,
            });
        }

        Ok((iovecs, base_ptr))
    }

    #[allow(unsafe_op_in_unsafe_fn)]
    unsafe fn mmap_rings(ring_fd: i32, params: &IOUringParams) -> InternalResult<IOUringRings> {
        let sq_ring_sz = params.sq_off.array as usize + params.sq_entries as usize * std::mem::size_of::<u32>();
        let cq_ring_sz = params.cq_off.cqes as usize + params.cq_entries as usize * std::mem::size_of::<u64>();

        let single_mmap = params.features & IOURING_FEAT_SINGLE_MMAP != 0;
        let ring_sz = std::cmp::max(sq_ring_sz, cq_ring_sz);

        // helper to create mmap
        let do_mmap = |len: usize, offset: libc::off_t| -> InternalResult<*mut c_void> {
            let ptr = mmap(null_mut(), len, PROT_READ | PROT_WRITE, MAP_SHARED, ring_fd, offset);

            if ptr == libc::MAP_FAILED {
                let err = std::io::Error::last_os_error();
                eprintln!("Unable to create mmap => {:?}", err);
                return Err(err.into());
            }

            Ok(ptr)
        };

        // helper for cleanup on failure (unmap mapped objects)
        let cleanup = |sq: *mut c_void, cq: *mut c_void, sqes: *mut c_void| {
            if !sqes.is_null() && sqes != libc::MAP_FAILED {
                munmap(sqes, params.sq_entries as usize * std::mem::size_of::<IoUringSqe>());
            }

            if !cq.is_null() && cq != libc::MAP_FAILED && cq != sq {
                munmap(cq, cq_ring_sz);
            }

            if !sq.is_null() && sq != libc::MAP_FAILED {
                munmap(sq, if single_mmap { ring_sz } else { sq_ring_sz });
            }
        };

        // SQ ring map
        let sq_ptr = do_mmap(if single_mmap { ring_sz } else { sq_ring_sz }, IOURING_OFF_SQ_RING)?;

        // CQ ring map
        let cq_ptr = if single_mmap {
            sq_ptr
        } else {
            match do_mmap(cq_ring_sz, IOURING_OFF_CQ_RING) {
                Ok(ptr) => ptr,

                Err(e) => {
                    cleanup(sq_ptr, null_mut(), null_mut());
                    return Err(e.into());
                }
            }
        };

        // SQEs array map
        let sqes_sz = params.sq_entries as usize * std::mem::size_of::<IoUringSqe>();

        let sqes_ptr = match do_mmap(sqes_sz, IOURING_OFF_SQES) {
            Ok(ptr) => ptr,

            Err(e) => {
                cleanup(sq_ptr, cq_ptr, null_mut());
                return Err(e.into());
            }
        };

        Ok(IOUringRings {
            sq_ptr,
            cq_ptr,
            sqes_ptr,
        })
    }

    #[allow(unsafe_op_in_unsafe_fn)]
    #[inline(always)]
    unsafe fn register_files(ring_fd: i32, file_fd: i32) -> InternalResult<()> {
        let fds = [file_fd];

        let ret = libc::syscall(
            libc::SYS_io_uring_register,
            ring_fd,
            IOURING_REGISTER_FILES as libc::c_ulong,
            fds.as_ptr(),
            fds.len() as libc::c_uint,
        ) as libc::c_int;

        if ret < 0 {
            eprintln!("Unable to register file! ring_fd={ring_fd} & file_fd={file_fd}");
            return Err(std::io::Error::last_os_error().into());
        }

        Ok(())
    }

    #[allow(unsafe_op_in_unsafe_fn)]
    #[inline(always)]
    unsafe fn register_buffers(ring_fd: i32, iovecs: *const libc::iovec, iovecs_len: usize) -> InternalResult<()> {
        let ret = libc::syscall(
            libc::SYS_io_uring_register,
            ring_fd,
            IOURING_REGISTER_BUFFERS as libc::c_ulong,
            iovecs,
            iovecs_len as u32 as libc::c_uint,
        ) as libc::c_int;

        if ret < 0 {
            let err = std::io::Error::last_os_error();
            eprintln!(
                "Unable to register bufs! ring_fd={ring_fd}, ret={}, errno={:?}",
                ret, err
            );
            return Err(err.into());
        }

        Ok(())
    }

    #[allow(unsafe_op_in_unsafe_fn)]
    unsafe fn submit_write_and_fsync(&self, buf_idx: usize, file_offset: u64) {
        // sanity check
        debug_assert!(buf_idx <= PAGE_BUF_SIZE, "buf idx is out of bounds");

        let (tail, idx) = self.next_sqe_index();
        let sqe_size = std::mem::size_of::<IoUringSqe>();
        let sqe_ptr = (self.rings.sqes_ptr as *mut IoUringSqe).add(idx as usize);

        let iov_ptr = self.iovecs.as_ptr().add(buf_idx) as *const libc::iovec;

        //
        // zero the sqe then populate
        //
        core::ptr::write_bytes(sqe_ptr as *mut u8, 0, sqe_size);

        //
        // prep for WRITEV syscall
        //

        // prep for WRITEV syscall
        (*sqe_ptr).opcode = IOURING_OP_WRITEV;
        // only link; don't mix registered-file / fixed flags here for now
        (*sqe_ptr).flags = IOSQE_IO_LINK;
        (*sqe_ptr).fd = self.file_fd; // use actual FD when not relying on IOSQE_FIXED_FILE
        (*sqe_ptr).off = OffUnion { off: file_offset };
        (*sqe_ptr).addr = AddrUnion { addr: iov_ptr as u64 };
        (*sqe_ptr).len = 0x01; // single iovec
        (*sqe_ptr).user_data = 0x00; // TODO: Tag the write w/ id so we can track for completion

        // prep for FSYNC syscall
        let tail2 = tail + 1;
        let idx2 = tail2 & self.sq_mask();
        let sqe2_ptr = (self.rings.sqes_ptr as *mut IoUringSqe).add(idx2 as usize);

        core::ptr::write_bytes(sqe2_ptr as *mut u8, 0, sqe_size);

        (*sqe2_ptr).opcode = IOURING_OP_FSYNC;
        (*sqe2_ptr).flags = 0x00; // fsync doesn't need fixed-file flag here
        (*sqe2_ptr).fd = self.file_fd; // use the real FD
        (*sqe2_ptr).user_data = 0x00; // TODO: Tag the fsync for completion tracking

        //
        // prep the submission w/ op order
        //

        let arr = self.sq_array_ptr();
        let pos = (tail & self.sq_mask()) as usize;
        let pos2 = (tail2 & self.sq_mask()) as usize;

        core::ptr::write_volatile(arr.add(pos), idx);
        core::ptr::write_volatile(arr.add(pos2), idx2);

        //
        // publish new tail (tail + 2)
        //

        let new_tail = tail.wrapping_add(2);

        std::sync::atomic::fence(std::sync::atomic::Ordering::Release);
        core::ptr::write_volatile(self.sq_tail_ptr(), new_tail);

        // immediate submit (blocking submission; kernel will process SQEs now)
        let res = libc::syscall(
            libc::SYS_io_uring_enter,
            self.ring_fd as libc::c_int,
            2u32, // to_submit
            0u32, // min_complete
            0u32, // flags
            std::ptr::null::<libc::sigset_t>(),
        ) as libc::c_int;

        if res < 0 {
            eprintln!("io_uring_enter submit failed: {:?}", std::io::Error::last_os_error());
        }

        std::sync::atomic::fence(std::sync::atomic::Ordering::SeqCst);

        let cq_head = (self.rings.cq_ptr as *mut u8).add(self.params.cq_off.head as usize) as *mut u32;
        let cq_tail = (self.rings.cq_ptr as *mut u8).add(self.params.cq_off.tail as usize) as *mut u32;
        let cq_mask = *((self.rings.cq_ptr as *mut u8).add(self.params.cq_off.ring_mask as usize) as *const u32);

        let mut head = core::ptr::read_volatile(cq_head);
        let tail = core::ptr::read_volatile(cq_tail);

        eprintln!("HEAD/TAIL => {head}:{tail}");

        while head != tail {
            let idx = head & cq_mask;

            eprintln!("CQE completed @ idx {idx}");
            head = head.wrapping_add(1);

            core::ptr::write_volatile(cq_head, head);
        }
    }

    #[allow(unsafe_op_in_unsafe_fn)]
    #[inline(always)]
    unsafe fn next_sqe_index(&self) -> (u32, u32) {
        let tail_ptr = self.sq_tail_ptr();
        let tail = core::ptr::read_volatile(tail_ptr);
        let mask = self.sq_mask();
        let idx = tail & mask;

        (tail, idx)
    }

    #[allow(unsafe_op_in_unsafe_fn)]
    #[inline(always)]
    unsafe fn sq_head_ptr(&self) -> *mut u32 {
        (self.rings.sq_ptr as *mut u8).add(self.params.sq_off.head as usize) as *mut u32
    }

    #[allow(unsafe_op_in_unsafe_fn)]
    #[inline(always)]
    unsafe fn sq_tail_ptr(&self) -> *mut u32 {
        (self.rings.sq_ptr as *mut u8).add(self.params.sq_off.tail as usize) as *mut u32
    }

    #[allow(unsafe_op_in_unsafe_fn)]
    #[inline(always)]
    unsafe fn sq_mask(&self) -> u32 {
        *((self.rings.sq_ptr as *mut u8).add(self.params.sq_off.ring_mask as usize) as *const u32)
    }

    #[allow(unsafe_op_in_unsafe_fn)]
    #[inline(always)]
    unsafe fn sq_array_ptr(&self) -> *mut u32 {
        (self.rings.sq_ptr as *mut u8).add(self.params.sq_off.array as usize) as *mut u32
    }
}

impl Drop for IOUring {
    fn drop(&mut self) {
        unsafe {
            //
            // clear up buffer alocated for write ops
            //
            if !self.buf_base.is_null() {
                let total_size = self.buf_page_size * PAGE_BUF_SIZE;

                match std::alloc::Layout::from_size_align(total_size, self.buf_page_size) {
                    Ok(layout) => std::alloc::dealloc(self.buf_base, layout),
                    Err(e) => eprintln!("Unable to drop layout => {:?}", e),
                }
            }

            //
            // Unmap the SQE's array map
            //
            if !self.rings.sqes_ptr.is_null() {
                munmap(
                    self.rings.sqes_ptr,
                    self.params.sq_entries as usize * std::mem::size_of::<IoUringSqe>(),
                );
            }

            //
            // Unmap the CQ map
            //
            if !self.rings.cq_ptr.is_null() && self.rings.cq_ptr != self.rings.sq_ptr {
                let cq_size =
                    self.params.cq_off.cqes as usize + self.params.cq_entries as usize * std::mem::size_of::<u64>();

                munmap(self.rings.cq_ptr, cq_size);
            }

            //
            // Unmap the SQ map
            //
            if !self.rings.sq_ptr.is_null() {
                let sq_size = if self.params.features & IOURING_FEAT_SINGLE_MMAP != 0 {
                    std::cmp::max(
                        self.params.sq_off.array as usize
                            + self.params.sq_entries as usize * std::mem::size_of::<u32>(),
                        self.params.cq_off.cqes as usize + self.params.cq_entries as usize * std::mem::size_of::<u64>(),
                    )
                } else {
                    self.params.sq_off.array as usize + self.params.sq_entries as usize * std::mem::size_of::<u32>()
                };

                munmap(self.rings.sq_ptr, sq_size);
            }

            libc::close(self.ring_fd);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{fs::OpenOptions, io::Read, path::PathBuf};
    use tempfile::TempDir;

    fn create_iouring() -> (IOUring, PathBuf, File, TempDir) {
        let tmp = TempDir::new().expect("tempdir");
        let path = tmp.path().join("temp_io_uring");
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&path)
            .expect("file");

        let io_ring = unsafe { IOUring::new(&file).expect("Failed to create io_uring") };

        (io_ring, path, file, tmp)
    }

    #[test]
    fn test_iouring_init() {
        let (io_ring, _path, _file, _tmp) = create_iouring();

        assert!(io_ring.ring_fd >= 0, "Ring fd must be non-negative");
        assert!(io_ring.file_fd >= 0, "File fd must be non-negative");

        assert!(io_ring.params.sq_off.array != 0, "SQE's offset must be set by kernel");
        assert!(io_ring.params.cq_off.cqes != 0, "CQE's offset must be set by kernel");

        assert!(!io_ring.rings.sq_ptr.is_null(), "SQ pointer must be valid");
        assert!(!io_ring.rings.cq_ptr.is_null(), "CQ pointer must be valid");
        assert!(!io_ring.rings.sqes_ptr.is_null(), "SQEs pointer must be valid");

        assert!(io_ring.buf_page_size > 0, "Buf page size must not be zero");
        assert_eq!(io_ring.active_buf_idx, 0, "Active buf idx must start from 0");
        assert!(io_ring.buf_base != null_mut(), "Base buf ptr must not be 0");

        drop(io_ring);
    }

    #[test]
    fn test_allocate_bufs() {
        unsafe {
            let page_size = libc::sysconf(libc::_SC_PAGESIZE) as usize;
            let (iovecs, base_ptr) = IOUring::allocate_bufs(page_size).expect("alloc failed");

            assert_eq!(iovecs.len(), PAGE_BUF_SIZE);
            assert!(!base_ptr.is_null());

            for i in 0..PAGE_BUF_SIZE {
                let ptr = base_ptr.add(i * page_size);

                assert_eq!(iovecs[i].iov_base, ptr as *mut c_void, "Each iovec must be zeroed");
                assert_eq!(iovecs[i].iov_len, page_size, "Each iovec must be of PAGE_SIZE");
            }

            // cleanup to avoid resource leak
            let layout = std::alloc::Layout::from_size_align(page_size * PAGE_BUF_SIZE, page_size).unwrap();
            std::alloc::dealloc(base_ptr, layout);
        }
    }

    #[test]
    fn test_register_files_invalid_fd() {
        unsafe {
            let res = IOUring::register_files(-1, -1);
            assert!(res.is_err());
        }
    }

    #[test]
    fn test_register_buffers_invalid_ptr() {
        unsafe {
            let res = IOUring::register_buffers(-1, null_mut(), 1);
            assert!(res.is_err());
        }
    }

    #[test]
    fn test_write_and_fsync() {
        let path = PathBuf::new().join("tmp_test_file");
        let (mut io_ring, _path, _file, _tmp) = create_iouring();
        let dummy_data = "Dummy Data to write w/ fsync".as_bytes();
        let offset: u64 = 0;

        unsafe {
            io_ring.write(&dummy_data, offset);
        }

        // manual sleep, so kernal could finish the process
        std::thread::sleep(std::time::Duration::from_millis(250));

        let mut file = OpenOptions::new().read(true).open(_path).expect("file");
        let mut dummy_buf = vec![0u8; dummy_data.len()];
        file.read_exact(&mut dummy_buf).expect("read from file");

        assert_eq!(dummy_data, &dummy_buf);
    }
}
