//! Windows implementation: overlapped named pipes for the control plane and
//! named file mappings for the shared region.
//!
//! Windows named pipes cannot carry duplicated handles, so the shared-memory
//! mapping is named and the name travels inside the `Welcome` handshake. The
//! client opens the mapping with `OpenFileMappingW`.

use std::cell::Cell;
use std::ptr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use windows_sys::Win32::Foundation::{
    CloseHandle, ERROR_ACCESS_DENIED, ERROR_ALREADY_EXISTS, ERROR_BROKEN_PIPE,
    ERROR_FILE_NOT_FOUND, ERROR_IO_PENDING, ERROR_NO_DATA, ERROR_PIPE_BUSY, ERROR_PIPE_CONNECTED,
    GENERIC_READ, GENERIC_WRITE, GetLastError, HANDLE, INVALID_HANDLE_VALUE, WAIT_OBJECT_0,
    WAIT_TIMEOUT,
};
use windows_sys::Win32::Storage::FileSystem::{
    CreateFileW, FILE_FLAG_FIRST_PIPE_INSTANCE, FILE_FLAG_OVERLAPPED, OPEN_EXISTING,
    PIPE_ACCESS_DUPLEX, ReadFile, WriteFile,
};
use windows_sys::Win32::System::IO::{CancelIoEx, GetOverlappedResult, OVERLAPPED};
use windows_sys::Win32::System::Memory::{
    CreateFileMappingW, FILE_MAP_ALL_ACCESS, MEMORY_MAPPED_VIEW_ADDRESS, MapViewOfFile,
    OpenFileMappingW, PAGE_READWRITE, UnmapViewOfFile,
};
use windows_sys::Win32::System::Pipes::{
    ConnectNamedPipe, CreateNamedPipeW, PIPE_READMODE_BYTE, PIPE_TYPE_BYTE,
    PIPE_UNLIMITED_INSTANCES, PIPE_WAIT, PeekNamedPipe, WaitNamedPipeW,
};
use windows_sys::Win32::System::Threading::{CreateEventW, WaitForSingleObject};

use crate::error::{Error, ErrorKind, Result};

const HANDSHAKE_LIMIT: usize = 64 * 1024;
const PIPE_BUFFER: u32 = 1024 * 1024;
const PIPE_PREFIX: &str = r"\\.\pipe\";

// Framing is length-prefixed on every platform, so Windows always uses byte
// mode pipes; message mode would make partial reads fail with
// `ERROR_MORE_DATA` for frames larger than the read buffer.

fn last_error() -> u32 {
    // SAFETY: plain Win32 call.
    unsafe { GetLastError() }
}

fn classify(code: u32) -> ErrorKind {
    match code {
        ERROR_ACCESS_DENIED => ErrorKind::PermissionDenied,
        ERROR_PIPE_BUSY | ERROR_ALREADY_EXISTS => ErrorKind::AddrInUse,
        ERROR_FILE_NOT_FOUND | ERROR_BROKEN_PIPE | ERROR_NO_DATA => ErrorKind::Disconnected,
        ERROR_IO_PENDING => ErrorKind::WouldBlock,
        _ => ErrorKind::Io,
    }
}

fn os_error(context: &str, code: u32) -> Error {
    let mut error = Error::new(
        classify(code),
        format!("{context} failed (win32 error {code})"),
    );
    error = error.with_os_code(code as i32);
    error
}

fn timeout_error() -> Error {
    Error::new(ErrorKind::Timeout, "operation timed out")
}

fn wide(value: &str) -> Vec<u16> {
    value.encode_utf16().chain(std::iter::once(0)).collect()
}

fn pipe_path(name: &str) -> String {
    if name.starts_with(r"\\") {
        name.to_owned()
    } else {
        let trimmed = name.strip_prefix('@').unwrap_or(name);
        format!("{PIPE_PREFIX}{trimmed}")
    }
}

fn millis(timeout: Option<Duration>) -> u32 {
    match timeout {
        None => u32::MAX,
        Some(timeout) => timeout.as_millis().min(u32::MAX as u128 - 1) as u32,
    }
}

fn remaining(deadline: Option<Instant>) -> Option<Duration> {
    deadline.map(|deadline| deadline.saturating_duration_since(Instant::now()))
}

struct Event(HANDLE);

impl Event {
    fn new() -> Result<Self> {
        // SAFETY: manual-reset, initially unsignalled, unnamed event.
        let handle = unsafe { CreateEventW(ptr::null(), 1, 0, ptr::null()) };
        if handle == INVALID_HANDLE_VALUE || handle.is_null() {
            return Err(os_error("CreateEventW", last_error()));
        }
        Ok(Self(handle))
    }
}

impl Drop for Event {
    fn drop(&mut self) {
        // SAFETY: the handle is owned by this guard.
        unsafe {
            CloseHandle(self.0);
        }
    }
}

fn wait_handle(event: HANDLE, timeout: Option<Duration>) -> Result<()> {
    // SAFETY: `event` is a valid waitable handle.
    match unsafe { WaitForSingleObject(event, millis(timeout)) } {
        WAIT_OBJECT_0 => Ok(()),
        WAIT_TIMEOUT => Err(timeout_error()),
        _ => Err(os_error("WaitForSingleObject", last_error())),
    }
}

/// A bound named-pipe endpoint.
#[derive(Debug)]
pub(crate) struct Listener {
    handle: Cell<HANDLE>,
    name: String,
}

fn open_pipe_instance(name: &str, first: bool) -> Result<HANDLE> {
    let path = wide(name);
    let mut open_mode = PIPE_ACCESS_DUPLEX | FILE_FLAG_OVERLAPPED;
    if first {
        open_mode |= FILE_FLAG_FIRST_PIPE_INSTANCE;
    }
    // SAFETY: `path` is NUL-terminated; security attributes are optional.
    let handle = unsafe {
        CreateNamedPipeW(
            path.as_ptr(),
            open_mode,
            PIPE_TYPE_BYTE | PIPE_READMODE_BYTE | PIPE_WAIT,
            PIPE_UNLIMITED_INSTANCES,
            PIPE_BUFFER,
            PIPE_BUFFER,
            0,
            ptr::null(),
        )
    };
    if handle == INVALID_HANDLE_VALUE || handle.is_null() {
        let code = last_error();
        if code == ERROR_ACCESS_DENIED || code == ERROR_PIPE_BUSY {
            return Err(Error::new(
                ErrorKind::AddrInUse,
                format!("pipe {name} is already in use"),
            ));
        }
        return Err(os_error("CreateNamedPipeW", code));
    }
    Ok(handle)
}

impl Listener {
    pub(crate) fn bind(name: &str, _prefer_seqpacket: bool) -> Result<Self> {
        let name = pipe_path(name);
        let handle = open_pipe_instance(&name, true)?;
        Ok(Self {
            handle: Cell::new(handle),
            name,
        })
    }

    pub(crate) fn accept(&self, timeout: Option<Duration>) -> Result<Stream> {
        let handle = self.handle.get();
        if handle == INVALID_HANDLE_VALUE {
            return Err(Error::new(
                ErrorKind::Closed,
                "listener has no free instance",
            ));
        }
        let event = Event::new()?;
        let mut overlapped = OVERLAPPED {
            hEvent: event.0,
            ..OVERLAPPED::default()
        };
        // SAFETY: handle is a listening pipe created with FILE_FLAG_OVERLAPPED.
        let connected = unsafe { ConnectNamedPipe(handle, &mut overlapped) };
        if connected == 0 {
            let code = last_error();
            match code {
                ERROR_PIPE_CONNECTED => {}
                ERROR_IO_PENDING => {
                    wait_handle(event.0, timeout)?;
                    let mut transferred = 0u32;
                    // SAFETY: the overlapped structure belongs to this operation.
                    let ok =
                        unsafe { GetOverlappedResult(handle, &overlapped, &mut transferred, 0) };
                    if ok == 0 && last_error() != ERROR_PIPE_CONNECTED {
                        return Err(os_error("GetOverlappedResult(connect)", last_error()));
                    }
                }
                _ => return Err(os_error("ConnectNamedPipe", code)),
            }
        }
        // The connected instance now belongs to the stream; install a fresh
        // listening instance so the listener stays usable.
        self.handle.set(INVALID_HANDLE_VALUE);
        if let Ok(next) = open_pipe_instance(&self.name, false) {
            self.handle.set(next);
        }
        Stream::new(handle)
    }
}

impl Drop for Listener {
    fn drop(&mut self) {
        let handle = self.handle.get();
        if handle != INVALID_HANDLE_VALUE && !handle.is_null() {
            // SAFETY: the handle is owned by this listener.
            unsafe {
                CloseHandle(handle);
            }
        }
    }
}

/// Connect to a named pipe.
pub(crate) fn connect(name: &str, _prefer_seqpacket: bool) -> Result<Stream> {
    let path = pipe_path(name);
    let path = wide(&path);
    for _ in 0..100 {
        // SAFETY: `path` is NUL-terminated; zero sharing and no template file.
        let handle = unsafe {
            CreateFileW(
                path.as_ptr(),
                GENERIC_READ | GENERIC_WRITE,
                0,
                ptr::null(),
                OPEN_EXISTING,
                FILE_FLAG_OVERLAPPED,
                ptr::null_mut(),
            )
        };
        if handle != INVALID_HANDLE_VALUE && !handle.is_null() {
            return Stream::new(handle);
        }
        let code = last_error();
        if code == ERROR_PIPE_BUSY {
            // SAFETY: `path` is NUL-terminated; wait up to 100 ms per attempt.
            unsafe {
                WaitNamedPipeW(path.as_ptr(), 100);
            }
            continue;
        }
        return Err(os_error("CreateFileW", code));
    }
    Err(Error::new(ErrorKind::Timeout, "named pipe stayed busy"))
}

/// One end of a named pipe.
pub(crate) struct Stream {
    handle: HANDLE,
    event: Event,
}

// SAFETY: operations are thread-safe at the Win32 level; the protocol still
// expects one reader and one writer per direction.
unsafe impl Send for Stream {}
unsafe impl Sync for Stream {}

impl Stream {
    fn new(handle: HANDLE) -> Result<Self> {
        Ok(Self {
            handle,
            event: Event::new()?,
        })
    }

    pub(crate) fn shutdown(&self) {
        // Closing the handle in `Drop` is enough. `FlushFileBuffers` would
        // block until the peer drains the pipe, which is unacceptable here.
    }

    fn read_once(&self, buf: &mut [u8], timeout: Option<Duration>) -> Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        let len = u32::try_from(buf.len())
            .map_err(|_| Error::new(ErrorKind::MessageTooLarge, "read buffer exceeds 4 GiB"))?;
        let mut overlapped = OVERLAPPED {
            hEvent: self.event.0,
            ..OVERLAPPED::default()
        };
        let mut read = 0u32;
        // SAFETY: `buf` is valid for writes of `len` bytes.
        let ok = unsafe {
            ReadFile(
                self.handle,
                buf.as_mut_ptr(),
                len,
                &mut read,
                &mut overlapped,
            )
        };
        if ok != 0 {
            return Ok(read as usize);
        }
        let code = last_error();
        if code != ERROR_IO_PENDING {
            return match classify(code) {
                ErrorKind::Disconnected => Ok(0),
                _ => Err(os_error("ReadFile", code)),
            };
        }
        match wait_handle(self.event.0, timeout) {
            Ok(()) => {
                let mut transferred = 0u32;
                // SAFETY: the overlapped structure belongs to this read.
                let ok =
                    unsafe { GetOverlappedResult(self.handle, &overlapped, &mut transferred, 0) };
                if ok != 0 {
                    Ok(transferred as usize)
                } else {
                    let code = last_error();
                    match classify(code) {
                        ErrorKind::Disconnected => Ok(0),
                        _ => Err(os_error("GetOverlappedResult(read)", code)),
                    }
                }
            }
            Err(error) if error.kind() == ErrorKind::Timeout => {
                // SAFETY: cancel exactly this operation, then reap it.
                unsafe {
                    CancelIoEx(self.handle, &overlapped);
                }
                let mut ignored = 0u32;
                // SAFETY: reaping the cancelled operation; the result is ignored.
                unsafe {
                    GetOverlappedResult(self.handle, &overlapped, &mut ignored, 1);
                }
                Err(error)
            }
            Err(error) => Err(error),
        }
    }

    fn write_once(&self, data: &[u8], timeout: Option<Duration>) -> Result<usize> {
        if data.is_empty() {
            return Ok(0);
        }
        let len = u32::try_from(data.len())
            .map_err(|_| Error::new(ErrorKind::MessageTooLarge, "write exceeds 4 GiB"))?;
        let mut overlapped = OVERLAPPED {
            hEvent: self.event.0,
            ..OVERLAPPED::default()
        };
        let mut written = 0u32;
        // SAFETY: `data` is valid for reads of `len` bytes.
        let ok = unsafe {
            WriteFile(
                self.handle,
                data.as_ptr(),
                len,
                &mut written,
                &mut overlapped,
            )
        };
        if ok != 0 {
            return Ok(written as usize);
        }
        let code = last_error();
        if code != ERROR_IO_PENDING {
            return match classify(code) {
                ErrorKind::Disconnected => {
                    Err(Error::new(ErrorKind::Disconnected, "peer closed the pipe"))
                }
                _ => Err(os_error("WriteFile", code)),
            };
        }
        match wait_handle(self.event.0, timeout) {
            Ok(()) => {
                let mut transferred = 0u32;
                // SAFETY: the overlapped structure belongs to this write.
                let ok =
                    unsafe { GetOverlappedResult(self.handle, &overlapped, &mut transferred, 0) };
                if ok != 0 {
                    Ok(transferred as usize)
                } else {
                    let code = last_error();
                    match classify(code) {
                        ErrorKind::Disconnected => {
                            Err(Error::new(ErrorKind::Disconnected, "peer closed the pipe"))
                        }
                        _ => Err(os_error("GetOverlappedResult(write)", code)),
                    }
                }
            }
            Err(error) if error.kind() == ErrorKind::Timeout => {
                // SAFETY: cancel exactly this operation, then reap it.
                unsafe {
                    CancelIoEx(self.handle, &overlapped);
                }
                let mut ignored = 0u32;
                // SAFETY: reaping the cancelled operation; the result is ignored.
                unsafe {
                    GetOverlappedResult(self.handle, &overlapped, &mut ignored, 1);
                }
                Err(error)
            }
            Err(error) => Err(error),
        }
    }

    pub(crate) fn send_all(&self, mut data: &[u8], timeout: Option<Duration>) -> Result<()> {
        let deadline = timeout.map(|timeout| Instant::now() + timeout);
        while !data.is_empty() {
            let written = self.write_once(data, remaining(deadline))?;
            if written == 0 {
                return Err(Error::new(ErrorKind::Disconnected, "peer closed the pipe"));
            }
            data = &data[written..];
        }
        Ok(())
    }

    pub(crate) fn wake(&self) -> Result<()> {
        match self.write_once(&[1u8], Some(Duration::from_millis(1))) {
            Ok(_) => Ok(()),
            Err(error) if error.kind() == ErrorKind::Timeout => Ok(()),
            Err(error) => Err(error),
        }
    }

    pub(crate) fn wait_wake(&self, timeout: Option<Duration>) -> Result<()> {
        let mut byte = [0u8; 1];
        match self.read_once(&mut byte, timeout) {
            Ok(0) => Err(Error::new(ErrorKind::Disconnected, "pipe closed")),
            Ok(_) => Ok(()),
            Err(error) => Err(error),
        }
    }

    pub(crate) fn try_read(&self, buf: &mut [u8]) -> Result<usize> {
        let mut available = 0u32;
        // SAFETY: null data buffer asks only for the byte count.
        let ok = unsafe {
            PeekNamedPipe(
                self.handle,
                ptr::null_mut(),
                0,
                ptr::null_mut(),
                &mut available,
                ptr::null_mut(),
            )
        };
        if ok == 0 {
            let code = last_error();
            return match classify(code) {
                ErrorKind::Disconnected => {
                    Err(Error::new(ErrorKind::Disconnected, "peer closed the pipe"))
                }
                _ => Err(os_error("PeekNamedPipe", code)),
            };
        }
        if available == 0 {
            return Err(Error::new(ErrorKind::WouldBlock, "no data available"));
        }
        let wanted = buf.len().min(available as usize);
        match self.read_once(&mut buf[..wanted], Some(Duration::ZERO)) {
            Err(error) if error.kind() == ErrorKind::Timeout => {
                Err(Error::new(ErrorKind::WouldBlock, "no data available"))
            }
            other => other,
        }
    }

    pub(crate) fn read_some(&self, buf: &mut [u8], timeout: Option<Duration>) -> Result<usize> {
        if matches!(timeout, Some(timeout) if timeout.is_zero()) {
            return self.try_read(buf);
        }
        self.read_once(buf, timeout)
    }

    pub(crate) fn send_handshake(
        &self,
        payload: &[u8],
        _handle: Option<&ShmHandle>,
        timeout: Option<Duration>,
    ) -> Result<()> {
        let mut frame = Vec::with_capacity(4 + payload.len());
        frame.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        frame.extend_from_slice(payload);
        // The mapping name travels inside the payload on Windows.
        self.send_all(&frame, timeout)
    }

    pub(crate) fn recv_handshake(
        &self,
        timeout: Option<Duration>,
    ) -> Result<(Vec<u8>, Option<ShmHandle>)> {
        let deadline = timeout.map(|timeout| Instant::now() + timeout);
        let mut header = [0u8; 4];
        self.read_exact(&mut header, remaining(deadline))?;
        let len = u32::from_le_bytes(header) as usize;
        if len > HANDSHAKE_LIMIT - 4 {
            return Err(Error::protocol("handshake frame is too large"));
        }
        let mut payload = vec![0u8; len];
        self.read_exact(&mut payload, remaining(deadline))?;
        Ok((payload, None))
    }

    fn read_exact(&self, mut buf: &mut [u8], timeout: Option<Duration>) -> Result<()> {
        let deadline = timeout.map(|timeout| Instant::now() + timeout);
        while !buf.is_empty() {
            let read = self.read_once(buf, remaining(deadline))?;
            if read == 0 {
                return Err(Error::new(ErrorKind::Disconnected, "peer closed the pipe"));
            }
            buf = &mut buf[read..];
        }
        Ok(())
    }
}

impl Drop for Stream {
    fn drop(&mut self) {
        // SAFETY: the handle is owned by this stream.
        unsafe {
            CloseHandle(self.handle);
        }
    }
}

/// Create a connected pipe pair inside the current process.
pub(crate) fn socket_pair(prefer_seqpacket: bool) -> Result<(Stream, Stream)> {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let name = format!(
        "{PIPE_PREFIX}maple-pair-{}-{}",
        std::process::id(),
        COUNTER.fetch_add(1, Ordering::Relaxed)
    );
    let listener = Listener::bind(&name, prefer_seqpacket)?;
    let path = wide(&name);

    let mut client = INVALID_HANDLE_VALUE;
    for _ in 0..100 {
        // SAFETY: `path` is NUL-terminated.
        let handle = unsafe {
            CreateFileW(
                path.as_ptr(),
                GENERIC_READ | GENERIC_WRITE,
                0,
                ptr::null(),
                OPEN_EXISTING,
                FILE_FLAG_OVERLAPPED,
                ptr::null_mut(),
            )
        };
        if handle != INVALID_HANDLE_VALUE && !handle.is_null() {
            client = handle;
            break;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    if client == INVALID_HANDLE_VALUE || client.is_null() {
        return Err(os_error("CreateFileW(pair)", last_error()));
    }

    match listener.accept(Some(Duration::from_secs(5))) {
        Ok(server) => Ok((Stream::new(client)?, server)),
        Err(error) => {
            // SAFETY: closing the client half we just opened.
            unsafe {
                CloseHandle(client);
            }
            Err(error)
        }
    }
}

/// A named shared-memory object.
pub(crate) struct ShmHandle {
    name: String,
}

impl ShmHandle {
    pub(crate) fn name(&self) -> &str {
        &self.name
    }
}

/// A mapped shared-memory region.
pub(crate) struct Map {
    ptr: std::ptr::NonNull<u8>,
    len: usize,
    mapping: HANDLE,
}

// SAFETY: access is synchronized by the ring protocol built on top of the map.
unsafe impl Send for Map {}
unsafe impl Sync for Map {}

impl Map {
    pub(crate) fn as_ptr(&self) -> *mut u8 {
        self.ptr.as_ptr()
    }

    pub(crate) fn len(&self) -> usize {
        self.len
    }
}

impl Drop for Map {
    fn drop(&mut self) {
        // SAFETY: the view and mapping handles came from successful calls.
        unsafe {
            UnmapViewOfFile(MEMORY_MAPPED_VIEW_ADDRESS {
                Value: self.ptr.as_ptr().cast(),
            });
            CloseHandle(self.mapping);
        }
    }
}

fn map_view(mapping: HANDLE, len: usize) -> Result<Map> {
    // SAFETY: `mapping` is a valid file-mapping handle with read/write access.
    let view = unsafe { MapViewOfFile(mapping, FILE_MAP_ALL_ACCESS, 0, 0, len) };
    if view.Value.is_null() {
        let error = os_error("MapViewOfFile", last_error());
        // SAFETY: the caller owns `mapping`; closing it here would double close,
        // so the caller handles cleanup.
        return Err(error);
    }
    Ok(Map {
        // SAFETY: `MapViewOfFile` returned a non-null view.
        ptr: unsafe { std::ptr::NonNull::new_unchecked(view.Value.cast::<u8>()) },
        len,
        mapping,
    })
}

static SHM_COUNTER: AtomicU64 = AtomicU64::new(0);

pub(crate) fn create_shm(len: usize) -> Result<(Map, ShmHandle)> {
    let len32 = u32::try_from(len)
        .map_err(|_| Error::invalid_config("region_len must fit in 32 bits on Windows"))?;
    let name = format!(
        "Local\\maple-{}-{}",
        std::process::id(),
        SHM_COUNTER.fetch_add(1, Ordering::Relaxed)
    );
    let path = wide(&name);
    // SAFETY: page-file backed mapping; `path` is NUL-terminated.
    let mapping = unsafe {
        CreateFileMappingW(
            INVALID_HANDLE_VALUE,
            ptr::null(),
            PAGE_READWRITE,
            0,
            len32,
            path.as_ptr(),
        )
    };
    if mapping == INVALID_HANDLE_VALUE || mapping.is_null() {
        return Err(os_error("CreateFileMappingW", last_error()));
    }
    match map_view(mapping, len) {
        Ok(map) => Ok((map, ShmHandle { name })),
        Err(error) => {
            // SAFETY: the mapping handle is owned here and was not stored.
            unsafe {
                CloseHandle(mapping);
            }
            Err(error)
        }
    }
}

pub(crate) fn attach_named_shm(name: &str, len: usize) -> Result<Map> {
    if name.is_empty() {
        return Err(Error::protocol("peer did not provide a shared-memory name"));
    }
    let path = wide(name);
    // SAFETY: `path` is NUL-terminated.
    let mapping = unsafe { OpenFileMappingW(FILE_MAP_ALL_ACCESS, 0, path.as_ptr()) };
    if mapping == INVALID_HANDLE_VALUE || mapping.is_null() {
        return Err(os_error("OpenFileMappingW", last_error()));
    }
    match map_view(mapping, len) {
        Ok(map) => Ok(map),
        Err(error) => {
            // SAFETY: the mapping handle is owned here and was not stored.
            unsafe {
                CloseHandle(mapping);
            }
            Err(error)
        }
    }
}
