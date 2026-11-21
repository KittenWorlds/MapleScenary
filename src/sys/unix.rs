//! Unix implementation: `AF_UNIX` sockets (SOCK_SEQPACKET where available),
//! `SCM_RIGHTS` handle passing, and `memfd`/`shm_open` shared memory.

use std::io;
use std::mem::{size_of, zeroed};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::path::PathBuf;
use std::ptr;
#[cfg(not(target_os = "android"))]
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
#[cfg(not(target_os = "android"))]
use std::time::{SystemTime, UNIX_EPOCH};

use crate::error::{Error, ErrorKind, Result};

/// Linux and Android support SOCK_SEQPACKET for `AF_UNIX`; Darwin does not.
pub(crate) const SEQ_PACKET_SUPPORTED: bool = cfg!(any(target_os = "linux", target_os = "android"));

#[cfg(any(target_os = "linux", target_os = "android"))]
const SEND_FLAGS: libc::c_int = libc::MSG_DONTWAIT | libc::MSG_NOSIGNAL;
#[cfg(not(any(target_os = "linux", target_os = "android")))]
const SEND_FLAGS: libc::c_int = libc::MSG_DONTWAIT;

const HANDSHAKE_LIMIT: usize = 64 * 1024;

/// Socket flavour used by a channel.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StreamKind {
    SeqPacket,
    Stream,
}

impl StreamKind {
    fn sock_type(self) -> libc::c_int {
        match self {
            Self::SeqPacket => libc::SOCK_SEQPACKET,
            Self::Stream => libc::SOCK_STREAM,
        }
    }
}

fn last_errno() -> i32 {
    io::Error::last_os_error().raw_os_error().unwrap_or(0)
}

fn os_error(context: &str) -> Error {
    Error::io(context, io::Error::last_os_error())
}

fn timeout_error() -> Error {
    Error::new(ErrorKind::Timeout, "operation timed out")
}

fn deadline_after(timeout: Option<Duration>) -> Option<Instant> {
    timeout.map(|timeout| Instant::now() + timeout)
}

fn wait_fd(fd: RawFd, events: libc::c_short, deadline: Option<Instant>) -> Result<()> {
    loop {
        let timeout_ms = match deadline {
            None => -1,
            Some(deadline) => {
                let now = Instant::now();
                if now >= deadline {
                    return Err(timeout_error());
                }
                let millis = (deadline - now).as_millis();
                if millis > i32::MAX as u128 {
                    i32::MAX
                } else {
                    (millis as i32).max(1)
                }
            }
        };
        let mut pollfd = libc::pollfd {
            fd,
            events,
            revents: 0,
        };
        // SAFETY: `pollfd` points at one valid descriptor for the duration of the call.
        let rc = unsafe { libc::poll(&mut pollfd, 1, timeout_ms) };
        if rc > 0 {
            if pollfd.revents & libc::POLLNVAL != 0 {
                return Err(Error::new(ErrorKind::Io, "invalid file descriptor"));
            }
            if pollfd.revents & events != 0 || pollfd.revents & (libc::POLLHUP | libc::POLLERR) != 0
            {
                return Ok(());
            }
            continue;
        }
        if rc == 0 {
            return Err(timeout_error());
        }
        let errno = last_errno();
        if errno == libc::EINTR {
            continue;
        }
        return Err(os_error("poll"));
    }
}

fn set_nonblocking(fd: RawFd) -> io::Result<()> {
    // SAFETY: `fd` is an open descriptor owned by the caller.
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
    if flags < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: same descriptor, valid flag bits.
    if unsafe { libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) } < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

fn set_cloexec(fd: RawFd) -> io::Result<()> {
    // SAFETY: `fd` is an open descriptor owned by the caller.
    if unsafe { libc::fcntl(fd, libc::F_SETFD, libc::FD_CLOEXEC) } < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

#[cfg(any(target_os = "macos", target_os = "ios"))]
fn set_no_sigpipe(fd: RawFd) {
    let one: libc::c_int = 1;
    // SAFETY: valid descriptor, `SO_NOSIGPIPE` takes an int.
    unsafe {
        libc::setsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_NOSIGPIPE,
            &one as *const _ as *const libc::c_void,
            size_of::<libc::c_int>() as libc::socklen_t,
        );
    }
}

#[cfg(not(any(target_os = "macos", target_os = "ios")))]
fn set_no_sigpipe(_fd: RawFd) {}

fn configure_stream(fd: RawFd) -> io::Result<()> {
    set_cloexec(fd)?;
    set_nonblocking(fd)?;
    set_no_sigpipe(fd);
    Ok(())
}

/// A bound local endpoint.
#[derive(Debug)]
pub(crate) struct Listener {
    fd: RawFd,
    kind: StreamKind,
    path: Option<PathBuf>,
}

impl Listener {
    pub(crate) fn bind(name: &str, prefer_seqpacket: bool) -> Result<Self> {
        let endpoint = Endpoint::parse(name)?;
        let kinds: &[StreamKind] = if prefer_seqpacket && SEQ_PACKET_SUPPORTED {
            &[StreamKind::SeqPacket, StreamKind::Stream]
        } else {
            &[StreamKind::Stream]
        };

        let mut last_error = None;
        for kind in kinds {
            match Self::bind_kind(&endpoint, *kind) {
                Ok(listener) => return Ok(listener),
                Err(error) => {
                    if error.kind() == ErrorKind::Unsupported {
                        last_error = Some(error);
                        continue;
                    }
                    return Err(error);
                }
            }
        }
        Err(last_error
            .unwrap_or_else(|| Error::new(ErrorKind::Unsupported, "no usable socket type")))
    }

    fn bind_kind(endpoint: &Endpoint, kind: StreamKind) -> Result<Self> {
        // SAFETY: plain syscall; return value is checked.
        let fd = unsafe { libc::socket(libc::AF_UNIX, kind.sock_type(), 0) };
        if fd < 0 {
            let errno = last_errno();
            if errno == libc::EPROTONOSUPPORT
                || errno == libc::EOPNOTSUPP
                || errno == libc::ESOCKTNOSUPPORT
                || errno == libc::EPERM
            {
                return Err(Error::new(
                    ErrorKind::Unsupported,
                    format!("socket(AF_UNIX, {:?}) unavailable: {errno}", kind),
                ));
            }
            return Err(os_error("socket"));
        }
        let guard = FdGuard(fd);

        if let Endpoint::Path(path) = endpoint
            && path.exists()
        {
            return Err(Error::new(
                ErrorKind::AddrInUse,
                format!("endpoint {} already exists", path.display()),
            ));
        }
        let (addr, len) = endpoint.sockaddr()?;
        // SAFETY: `addr`/`len` describe a valid `sockaddr_un`.
        if unsafe { libc::bind(fd, &addr as *const _ as *const libc::sockaddr, len) } < 0 {
            return Err(os_error("bind"));
        }
        // SAFETY: valid listening socket.
        if unsafe { libc::listen(fd, 128) } < 0 {
            if let Endpoint::Path(path) = endpoint {
                let _ = std::fs::remove_file(path);
            }
            return Err(os_error("listen"));
        }
        configure_stream(fd).map_err(|err| Error::io("configure listener", err))?;
        let path = match endpoint {
            Endpoint::Path(path) => Some(path.clone()),
            Endpoint::Abstract(_) => None,
        };
        let fd = guard.into_raw();
        Ok(Self { fd, kind, path })
    }

    pub(crate) fn accept(&self, timeout: Option<Duration>) -> Result<Stream> {
        let deadline = deadline_after(timeout);
        loop {
            // SAFETY: valid listening descriptor; null peer address is allowed.
            let fd = unsafe { libc::accept(self.fd, ptr::null_mut(), ptr::null_mut()) };
            if fd >= 0 {
                configure_stream(fd).map_err(|err| Error::io("configure connection", err))?;
                return Ok(Stream::new(fd, self.kind));
            }
            let errno = last_errno();
            if errno == libc::EAGAIN || errno == libc::EWOULDBLOCK {
                wait_fd(self.fd, libc::POLLIN, deadline)?;
                continue;
            }
            if errno == libc::EINTR || errno == libc::ECONNABORTED {
                continue;
            }
            return Err(os_error("accept"));
        }
    }
}

impl Drop for Listener {
    fn drop(&mut self) {
        // SAFETY: descriptor is owned by this listener and closed exactly once.
        unsafe {
            libc::close(self.fd);
        }
        if let Some(path) = &self.path {
            let _ = std::fs::remove_file(path);
        }
    }
}

/// Connect to a named endpoint, falling back from SEQPACKET to STREAM.
pub(crate) fn connect(name: &str, prefer_seqpacket: bool) -> Result<Stream> {
    let endpoint = Endpoint::parse(name)?;
    let kinds: &[StreamKind] = if prefer_seqpacket && SEQ_PACKET_SUPPORTED {
        &[StreamKind::SeqPacket, StreamKind::Stream]
    } else {
        &[StreamKind::Stream]
    };
    let mut last_error = None;
    for kind in kinds {
        // SAFETY: plain syscall; return value is checked.
        let fd = unsafe { libc::socket(libc::AF_UNIX, kind.sock_type(), 0) };
        if fd < 0 {
            let errno = last_errno();
            if errno == libc::EPROTONOSUPPORT
                || errno == libc::EOPNOTSUPP
                || errno == libc::ESOCKTNOSUPPORT
            {
                last_error = Some(Error::new(
                    ErrorKind::Unsupported,
                    format!("socket(AF_UNIX, {kind:?}) unavailable"),
                ));
                continue;
            }
            return Err(os_error("socket"));
        }
        let guard = FdGuard(fd);
        configure_stream(fd).map_err(|err| Error::io("configure client", err))?;
        let (addr, len) = endpoint.sockaddr()?;
        // SAFETY: `addr`/`len` describe a valid `sockaddr_un`.
        if unsafe { libc::connect(fd, &addr as *const _ as *const libc::sockaddr, len) } < 0 {
            let error = os_error("connect");
            if matches!(
                error.os_code(),
                Some(code) if code == libc::EPROTOTYPE || code == libc::EPROTONOSUPPORT
            ) {
                last_error = Some(error);
                continue;
            }
            return Err(error);
        }
        return Ok(Stream::new(guard.into_raw(), *kind));
    }
    Err(last_error.unwrap_or_else(|| Error::new(ErrorKind::Unsupported, "no usable socket type")))
}

struct FdGuard(RawFd);

impl FdGuard {
    fn into_raw(self) -> RawFd {
        let fd = self.0;
        std::mem::forget(self);
        fd
    }
}

impl Drop for FdGuard {
    fn drop(&mut self) {
        // SAFETY: guard owns the descriptor.
        unsafe {
            libc::close(self.0);
        }
    }
}

enum Endpoint {
    Path(PathBuf),
    Abstract(Vec<u8>),
}

impl Endpoint {
    fn parse(name: &str) -> Result<Self> {
        if name.is_empty() {
            return Err(Error::invalid_config("endpoint name must not be empty"));
        }
        if let Some(rest) = name.strip_prefix('@') {
            if rest.is_empty() {
                return Err(Error::invalid_config(
                    "abstract endpoint name must not be empty",
                ));
            }
            if !SEQ_PACKET_SUPPORTED {
                return Err(Error::new(
                    ErrorKind::Unsupported,
                    "abstract Unix sockets are only available on Linux/Android",
                ));
            }
            return Ok(Self::Abstract(rest.as_bytes().to_vec()));
        }
        Ok(Self::Path(PathBuf::from(name)))
    }

    fn sockaddr(&self) -> Result<(libc::sockaddr_un, libc::socklen_t)> {
        // SAFETY: `sockaddr_un` is a plain-old-data C struct.
        let mut addr: libc::sockaddr_un = unsafe { zeroed() };
        addr.sun_family = libc::AF_UNIX as libc::sa_family_t;
        let base = size_of::<libc::sa_family_t>() as libc::socklen_t;
        match self {
            Self::Path(path) => {
                let bytes = path.as_os_str().as_encoded_bytes();
                if bytes.len() + 1 > addr.sun_path.len() {
                    return Err(Error::invalid_config(format!(
                        "endpoint path must be shorter than {} bytes",
                        addr.sun_path.len()
                    )));
                }
                // SAFETY: bounds checked above; destination is a local struct.
                unsafe {
                    ptr::copy_nonoverlapping(
                        bytes.as_ptr(),
                        addr.sun_path.as_mut_ptr().cast::<u8>(),
                        bytes.len(),
                    );
                }
                let len = base + bytes.len() as libc::socklen_t + 1;
                Ok((addr, len))
            }
            Self::Abstract(name) => {
                if name.len() + 1 > addr.sun_path.len() {
                    return Err(Error::invalid_config("abstract endpoint name is too long"));
                }
                addr.sun_path[0] = 0;
                // SAFETY: bounds checked above.
                unsafe {
                    ptr::copy_nonoverlapping(
                        name.as_ptr(),
                        addr.sun_path.as_mut_ptr().cast::<u8>().add(1),
                        name.len(),
                    );
                }
                let len = base + 1 + name.len() as libc::socklen_t;
                Ok((addr, len))
            }
        }
    }
}

/// One end of a local socket.
pub(crate) struct Stream {
    fd: RawFd,
    kind: StreamKind,
}

// SAFETY: sending and receiving use raw descriptor operations that are safe to
// invoke from multiple threads. The protocol still expects one reader and one
// writer per direction.
unsafe impl Send for Stream {}
unsafe impl Sync for Stream {}

impl Stream {
    fn new(fd: RawFd, kind: StreamKind) -> Self {
        Self { fd, kind }
    }

    pub(crate) fn shutdown(&self) {
        // SAFETY: shutdown never closes the descriptor.
        unsafe {
            libc::shutdown(self.fd, libc::SHUT_RDWR);
        }
    }

    pub(crate) fn send_all(&self, mut data: &[u8], timeout: Option<Duration>) -> Result<()> {
        let deadline = deadline_after(timeout);
        while !data.is_empty() {
            // SAFETY: `data` is a valid immutable slice.
            let sent = unsafe {
                libc::send(
                    self.fd,
                    data.as_ptr().cast::<libc::c_void>(),
                    data.len(),
                    SEND_FLAGS,
                )
            };
            if sent > 0 {
                data = &data[sent as usize..];
                continue;
            }
            if sent == 0 {
                return Err(Error::new(
                    ErrorKind::Disconnected,
                    "peer closed the socket",
                ));
            }
            let errno = last_errno();
            if errno == libc::EAGAIN || errno == libc::EWOULDBLOCK {
                wait_fd(self.fd, libc::POLLOUT, deadline)?;
                continue;
            }
            if errno == libc::EINTR {
                continue;
            }
            if errno == libc::EPIPE || errno == libc::ECONNRESET {
                return Err(Error::new(
                    ErrorKind::Disconnected,
                    "peer closed the socket",
                ));
            }
            return Err(os_error("send"));
        }
        Ok(())
    }

    /// Best-effort one-byte doorbell. A full socket buffer is fine: it already
    /// guarantees the peer has a pending wakeup.
    pub(crate) fn wake(&self) -> Result<()> {
        let byte = 1u8;
        loop {
            // SAFETY: `byte` is a valid one-byte buffer.
            let sent = unsafe {
                libc::send(
                    self.fd,
                    (&byte as *const u8).cast::<libc::c_void>(),
                    1,
                    SEND_FLAGS,
                )
            };
            if sent == 1 {
                return Ok(());
            }
            if sent < 0 {
                let errno = last_errno();
                if errno == libc::EAGAIN || errno == libc::EWOULDBLOCK {
                    return Ok(());
                }
                if errno == libc::EINTR {
                    continue;
                }
                if errno == libc::EPIPE || errno == libc::ECONNRESET {
                    return Err(Error::new(
                        ErrorKind::Disconnected,
                        "peer closed the socket",
                    ));
                }
                return Err(os_error("send wakeup"));
            }
            return Err(Error::new(ErrorKind::Internal, "zero-byte send on wakeup"));
        }
    }

    /// Wait for and consume one doorbell byte.
    pub(crate) fn wait_wake(&self, timeout: Option<Duration>) -> Result<()> {
        let deadline = deadline_after(timeout);
        let mut byte = [0u8; 1];
        loop {
            // SAFETY: `byte` is a valid one-byte buffer.
            let received = unsafe {
                libc::recv(
                    self.fd,
                    byte.as_mut_ptr().cast::<libc::c_void>(),
                    1,
                    libc::MSG_DONTWAIT,
                )
            };
            if received == 1 {
                return Ok(());
            }
            if received == 0 {
                return Err(Error::new(
                    ErrorKind::Disconnected,
                    "peer closed the socket",
                ));
            }
            if received < 0 {
                let errno = last_errno();
                if errno == libc::EAGAIN || errno == libc::EWOULDBLOCK {
                    wait_fd(self.fd, libc::POLLIN, deadline)?;
                    continue;
                }
                if errno == libc::EINTR {
                    continue;
                }
                if errno == libc::ECONNRESET {
                    return Err(Error::new(ErrorKind::Disconnected, "peer reset the socket"));
                }
                return Err(os_error("recv wakeup"));
            }
        }
    }

    /// Non-blocking read; returns `WouldBlock` when no byte is pending.
    pub(crate) fn try_read(&self, buf: &mut [u8]) -> Result<usize> {
        // SAFETY: `buf` is valid for writes of its own length.
        let received = unsafe {
            libc::recv(
                self.fd,
                buf.as_mut_ptr().cast::<libc::c_void>(),
                buf.len(),
                libc::MSG_DONTWAIT,
            )
        };
        if received >= 0 {
            return Ok(received as usize);
        }
        let errno = last_errno();
        if errno == libc::EAGAIN || errno == libc::EWOULDBLOCK {
            return Err(Error::new(ErrorKind::WouldBlock, "no data available"));
        }
        if errno == libc::EINTR {
            return Err(Error::new(ErrorKind::WouldBlock, "interrupted"));
        }
        if errno == libc::ECONNRESET {
            return Err(Error::new(ErrorKind::Disconnected, "peer reset the socket"));
        }
        Err(os_error("recv"))
    }

    /// Blocking read with a deadline; returns 0 on orderly shutdown.
    pub(crate) fn read_some(&self, buf: &mut [u8], timeout: Option<Duration>) -> Result<usize> {
        let deadline = deadline_after(timeout);
        loop {
            match self.try_read(buf) {
                Ok(n) => return Ok(n),
                Err(error) if error.kind() == ErrorKind::WouldBlock => {
                    wait_fd(self.fd, libc::POLLIN, deadline)?;
                }
                Err(error) => return Err(error),
            }
        }
    }

    pub(crate) fn send_handshake(
        &self,
        payload: &[u8],
        handle: Option<&ShmHandle>,
        timeout: Option<Duration>,
    ) -> Result<()> {
        let mut frame = Vec::with_capacity(4 + payload.len());
        frame.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        frame.extend_from_slice(payload);
        let deadline = deadline_after(timeout);
        let mut offset = 0;
        let mut first = true;
        while offset < frame.len() {
            let fd = if first {
                handle.map(|handle| handle.fd.as_raw_fd())
            } else {
                None
            };
            let sent = self.sendmsg(&frame[offset..], fd, deadline)?;
            if sent == 0 {
                return Err(Error::new(
                    ErrorKind::Disconnected,
                    "peer closed the socket",
                ));
            }
            offset += sent;
            first = false;
        }
        Ok(())
    }

    pub(crate) fn recv_handshake(
        &self,
        timeout: Option<Duration>,
    ) -> Result<(Vec<u8>, Option<ShmHandle>)> {
        let deadline = deadline_after(timeout);
        if self.kind == StreamKind::SeqPacket {
            let mut buffer = vec![0u8; HANDSHAKE_LIMIT];
            let (received, fd) = self.recvmsg(&mut buffer, deadline)?;
            if received < 4 {
                return Err(Error::protocol("handshake datagram is truncated"));
            }
            let len = u32::from_le_bytes(buffer[0..4].try_into().unwrap()) as usize;
            if len > HANDSHAKE_LIMIT - 4 || received != 4 + len {
                return Err(Error::protocol("handshake datagram has an invalid length"));
            }
            Ok((buffer[4..4 + len].to_vec(), fd.map(ShmHandle::new)))
        } else {
            let mut header = [0u8; 4];
            let mut fd = None;
            self.read_exact_handshake(&mut header, &mut fd, deadline)?;
            let len = u32::from_le_bytes(header) as usize;
            if len > HANDSHAKE_LIMIT - 4 {
                return Err(Error::protocol("handshake frame is too large"));
            }
            let mut payload = vec![0u8; len];
            self.read_exact_handshake(&mut payload, &mut fd, deadline)?;
            Ok((payload, fd.map(ShmHandle::new)))
        }
    }

    fn read_exact_handshake(
        &self,
        mut buf: &mut [u8],
        fd: &mut Option<OwnedFd>,
        deadline: Option<Instant>,
    ) -> Result<()> {
        while !buf.is_empty() {
            let (received, received_fd) = self.recvmsg(buf, deadline)?;
            if received == 0 {
                return Err(Error::new(
                    ErrorKind::Disconnected,
                    "peer closed the socket",
                ));
            }
            if received_fd.is_some() {
                *fd = received_fd;
            }
            buf = &mut buf[received..];
        }
        Ok(())
    }

    fn sendmsg(&self, data: &[u8], fd: Option<RawFd>, deadline: Option<Instant>) -> Result<usize> {
        let mut iov = libc::iovec {
            iov_base: data.as_ptr() as *mut libc::c_void,
            iov_len: data.len(),
        };
        // SAFETY: `msghdr` is plain-old-data; fields are filled below.
        let mut msg: libc::msghdr = unsafe { zeroed() };
        msg.msg_iov = &mut iov;
        msg.msg_iovlen = 1;
        let mut control = [0u64; 8];
        if let Some(fd) = fd {
            msg.msg_control = control.as_mut_ptr().cast::<libc::c_void>();
            msg.msg_controllen =
                unsafe { libc::CMSG_SPACE(size_of::<RawFd>() as libc::c_uint) } as _;
            // SAFETY: the control buffer is large enough for one `SCM_RIGHTS` header.
            unsafe {
                let cmsg = libc::CMSG_FIRSTHDR(&msg);
                (*cmsg).cmsg_level = libc::SOL_SOCKET;
                (*cmsg).cmsg_type = libc::SCM_RIGHTS;
                (*cmsg).cmsg_len = libc::CMSG_LEN(size_of::<RawFd>() as libc::c_uint) as _;
                ptr::copy_nonoverlapping(fd_ptr(&fd), libc::CMSG_DATA(cmsg).cast::<RawFd>(), 1);
            }
        }
        loop {
            // SAFETY: `msg` describes valid iovec and control buffers.
            let sent = unsafe { libc::sendmsg(self.fd, &msg, SEND_FLAGS) };
            if sent >= 0 {
                return Ok(sent as usize);
            }
            let errno = last_errno();
            if errno == libc::EAGAIN || errno == libc::EWOULDBLOCK {
                wait_fd(self.fd, libc::POLLOUT, deadline)?;
                continue;
            }
            if errno == libc::EINTR {
                continue;
            }
            if errno == libc::EPIPE || errno == libc::ECONNRESET {
                return Err(Error::new(
                    ErrorKind::Disconnected,
                    "peer closed the socket",
                ));
            }
            return Err(os_error("sendmsg"));
        }
    }

    fn recvmsg(
        &self,
        buf: &mut [u8],
        deadline: Option<Instant>,
    ) -> Result<(usize, Option<OwnedFd>)> {
        let mut iov = libc::iovec {
            iov_base: buf.as_mut_ptr() as *mut libc::c_void,
            iov_len: buf.len(),
        };
        // SAFETY: `msghdr` is plain-old-data; fields are filled below.
        let mut msg: libc::msghdr = unsafe { zeroed() };
        msg.msg_iov = &mut iov;
        msg.msg_iovlen = 1;
        let mut control = [0u64; 8];
        loop {
            msg.msg_control = control.as_mut_ptr().cast::<libc::c_void>();
            msg.msg_controllen = size_of::<[u64; 8]>() as _;
            // SAFETY: `msg` describes valid writable buffers.
            let received = unsafe { libc::recvmsg(self.fd, &mut msg, libc::MSG_DONTWAIT) };
            if received >= 0 {
                let mut received_fd = None;
                // SAFETY: control buffer was filled by `recvmsg` and the length
                // is validated by the CMSG iteration helpers.
                unsafe {
                    let mut cmsg = libc::CMSG_FIRSTHDR(&msg);
                    while !cmsg.is_null() {
                        if (*cmsg).cmsg_level == libc::SOL_SOCKET
                            && (*cmsg).cmsg_type == libc::SCM_RIGHTS
                        {
                            let raw = ptr::read_unaligned(libc::CMSG_DATA(cmsg).cast::<RawFd>());
                            received_fd = Some(OwnedFd::from_raw_fd(raw));
                        }
                        cmsg = libc::CMSG_NXTHDR(&msg, cmsg);
                    }
                }
                return Ok((received as usize, received_fd));
            }
            let errno = last_errno();
            if errno == libc::EAGAIN || errno == libc::EWOULDBLOCK {
                wait_fd(self.fd, libc::POLLIN, deadline)?;
                continue;
            }
            if errno == libc::EINTR {
                continue;
            }
            if errno == libc::ECONNRESET {
                return Err(Error::new(ErrorKind::Disconnected, "peer reset the socket"));
            }
            return Err(os_error("recvmsg"));
        }
    }
}

fn fd_ptr(fd: &RawFd) -> *const RawFd {
    fd as *const RawFd
}

impl Drop for Stream {
    fn drop(&mut self) {
        // SAFETY: descriptor is owned by this stream and closed exactly once.
        unsafe {
            libc::close(self.fd);
        }
    }
}

/// Create a connected pair of sockets for in-process channels.
pub(crate) fn socket_pair(prefer_seqpacket: bool) -> Result<(Stream, Stream)> {
    let kind = if prefer_seqpacket && SEQ_PACKET_SUPPORTED {
        StreamKind::SeqPacket
    } else {
        StreamKind::Stream
    };
    let mut fds = [0 as RawFd; 2];
    // SAFETY: `fds` points at space for two descriptors.
    let rc = unsafe { libc::socketpair(libc::AF_UNIX, kind.sock_type(), 0, fds.as_mut_ptr()) };
    if rc < 0 {
        return Err(os_error("socketpair"));
    }
    configure_stream(fds[0]).map_err(|err| Error::io("configure socketpair", err))?;
    configure_stream(fds[1]).map_err(|err| Error::io("configure socketpair", err))?;
    Ok((Stream::new(fds[0], kind), Stream::new(fds[1], kind)))
}

/// A shared-memory file descriptor that can be passed to the peer.
pub(crate) struct ShmHandle {
    fd: OwnedFd,
}

impl ShmHandle {
    fn new(fd: OwnedFd) -> Self {
        Self { fd }
    }

    /// Unix passes the mapping as a file descriptor, so no name is needed.
    pub(crate) fn name(&self) -> &str {
        ""
    }
}

/// A mapped shared-memory region.
pub(crate) struct Map {
    ptr: std::ptr::NonNull<u8>,
    len: usize,
}

// SAFETY: the mapping is process-wide memory; access is synchronized by the
// ring protocol implemented on top of it.
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
        // SAFETY: the pointer/len pair came from a successful `mmap`.
        unsafe {
            libc::munmap(self.ptr.as_ptr().cast::<libc::c_void>(), self.len);
        }
    }
}

#[cfg(not(target_os = "android"))]
static SHM_COUNTER: AtomicU64 = AtomicU64::new(0);

fn create_shm_fd() -> Result<RawFd> {
    #[cfg(any(target_os = "linux", target_os = "android"))]
    {
        let name = c"maple-scenary";
        // SAFETY: valid NUL-terminated name.
        let fd = unsafe { libc::memfd_create(name.as_ptr(), libc::MFD_CLOEXEC) };
        if fd >= 0 {
            return Ok(fd);
        }
        let errno = last_errno();
        if errno != libc::ENOSYS {
            return Err(os_error("memfd_create"));
        }
        // Android does not expose `shm_open` through `libc`; every supported
        // Android release has `memfd_create`, so treat ENOSYS as unsupported.
        #[cfg(target_os = "android")]
        return Err(os_error("memfd_create"));
    }
    #[cfg(not(target_os = "android"))]
    open_named_shm()
}

#[cfg(not(target_os = "android"))]
fn open_named_shm() -> Result<RawFd> {
    // Keep the name short: Darwin limits shared-memory names to 31 bytes.
    let salt = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.subsec_nanos() as u64)
        .unwrap_or(0);
    let unique = format!(
        "/mpl{:x}{:x}{:x}",
        std::process::id(),
        SHM_COUNTER.fetch_add(1, Ordering::Relaxed),
        salt & 0xffff
    );
    let name = std::ffi::CString::new(unique)
        .map_err(|_| Error::new(ErrorKind::Internal, "invalid shm name"))?;
    // SAFETY: valid NUL-terminated name.
    let fd = unsafe {
        libc::shm_open(
            name.as_ptr(),
            libc::O_CREAT | libc::O_EXCL | libc::O_RDWR,
            0o600,
        )
    };
    if fd < 0 {
        return Err(os_error("shm_open"));
    }
    // Unlink immediately: the descriptor keeps the object alive.
    // SAFETY: valid name pointer.
    unsafe {
        libc::shm_unlink(name.as_ptr());
    }
    Ok(fd)
}

fn map_fd(fd: RawFd, len: usize) -> Result<Map> {
    // SAFETY: `fd` is an open shared-memory descriptor of at least `len` bytes.
    let ptr = unsafe {
        libc::mmap(
            ptr::null_mut(),
            len,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_SHARED,
            fd,
            0,
        )
    };
    if ptr == libc::MAP_FAILED {
        return Err(os_error("mmap"));
    }
    Ok(Map {
        // SAFETY: `mmap` returned a non-null pointer distinct from MAP_FAILED.
        ptr: unsafe { std::ptr::NonNull::new_unchecked(ptr.cast::<u8>()) },
        len,
    })
}

pub(crate) fn create_shm(len: usize) -> Result<(Map, ShmHandle)> {
    let fd = create_shm_fd()?;
    let guard = FdGuard(fd);
    // SAFETY: `fd` is a writable shared-memory descriptor.
    if unsafe { libc::ftruncate(fd, len as libc::off_t) } < 0 {
        return Err(os_error("ftruncate"));
    }
    let map = map_fd(fd, len)?;
    Ok((map, ShmHandle::new(guard.into_owned())))
}

impl FdGuard {
    fn into_owned(self) -> OwnedFd {
        let fd = self.0;
        std::mem::forget(self);
        // SAFETY: ownership of the descriptor is transferred exactly once.
        unsafe { OwnedFd::from_raw_fd(fd) }
    }
}

pub(crate) fn attach_shm(handle: ShmHandle, len: usize) -> Result<Map> {
    map_fd(handle.fd.as_raw_fd(), len)
}
