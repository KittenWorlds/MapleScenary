//! C ABI (`cdylib` / `staticlib`) for non-Rust hosts.
//!
//! The ABI is deliberately small: create channels, send bytes, receive
//! borrowed messages, free them. Every entry point returns `0` on success or a
//! negative code matching [`ErrorKind::ffi_code`]. The last failure message is
//! available through [`maple_last_error_message`].
//!
//! ```c
//! // From C:
//! //   maple_config cfg; maple_config_default(&cfg);
//! //   maple_channel *a, *b;
//! //   maple_pair(&cfg, &a, &b);
//! //   maple_send(a, 1, "hi", 2);
//! //   maple_message *msg;
//! //   maple_recv(b, 1000, &msg);
//! ```

use std::cell::RefCell;
use std::ffi::{CStr, CString, c_char};
use std::time::Duration;

use crate::channel::{Channel, Message};
use crate::config::Config;
use crate::error::{Error, ErrorKind, Result};
use crate::listener::Listener;

/// Millisecond value meaning "block until a message arrives".
pub const MAPLE_WAIT_INFINITE: u64 = u64::MAX;

/// C configuration mirror. Always initialize it with
/// [`maple_config_default`] before overriding fields.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct MapleConfig {
    pub region_len: u64,
    pub ring_capacity: u32,
    pub min_unit: u32,
    pub max_message_len: u64,
    pub force_stream: u8,
    pub _padding: [u8; 7],
}

impl MapleConfig {
    fn to_config(self) -> Result<Config> {
        let config = Config {
            region_len: usize::try_from(self.region_len)
                .map_err(|_| Error::invalid_config("region_len does not fit in usize"))?,
            ring_capacity: self.ring_capacity,
            min_unit: self.min_unit as usize,
            max_message_len: usize::try_from(self.max_message_len)
                .map_err(|_| Error::invalid_config("max_message_len does not fit in usize"))?,
            force_stream: self.force_stream != 0,
        };
        config.validate()?;
        Ok(config)
    }
}

thread_local! {
    static LAST_ERROR: RefCell<CString> = RefCell::new(CString::new("").expect("static C string"));
}

fn set_error(message: impl Into<String>) {
    let mut bytes = message.into().into_bytes();
    bytes.retain(|byte| *byte != 0);
    LAST_ERROR.with(|slot| {
        *slot.borrow_mut() = CString::new(bytes).unwrap_or_default();
    });
}

fn run(action: impl FnOnce() -> Result<()>) -> i32 {
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(action)) {
        Ok(Ok(())) => 0,
        Ok(Err(error)) => {
            set_error(error.to_string());
            error.kind().ffi_code()
        }
        Err(_) => {
            set_error("panic inside MapleScenary");
            ErrorKind::Internal.ffi_code()
        }
    }
}

fn config_from(ptr: *const MapleConfig) -> Result<Config> {
    if ptr.is_null() {
        return Ok(Config::default());
    }
    // SAFETY: the caller guarantees a readable `MapleConfig` pointer.
    unsafe { (*ptr).to_config() }
}

fn timeout_from(millis: u64) -> Option<Duration> {
    match millis {
        MAPLE_WAIT_INFINITE => None,
        0 => Some(Duration::ZERO),
        other => Some(Duration::from_millis(other)),
    }
}

/// Fill `out` with the default configuration.
///
/// # Safety
/// `out` must be a valid writable pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn maple_config_default(out: *mut MapleConfig) {
    if out.is_null() {
        return;
    }
    let config = Config::default();
    let value = MapleConfig {
        region_len: config.region_len as u64,
        ring_capacity: config.ring_capacity,
        min_unit: config.min_unit as u32,
        max_message_len: config.max_message_len as u64,
        force_stream: u8::from(config.force_stream),
        _padding: [0; 7],
    };
    // SAFETY: `out` was checked and is writable by contract.
    unsafe { out.write(value) };
}

/// Return the ABI revision understood by this build.
#[unsafe(no_mangle)]
pub extern "C" fn maple_abi_version() -> u32 {
    crate::ABI_VERSION
}

/// Create two connected channels in the current process.
///
/// # Safety
/// `out_a` and `out_b` must be valid writable pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn maple_pair(
    config: *const MapleConfig,
    out_a: *mut *mut MapleChannel,
    out_b: *mut *mut MapleChannel,
) -> i32 {
    if out_a.is_null() || out_b.is_null() {
        set_error("maple_pair received a null output pointer");
        return ErrorKind::InvalidConfig.ffi_code();
    }
    run(|| {
        let config = config_from(config)?;
        let (a, b) = crate::pair(config)?;
        // SAFETY: outputs were checked non-null.
        unsafe {
            out_a.write(Box::into_raw(Box::new(MapleChannel(a))));
            out_b.write(Box::into_raw(Box::new(MapleChannel(b))));
        }
        Ok(())
    })
}

/// Bind a listening endpoint.
///
/// # Safety
/// `name` must be a NUL-terminated UTF-8 string and `out` a writable pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn maple_listen(
    name: *const c_char,
    config: *const MapleConfig,
    out: *mut *mut MapleListener,
) -> i32 {
    if name.is_null() || out.is_null() {
        set_error("maple_listen received a null pointer");
        return ErrorKind::InvalidConfig.ffi_code();
    }
    run(|| {
        // SAFETY: `name` is NUL-terminated by contract.
        let name = unsafe { CStr::from_ptr(name) }
            .to_str()
            .map_err(|_| Error::invalid_config("endpoint name is not UTF-8"))?;
        let config = config_from(config)?;
        let listener = Listener::bind(name, config)?;
        // SAFETY: `out` was checked non-null.
        unsafe { out.write(Box::into_raw(Box::new(MapleListener(listener)))) };
        Ok(())
    })
}

/// Accept one connection from a listener.
///
/// # Safety
/// `listener` and `out` must be valid pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn maple_accept(
    listener: *const MapleListener,
    timeout_ms: u64,
    out: *mut *mut MapleChannel,
) -> i32 {
    if listener.is_null() || out.is_null() {
        set_error("maple_accept received a null pointer");
        return ErrorKind::InvalidConfig.ffi_code();
    }
    run(|| {
        // SAFETY: pointer checked above.
        let listener = unsafe { &*listener };
        let channel = listener.0.accept(timeout_from(timeout_ms))?;
        // SAFETY: `out` was checked non-null.
        unsafe { out.write(Box::into_raw(Box::new(MapleChannel(channel)))) };
        Ok(())
    })
}

/// Connect to a listening endpoint.
///
/// # Safety
/// `name` must be NUL-terminated and `out` a writable pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn maple_connect(
    name: *const c_char,
    config: *const MapleConfig,
    out: *mut *mut MapleChannel,
) -> i32 {
    if name.is_null() || out.is_null() {
        set_error("maple_connect received a null pointer");
        return ErrorKind::InvalidConfig.ffi_code();
    }
    run(|| {
        // SAFETY: `name` is NUL-terminated by contract.
        let name = unsafe { CStr::from_ptr(name) }
            .to_str()
            .map_err(|_| Error::invalid_config("endpoint name is not UTF-8"))?;
        let config = config_from(config)?;
        let channel = crate::connect(name, config)?;
        // SAFETY: `out` was checked non-null.
        unsafe { out.write(Box::into_raw(Box::new(MapleChannel(channel)))) };
        Ok(())
    })
}

/// Send one payload.
///
/// # Safety
/// `channel` must be valid; `data` must be readable for `len` bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn maple_send(
    channel: *const MapleChannel,
    kind: u32,
    data: *const u8,
    len: usize,
) -> i32 {
    if channel.is_null() || (len != 0 && data.is_null()) {
        set_error("maple_send received a null pointer");
        return ErrorKind::InvalidConfig.ffi_code();
    }
    run(|| {
        // SAFETY: pointer and length were checked.
        let payload = unsafe { std::slice::from_raw_parts(data, len) };
        // SAFETY: pointer checked above.
        unsafe { &*channel }.0.send(kind, payload)
    })
}

/// Receive one message. The caller owns `*out` and must free it with
/// [`maple_message_free`].
///
/// # Safety
/// `channel` and `out` must be valid pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn maple_recv(
    channel: *const MapleChannel,
    timeout_ms: u64,
    out: *mut *mut MapleMessage,
) -> i32 {
    if channel.is_null() || out.is_null() {
        set_error("maple_recv received a null pointer");
        return ErrorKind::InvalidConfig.ffi_code();
    }
    run(|| {
        // SAFETY: pointers checked above.
        let channel = unsafe { &*channel };
        let message = match timeout_from(timeout_ms) {
            Some(timeout) if timeout.is_zero() => channel
                .0
                .try_recv()?
                .ok_or_else(|| Error::new(ErrorKind::Timeout, "no message available"))?,
            timeout => channel.0.recv(timeout)?,
        };
        // SAFETY: `out` was checked non-null.
        unsafe { out.write(Box::into_raw(Box::new(MapleMessage(message)))) };
        Ok(())
    })
}

/// Kind of a received message.
///
/// # Safety
/// `message` must be a pointer returned by [`maple_recv`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn maple_message_kind(message: *const MapleMessage) -> u32 {
    if message.is_null() {
        return 0;
    }
    // SAFETY: pointer is valid by contract.
    unsafe { &*message }.0.kind()
}

/// Payload length in bytes.
///
/// # Safety
/// `message` must be a pointer returned by [`maple_recv`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn maple_message_len(message: *const MapleMessage) -> usize {
    if message.is_null() {
        return 0;
    }
    // SAFETY: pointer is valid by contract.
    unsafe { &*message }.0.len()
}

/// Borrowed payload pointer, valid until [`maple_message_free`].
///
/// # Safety
/// `message` must be a pointer returned by [`maple_recv`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn maple_message_data(message: *const MapleMessage) -> *const u8 {
    if message.is_null() {
        return std::ptr::null();
    }
    // SAFETY: pointer is valid by contract.
    unsafe { &*message }.0.bytes().as_ptr()
}

/// Release a message.
///
/// # Safety
/// `message` must be null or a pointer returned by [`maple_recv`] that has not
/// been freed yet.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn maple_message_free(message: *mut MapleMessage) {
    if !message.is_null() {
        // SAFETY: ownership was transferred by `maple_recv`.
        drop(unsafe { Box::from_raw(message) });
    }
}

/// Release a channel.
///
/// # Safety
/// `channel` must be null or a pointer returned by one of the constructors and
/// not freed yet.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn maple_channel_free(channel: *mut MapleChannel) {
    if !channel.is_null() {
        // SAFETY: ownership was transferred by a constructor.
        drop(unsafe { Box::from_raw(channel) });
    }
}

/// Release a listener.
///
/// # Safety
/// `listener` must be null or a pointer returned by [`maple_listen`] and not
/// freed yet.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn maple_listener_free(listener: *mut MapleListener) {
    if !listener.is_null() {
        // SAFETY: ownership was transferred by `maple_listen`.
        drop(unsafe { Box::from_raw(listener) });
    }
}

/// Last failure message for the current thread. Valid until the next call into
/// this library on the same thread.
#[unsafe(no_mangle)]
pub extern "C" fn maple_last_error_message() -> *const c_char {
    LAST_ERROR.with(|slot| slot.borrow().as_ptr())
}

/// Opaque channel handle.
pub struct MapleChannel(Channel);
/// Opaque listener handle.
pub struct MapleListener(Listener);
/// Opaque message handle.
pub struct MapleMessage(Message);
