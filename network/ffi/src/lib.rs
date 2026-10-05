//! Thin native boundary for the shared network actor.
#![deny(unsafe_op_in_unsafe_fn)]
#![cfg(target_os = "linux")]

mod boundary;
mod handles;

use handles::Handles;
use skvoz_network::config::StartupConfig;
use skvoz_network::runtime::{OwnedMessage, PollError, RuntimeFailure, RuntimeHandle};
use std::os::fd::IntoRawFd;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Mutex, MutexGuard, OnceLock};
use std::time::Duration;

pub const SUCCESS: u32 = 0;
pub const INSUFFICIENT_BUFFER: u32 = 1;
pub const TIMEOUT: u32 = 2;
pub const INVALID_ARGUMENT: u32 = 3;
pub const CLOSED: u32 = 4;
pub const INTERNAL: u32 = 5;

type Owner = Option<RuntimeHandle>;

fn registry() -> &'static Mutex<Handles<Owner>> {
    static REGISTRY: OnceLock<Mutex<Handles<Owner>>> = OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(Handles::default()))
}

fn lock<T>(value: &Mutex<T>) -> MutexGuard<'_, T> {
    value.lock().unwrap_or_else(|poison| poison.into_inner())
}

fn guarded(action: impl FnOnce() -> u32) -> u32 {
    catch_unwind(AssertUnwindSafe(action)).unwrap_or(INTERNAL)
}

fn failure(error: RuntimeFailure) -> u32 {
    match error {
        RuntimeFailure::InvalidArgument => INVALID_ARGUMENT,
        RuntimeFailure::Closed => CLOSED,
        RuntimeFailure::Overloaded | RuntimeFailure::Internal => INTERNAL,
    }
}

fn with_runtime(handle: u64, action: impl FnOnce(&mut RuntimeHandle) -> u32) -> u32 {
    let Some(owner) = lock(registry()).get(handle) else {
        return CLOSED;
    };
    let mut owner = lock(&owner);
    let Some(runtime) = owner.as_mut() else {
        return CLOSED;
    };
    match catch_unwind(AssertUnwindSafe(|| action(runtime))) {
        Ok(status) => status,
        Err(_) => {
            // Retire this owner rather than exposing a possibly partial operation
            // after panic. Runtime Drop performs its normal terminal cleanup.
            let runtime = owner.take();
            lock(registry()).remove(handle);
            drop(owner);
            drop(runtime);
            INTERNAL
        }
    }
}

/// # Safety
/// Output storage is checked, writable and disjoint as required by next_event.
unsafe fn deliver(
    message: OwnedMessage,
    out_buffer: *mut u8,
    capacity: usize,
    out_len: *mut usize,
    out_fd: *mut i32,
) -> u32 {
    if message.json.is_empty()
        || message.json.len() > boundary::JSON_MAX
        || message.json.len() > capacity
    {
        return INTERNAL;
    }
    // SAFETY: the caller supplies valid disjoint output storage and the message
    // length is within capacity. The owned Vec cannot alias output storage.
    unsafe {
        std::ptr::copy_nonoverlapping(message.json.as_ptr(), out_buffer, message.json.len());
        out_len.write(message.json.len());
        out_fd.write(message.fd.map_or(-1, IntoRawFd::into_raw_fd));
    }
    SUCCESS
}

#[unsafe(no_mangle)]
pub extern "C" fn skvoz_network_abi_version() -> u32 {
    1
}

/// Create the same bounded Rust actor used by native embedding.
///
/// # Safety
/// Input points to `len` readable initialized bytes in one allocation, without
/// concurrent mutation during this call; `out_handle` points to a
/// writable aligned u64. The regions do not overlap. See the public C header.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn skvoz_network_create(
    config: *const u8,
    len: usize,
    helper_fd: i32,
    out_handle: *mut u64,
) -> u32 {
    guarded(|| {
        if !boundary::output_valid(out_handle) {
            return INVALID_ARGUMENT;
        }
        // SAFETY: output storage is supplied by the caller; alignment/nonnull
        // were checked. Reset it before any fallible admission operation.
        unsafe { out_handle.write(0) };
        if !boundary::disjoint(
            config as usize,
            len,
            out_handle as usize,
            std::mem::size_of::<u64>(),
        ) {
            return INVALID_ARGUMENT;
        }
        // SAFETY: input readability is part of this function's caller contract.
        let Ok(json) = (unsafe { boundary::input(config, len) }) else {
            return INVALID_ARGUMENT;
        };
        let Ok(config) = StartupConfig::parse_json(&json) else {
            return INVALID_ARGUMENT;
        };
        let Ok(helper) = boundary::duplicate(helper_fd) else {
            return INVALID_ARGUMENT;
        };
        let runtime = match RuntimeHandle::start(config, helper) {
            Ok(runtime) => runtime,
            Err(error) => return failure(error),
        };
        let Some(handle) = lock(registry()).insert(Some(runtime)) else {
            return INTERNAL;
        };
        // SAFETY: the checked caller-owned output remains valid for the call.
        unsafe { out_handle.write(handle) };
        SUCCESS
    })
}

/// Admit a copied API1 request; an optional input FD remains caller-owned.
///
/// # Safety
/// Input points to `len` readable initialized bytes in one allocation, without
/// concurrent mutation during this call; `out_request_id` points to
/// a writable aligned u32. Regions do not overlap. Calls for one handle serialize.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn skvoz_network_request(
    handle: u64,
    json: *const u8,
    len: usize,
    fd: i32,
    out_request_id: *mut u32,
) -> u32 {
    guarded(|| {
        if !boundary::output_valid(out_request_id) {
            return INVALID_ARGUMENT;
        }
        // SAFETY: caller guarantees writable output storage, checked nonnull.
        unsafe { out_request_id.write(0) };
        if !boundary::disjoint(
            json as usize,
            len,
            out_request_id as usize,
            std::mem::size_of::<u32>(),
        ) {
            return INVALID_ARGUMENT;
        }
        with_runtime(handle, |runtime| {
            // SAFETY: caller guarantees input readability for this call.
            let Ok(json) = (unsafe { boundary::input(json, len) }) else {
                return INVALID_ARGUMENT;
            };
            let Ok(fd) = boundary::duplicate(fd) else {
                return INVALID_ARGUMENT;
            };
            match runtime.request_json(&json, fd) {
                Ok(id) => {
                    // SAFETY: caller output storage stays valid for the call.
                    unsafe { out_request_id.write(id) };
                    SUCCESS
                }
                Err(error) => failure(error),
            }
        })
    })
}

/// Return one response/event and transfer its descriptor only on success.
///
/// # Safety
/// Nonnull buffer points to `capacity` writable bytes; null requires capacity0.
/// `out_len` and `out_fd` point to aligned writable values. All output regions
/// are disjoint. Calls for one handle serialize; destroy follows stopped polling.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn skvoz_network_next_event(
    handle: u64,
    out_buffer: *mut u8,
    capacity: usize,
    out_len: *mut usize,
    out_fd: *mut i32,
    timeout_ms: u32,
) -> u32 {
    guarded(|| {
        if !boundary::output_valid(out_len)
            || !boundary::output_valid(out_fd)
            || !boundary::disjoint(
                out_len as usize,
                std::mem::size_of::<usize>(),
                out_fd as usize,
                std::mem::size_of::<i32>(),
            )
        {
            return INVALID_ARGUMENT;
        }
        // SAFETY: caller supplies disjoint writable aligned output values.
        unsafe {
            out_len.write(0);
            out_fd.write(-1);
        }
        if timeout_ms > 1000
            || !boundary::range_valid(out_buffer as usize, capacity)
            || (out_buffer.is_null() && capacity != 0)
            || !boundary::disjoint(
                out_buffer as usize,
                capacity,
                out_len as usize,
                std::mem::size_of::<usize>(),
            )
            || !boundary::disjoint(
                out_buffer as usize,
                capacity,
                out_fd as usize,
                std::mem::size_of::<i32>(),
            )
        {
            return INVALID_ARGUMENT;
        }
        with_runtime(handle, |runtime| {
            match runtime.next_message(capacity, Duration::from_millis(u64::from(timeout_ms))) {
                Ok(message) => {
                    // SAFETY: output ranges were checked and the caller
                    // guarantees accessibility/exclusive ownership for the call.
                    unsafe { deliver(message, out_buffer, capacity, out_len, out_fd) }
                }
                Err(PollError::InsufficientBuffer { required }) => {
                    if required == 0 || required > boundary::JSON_MAX || required <= capacity {
                        return INTERNAL;
                    }
                    // SAFETY: output is caller-owned writable storage.
                    unsafe { out_len.write(required) };
                    INSUFFICIENT_BUFFER
                }
                Err(PollError::Timeout) => TIMEOUT,
                Err(PollError::Closed) => CLOSED,
                Err(PollError::Internal) => INTERNAL,
            }
        })
    })
}

/// Retire a generation handle and stop its actor, closing undelivered FDs.
#[unsafe(no_mangle)]
pub extern "C" fn skvoz_network_destroy(handle: u64) -> u32 {
    guarded(|| {
        let Some(owner) = lock(registry()).remove(handle) else {
            return CLOSED;
        };
        let Some(runtime) = lock(&owner).take() else {
            return CLOSED;
        };
        match runtime.shutdown() {
            Ok(()) => SUCCESS,
            Err(error) => failure(error),
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
    use std::os::unix::net::UnixStream;

    #[test]
    fn panic_does_not_cross_the_abi() {
        assert_eq!(guarded(|| panic!("boundary test")), INTERNAL);
    }

    #[test]
    fn message_delivery_transfers_fd_once_until_caller_closes_it() {
        let (socket, mut peer) = UnixStream::pair().unwrap();
        let expected = socket.as_raw_fd();
        let message = OwnedMessage {
            json: b"{}".to_vec(),
            fd: Some(socket.into()),
        };
        let mut bytes = [0; 2];
        let mut len = 0;
        let mut fd = -1;
        // SAFETY: all outputs are valid disjoint local storage.
        assert_eq!(
            unsafe { deliver(message, bytes.as_mut_ptr(), bytes.len(), &mut len, &mut fd) },
            SUCCESS
        );
        assert_eq!(bytes, *b"{}");
        assert_eq!(len, 2);
        assert_eq!(fd, expected);
        // SAFETY: deliver transferred this one descriptor to its caller exactly
        // once. This test now represents the C caller owning/closing the FD.
        let mut caller = UnixStream::from(unsafe { OwnedFd::from_raw_fd(fd) });
        caller.write_all(b"x").unwrap();
        let mut received = [0];
        peer.read_exact(&mut received).unwrap();
        assert_eq!(received, *b"x");
        drop(caller);
        assert_eq!(peer.read(&mut received).unwrap(), 0);
    }

    #[test]
    fn invalid_actor_delivery_closes_its_descriptor_without_exposing_it() {
        let (socket, mut peer) = UnixStream::pair().unwrap();
        let message = OwnedMessage {
            json: b"{}".to_vec(),
            fd: Some(socket.into()),
        };
        let mut bytes = [0; 1];
        let mut len = 0;
        let mut fd = -1;
        // SAFETY: valid local output regions; the message is larger than the
        // buffer and must reject before copying or transferring its descriptor.
        assert_eq!(
            unsafe { deliver(message, bytes.as_mut_ptr(), bytes.len(), &mut len, &mut fd) },
            INTERNAL
        );
        assert_eq!((len, fd), (0, -1));
        assert_eq!(peer.read(&mut [0]).unwrap(), 0);
    }
}
