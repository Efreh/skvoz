#![cfg(target_os = "linux")]
use skvoz_network::local_api::parse_strict_json;
use skvoz_network_ffi::*;
use std::io::{Read, Write};
use std::os::fd::AsRawFd;
use std::os::unix::net::UnixStream;
use std::time::{Duration, Instant};

const CONFIG: &[u8] = include_bytes!("../../tests/fixtures/client-startup.json");
const HELLO: &[u8] =
    b"{\"v\":1,\"id\":1,\"op\":\"HELLO\",\"args\":{\"api\":1,\"network\":3},\"fd_count\":0}";

struct Owner(u64);
impl Owner {
    fn create() -> Self {
        let mut handle = 0;
        // SAFETY: input/output are owned, disjoint, and correctly sized/aligned.
        let status =
            unsafe { skvoz_network_create(CONFIG.as_ptr(), CONFIG.len(), -1, &mut handle) };
        assert_eq!(status, SUCCESS);
        assert_ne!(handle, 0);
        Self(handle)
    }
}
impl Drop for Owner {
    fn drop(&mut self) {
        if self.0 != 0 {
            // Cleanup must not panic while a failed assertion is unwinding.
            let _ = skvoz_network_destroy(self.0);
        }
    }
}

#[test]
fn hello_remains_responsive_without_a_broker_and_small_buffer_retains_message() {
    let owner = Owner::create();
    let mut id = 0;
    // SAFETY: pointers are valid local buffers; one thread owns the handle.
    assert_eq!(
        unsafe { skvoz_network_request(owner.0, HELLO.as_ptr(), HELLO.len(), -1, &mut id) },
        SUCCESS
    );
    assert_eq!(id, 1);
    let deadline = Instant::now() + Duration::from_secs(1);
    let mut found = false;
    while Instant::now() < deadline {
        let mut required = 0;
        let mut fd = 0;
        // SAFETY: null buffer with capacity0 is explicitly a size query.
        let status = unsafe {
            skvoz_network_next_event(
                owner.0,
                std::ptr::null_mut(),
                0,
                &mut required,
                &mut fd,
                100,
            )
        };
        if status == TIMEOUT {
            continue;
        }
        assert_eq!(status, INSUFFICIENT_BUFFER);
        assert!((1..=32768).contains(&required));
        assert_eq!(fd, -1);
        let mut second_size = 0;
        // SAFETY: repeated query retains the same actor-owned message.
        assert_eq!(
            unsafe {
                skvoz_network_next_event(
                    owner.0,
                    std::ptr::null_mut(),
                    0,
                    &mut second_size,
                    &mut fd,
                    0,
                )
            },
            INSUFFICIENT_BUFFER
        );
        assert_eq!(second_size, required);
        let mut buffer = vec![0; required];
        // SAFETY: buffer has exactly the advertised capacity, outputs disjoint.
        assert_eq!(
            unsafe {
                skvoz_network_next_event(
                    owner.0,
                    buffer.as_mut_ptr(),
                    buffer.len(),
                    &mut second_size,
                    &mut fd,
                    0,
                )
            },
            SUCCESS
        );
        assert_eq!(second_size, required);
        assert_eq!(fd, -1);
        let message = parse_strict_json(&buffer).unwrap();
        if message.get("id").and_then(|id| id.as_u64()) == Some(1) {
            assert!(message["error"].is_null());
            assert_eq!(message["result"]["api"], 1);
            found = true;
            break;
        }
    }
    assert!(
        found,
        "HELLO waited for unavailable NATS instead of local admission"
    );
}

#[test]
fn destroyed_generation_is_stale_even_after_slot_reuse() {
    let mut first = Owner::create();
    let stale = first.0;
    assert_eq!(skvoz_network_destroy(stale), SUCCESS);
    first.0 = 0;
    let second = Owner::create();
    assert_ne!(stale, second.0);
    let mut id = 9;
    // SAFETY: owned input/output; stale handle is data, never a pointer.
    assert_eq!(
        unsafe { skvoz_network_request(stale, HELLO.as_ptr(), HELLO.len(), -1, &mut id) },
        CLOSED
    );
    assert_eq!(id, 0);
    assert_eq!(skvoz_network_destroy(stale), CLOSED);
}

#[test]
fn rejected_helper_duplicate_preserves_original() {
    let (mut original, mut peer) = UnixStream::pair().unwrap();
    let mut handle = 9;
    // SAFETY: the descriptor is borrowed, input/output are valid local storage.
    assert_eq!(
        unsafe {
            skvoz_network_create(
                CONFIG.as_ptr(),
                CONFIG.len(),
                original.as_raw_fd(),
                &mut handle,
            )
        },
        INVALID_ARGUMENT
    );
    assert_eq!(handle, 0);
    original.write_all(b"x").unwrap();
    let mut received = [0];
    peer.read_exact(&mut received).unwrap();
    assert_eq!(received, *b"x");
}

#[test]
fn rejected_request_duplicate_preserves_caller_descriptor() {
    let owner = Owner::create();
    let (mut original, mut peer) = UnixStream::pair().unwrap();
    let mut id = 9;
    // SAFETY: HELLO declares no FD. Its extra borrowed FD must be rejected and
    // only the duplicated descriptor is transferred to the runtime for cleanup.
    assert_eq!(
        unsafe {
            skvoz_network_request(
                owner.0,
                HELLO.as_ptr(),
                HELLO.len(),
                original.as_raw_fd(),
                &mut id,
            )
        },
        INVALID_ARGUMENT
    );
    assert_eq!(id, 0);
    original.write_all(b"x").unwrap();
    let mut received = [0];
    peer.read_exact(&mut received).unwrap();
    assert_eq!(received, *b"x");
}

#[test]
fn concurrent_destroy_retires_one_owner_exactly_once() {
    let mut owner = Owner::create();
    let handle = owner.0;
    owner.0 = 0;
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(3));
    let workers: Vec<_> = (0..2)
        .map(|_| {
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                skvoz_network_destroy(handle)
            })
        })
        .collect();
    barrier.wait();
    let mut statuses: Vec<_> = workers
        .into_iter()
        .map(|worker| worker.join().unwrap())
        .collect();
    statuses.sort_unstable();
    assert_eq!(statuses, [SUCCESS, CLOSED]);
    assert_eq!(skvoz_network_destroy(handle), CLOSED);
}

#[test]
fn malformed_owner_request_closes_poll_and_future_admission() {
    let mut owner = Owner::create();
    let malformed = b"{}";
    let mut id = 9;
    // SAFETY: all input/output storage is owned and disjoint.
    assert_eq!(
        unsafe { skvoz_network_request(owner.0, malformed.as_ptr(), malformed.len(), -1, &mut id) },
        INVALID_ARGUMENT
    );
    assert_eq!(id, 0);
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let mut bytes = [0u8; 32768];
        let mut length = 9;
        let mut fd = 9;
        // SAFETY: buffer and scalar outputs are valid disjoint local storage.
        let status = unsafe {
            skvoz_network_next_event(
                owner.0,
                bytes.as_mut_ptr(),
                bytes.len(),
                &mut length,
                &mut fd,
                100,
            )
        };
        assert_eq!(fd, -1);
        if status == CLOSED {
            assert_eq!(length, 0);
            break;
        }
        assert!(status == SUCCESS || status == TIMEOUT, "status {status}");
        assert!(Instant::now() < deadline, "owner poll did not close");
    }
    // SAFETY: terminal runtime admission uses valid input/output buffers.
    assert_eq!(
        unsafe { skvoz_network_request(owner.0, HELLO.as_ptr(), HELLO.len(), -1, &mut id) },
        CLOSED
    );
    assert_eq!(id, 0);
    assert_eq!(skvoz_network_destroy(owner.0), SUCCESS);
    assert_eq!(skvoz_network_destroy(owner.0), CLOSED);
    owner.0 = 0;
}

#[test]
fn concurrent_poll_and_destroy_leave_no_usable_owner() {
    let mut owner = Owner::create();
    let handle = owner.0;
    owner.0 = 0;
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
    let poll_barrier = barrier.clone();
    let poll = std::thread::spawn(move || {
        let mut bytes = [0; 32768];
        let mut length = 9;
        let mut fd = 9;
        poll_barrier.wait();
        // SAFETY: each output is owned by this thread and disjoint. The library
        // serializes the owner while another thread retires its generation.
        let status = unsafe {
            skvoz_network_next_event(
                handle,
                bytes.as_mut_ptr(),
                bytes.len(),
                &mut length,
                &mut fd,
                1000,
            )
        };
        assert_eq!(fd, -1);
        assert!(status == SUCCESS || status == TIMEOUT || status == CLOSED);
        if status != SUCCESS {
            assert_eq!(length, 0);
        }
    });
    barrier.wait();
    assert_eq!(skvoz_network_destroy(handle), SUCCESS);
    poll.join().unwrap();
    let mut id = 9;
    // SAFETY: retired handles are numeric data; caller storage is valid.
    assert_eq!(
        unsafe { skvoz_network_request(handle, HELLO.as_ptr(), HELLO.len(), -1, &mut id) },
        CLOSED
    );
    assert_eq!(id, 0);
}

#[test]
fn mandatory_nullable_config_keys_cannot_be_omitted() {
    let canonical = parse_strict_json(CONFIG).unwrap();
    for (parent, key) in [
        (None, "server"),
        (Some("core"), "ca_file"),
        (Some("core"), "tls_server_name"),
    ] {
        let mut value = canonical.clone();
        let object = match parent {
            None => value.as_object_mut().unwrap(),
            Some(parent) => value[parent].as_object_mut().unwrap(),
        };
        assert!(object.remove(key).is_some());
        let bytes = value.to_string().into_bytes();
        let mut handle = 9;
        // SAFETY: mutation is an owned finite JSON buffer and valid output.
        assert_eq!(
            unsafe { skvoz_network_create(bytes.as_ptr(), bytes.len(), -1, &mut handle) },
            INVALID_ARGUMENT,
            "missing {parent:?}.{key}"
        );
        assert_eq!(handle, 0);
    }
}
