#![cfg(target_os = "linux")]
use skvoz_network_ffi::*;

#[test]
fn invalid_ranges_outputs_and_stale_handles_are_rejected() {
    let mut handle = 99;
    let mut id = 99;
    let mut len = 99;
    let mut fd = 99;
    let json = b"{}";
    // SAFETY: every nonnull pointer is backed by correctly sized/aligned local
    // storage. Oversized/null input paths must reject before reading any bytes.
    unsafe {
        assert_eq!(skvoz_network_abi_version(), 1);
        assert_eq!(
            skvoz_network_create(std::ptr::null(), 1, -1, &mut handle),
            INVALID_ARGUMENT
        );
        assert_eq!(handle, 0);
        assert_eq!(
            skvoz_network_create(json.as_ptr(), 32769, -1, &mut handle),
            INVALID_ARGUMENT
        );
        assert_eq!(
            skvoz_network_create(json.as_ptr(), 2, -1, std::ptr::null_mut()),
            INVALID_ARGUMENT
        );
        assert_eq!(
            skvoz_network_request(0, json.as_ptr(), 2, -1, &mut id),
            CLOSED
        );
        assert_eq!(id, 0);
        assert_eq!(
            skvoz_network_request(0, json.as_ptr(), 2, -1, std::ptr::null_mut()),
            INVALID_ARGUMENT
        );
        assert_eq!(
            skvoz_network_next_event(0, std::ptr::null_mut(), 0, &mut len, &mut fd, 0),
            CLOSED
        );
        assert_eq!((len, fd), (0, -1));
        assert_eq!(
            skvoz_network_next_event(0, std::ptr::null_mut(), 1, &mut len, &mut fd, 0),
            INVALID_ARGUMENT
        );
        assert_eq!(
            skvoz_network_next_event(0, std::ptr::null_mut(), 0, &mut len, &mut fd, 1001),
            INVALID_ARGUMENT
        );
        assert_eq!(
            skvoz_network_next_event(0, std::ptr::null_mut(), 0, std::ptr::null_mut(), &mut fd, 0),
            INVALID_ARGUMENT
        );
        assert_eq!(skvoz_network_destroy(0), CLOSED);
        assert_eq!(skvoz_network_destroy(u64::MAX), CLOSED);
    }
}

#[test]
fn deterministic_malformed_input_corpus_never_admits_an_actor() {
    let mut corpus: Vec<Vec<u8>> = vec![
        b"null".to_vec(),
        b"[]".to_vec(),
        b"{}".to_vec(),
        b"{\"v\":1,\"v\":1}".to_vec(),
        b"{\"v\":2}".to_vec(),
        vec![0xff],
        vec![0; 32768],
        vec![b' '; 32768],
    ];
    for depth in 1..=160 {
        let mut input = vec![b'['; depth];
        input.extend(std::iter::repeat_n(b']', depth));
        corpus.push(input);
    }
    let mut state = 0x5a17_c3d9_u32;
    for size in [1, 2, 16, 128, 1024, 32768] {
        let bytes = (0..size)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 17;
                state ^= state << 5;
                state as u8
            })
            .collect();
        corpus.push(bytes);
    }
    for input in corpus {
        let mut handle = 99;
        // SAFETY: each input is owned and readable; output is aligned storage.
        let status = unsafe { skvoz_network_create(input.as_ptr(), input.len(), -1, &mut handle) };
        assert_eq!(status, INVALID_ARGUMENT, "input length {}", input.len());
        assert_eq!(handle, 0);
    }
}

#[test]
fn wrapping_and_overlapping_pointer_ranges_reject_before_reading() {
    let mut handle = 99u64;
    let mut length = 99usize;
    let mut fd = 99i32;
    // SAFETY: invalid numeric/overlapping ranges are checked before dereferencing
    // them; all writable scalar pointers refer to aligned owned storage.
    unsafe {
        assert_eq!(
            skvoz_network_create(usize::MAX as *const u8, 2, -1, &mut handle),
            INVALID_ARGUMENT
        );
        assert_eq!(handle, 0);
        let handle_ptr = &mut handle as *mut u64;
        assert_eq!(
            skvoz_network_create(handle_ptr.cast(), 8, -1, handle_ptr),
            INVALID_ARGUMENT
        );
        assert_eq!(
            skvoz_network_next_event(0, (usize::MAX - 1) as *mut u8, 4, &mut length, &mut fd, 0,),
            INVALID_ARGUMENT
        );
        let length_ptr = &mut length as *mut usize;
        assert_eq!(
            skvoz_network_next_event(
                0,
                length_ptr.cast(),
                std::mem::size_of::<usize>(),
                length_ptr,
                &mut fd,
                0,
            ),
            INVALID_ARGUMENT
        );
        assert_eq!(
            skvoz_network_next_event(0, std::ptr::null_mut(), 0, length_ptr, length_ptr.cast(), 0),
            INVALID_ARGUMENT
        );
    }
}
