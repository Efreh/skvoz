use std::os::fd::OwnedFd;

pub(crate) const JSON_MAX: usize = 32_768;

pub(crate) fn duplicate(fd: i32) -> Result<Option<OwnedFd>, ()> {
    match fd {
        -1 => Ok(None),
        0.. => skvoz_network_native::duplicate_inherited(fd)
            .map(Some)
            .map_err(|_| ()),
        _ => Err(()),
    }
}

pub(crate) fn output_valid<T>(ptr: *mut T) -> bool {
    !ptr.is_null()
        && (ptr as usize).is_multiple_of(std::mem::align_of::<T>())
        && range_valid(ptr as usize, std::mem::size_of::<T>())
}

pub(crate) fn range_valid(address: usize, length: usize) -> bool {
    length <= isize::MAX as usize && address.checked_add(length).is_some()
}

pub(crate) fn disjoint(a: usize, a_len: usize, b: usize, b_len: usize) -> bool {
    match (a.checked_add(a_len), b.checked_add(b_len)) {
        (Some(a_end), Some(b_end)) => a_len == 0 || b_len == 0 || a_end <= b || b_end <= a,
        _ => false,
    }
}

/// Copy the bounded input while the caller still owns its storage.
///
/// # Safety
/// The caller must provide readable, initialized bytes in one allocation for
/// the full input size, without concurrent mutation for the duration of the call.
pub(crate) unsafe fn input(ptr: *const u8, length: usize) -> Result<Vec<u8>, ()> {
    if ptr.is_null() || length == 0 || length > JSON_MAX || !range_valid(ptr as usize, length) {
        return Err(());
    }
    // SAFETY: the caller guarantees readability; nonnull and finite length were
    // checked before creating the slice. No caller pointer survives this call.
    Ok(unsafe { std::slice::from_raw_parts(ptr, length) }.to_vec())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::os::fd::AsRawFd;
    use std::os::unix::net::UnixStream;

    #[test]
    fn duplicate_retains_original_and_owns_a_distinct_descriptor() {
        let (mut original, mut peer) = UnixStream::pair().unwrap();
        let copied = duplicate(original.as_raw_fd()).unwrap().unwrap();
        assert_ne!(copied.as_raw_fd(), original.as_raw_fd());
        let descriptor =
            std::fs::read_to_string(format!("/proc/self/fdinfo/{}", copied.as_raw_fd())).unwrap();
        let flags = descriptor
            .lines()
            .find_map(|line| line.strip_prefix("flags:\t"))
            .map(|value| u32::from_str_radix(value, 8).unwrap())
            .unwrap();
        assert_ne!(flags & 0o2_000_000, 0, "O_CLOEXEC must be set");
        drop(copied);
        original.write_all(b"x").unwrap();
        let mut received = [0];
        peer.read_exact(&mut received).unwrap();
        assert_eq!(received, *b"x");
        drop(original);
        assert_eq!(peer.read(&mut [0]).unwrap(), 0);
        assert!(duplicate(-2).is_err());
        assert!(duplicate(i32::MAX).is_err());
        assert!(duplicate(-1).unwrap().is_none());
    }

    #[test]
    fn bounded_input_is_copied_and_invalid_ranges_rejected() {
        let bytes = b"{}";
        // SAFETY: bytes is readable for its stated length. Invalid ranges are
        // rejected before dereferencing and deliberately use a null pointer.
        unsafe {
            let copy = input(bytes.as_ptr(), bytes.len()).unwrap();
            assert_eq!(copy, bytes);
            assert_ne!(copy.as_ptr(), bytes.as_ptr());
            assert!(input(std::ptr::null(), 1).is_err());
            assert!(input(bytes.as_ptr(), 0).is_err());
            assert!(input(bytes.as_ptr(), JSON_MAX + 1).is_err());
        }
        let mut output = 0u64;
        assert!(output_valid(&mut output));
        assert!(!output_valid::<u64>(std::ptr::null_mut()));
        let misaligned = std::ptr::addr_of_mut!(output)
            .cast::<u8>()
            .wrapping_add(1)
            .cast::<u64>();
        assert!(!output_valid(misaligned));
        assert!(!output_valid::<u64>((usize::MAX - 7) as *mut u64));
        // No invalid pointer is dereferenced: checked arithmetic rejects it.
        assert!(unsafe { input(usize::MAX as *const u8, 2) }.is_err());
    }

    #[test]
    fn disjoint_regions_use_checked_arithmetic() {
        assert!(disjoint(10, 2, 12, 4));
        assert!(disjoint(12, 4, 10, 2));
        assert!(!disjoint(10, 3, 12, 4));
        assert!(!disjoint(usize::MAX, 2, 0, 1));
        assert!(disjoint(10, 0, 10, 4));
        assert!(!range_valid(1, usize::MAX));
    }
}
