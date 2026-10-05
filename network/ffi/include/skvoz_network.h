#ifndef SKVOZ_NETWORK_H
#define SKVOZ_NETWORK_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define SKVOZ_NETWORK_ABI_VERSION UINT32_C(1)
#define SKVOZ_NETWORK_SUCCESS UINT32_C(0)
#define SKVOZ_NETWORK_INSUFFICIENT_BUFFER UINT32_C(1)
#define SKVOZ_NETWORK_TIMEOUT UINT32_C(2)
#define SKVOZ_NETWORK_INVALID_ARGUMENT UINT32_C(3)
#define SKVOZ_NETWORK_CLOSED UINT32_C(4)
#define SKVOZ_NETWORK_INTERNAL UINT32_C(5)
#define SKVOZ_NETWORK_JSON_MAX ((size_t)32768)
#define SKVOZ_NETWORK_POLL_MAX_MS UINT32_C(1000)

/* Linux ABI1. Calls for one handle are serialized by its owner. All pointer
 * arguments must point to accessible, correctly aligned storage for the stated
 * size; each input range lies in one allocation and is immutable during the call.
 * Output storage is exclusively writable during the call. Input/output regions
 * must not overlap. No caller pointers are saved.
 * JSON is UTF-8 without a trailing NUL. No callback or packet JSON API exists. */
uint32_t skvoz_network_abi_version(void);

/* helper_fd is borrowed: -1 for client/TCP-only server; a connected exclusive
 * helper channel for an IP server. The library duplicates it with CLOEXEC.
 * Success writes a generation-checked handle; failure writes zero. */
uint32_t skvoz_network_create(const uint8_t *config, size_t len,
                             int32_t helper_fd, uint64_t *out_handle);

/* fd is borrowed and duplicated with CLOEXEC. Rejection closes only the copy.
 * Success returns the JSON request id. Responses arrive through next_event. */
uint32_t skvoz_network_request(uint64_t handle, const uint8_t *json, size_t len,
                              int32_t fd, uint32_t *out_request_id);

/* Includes responses and events. Buffer bytes have no trailing NUL.
 * timeout_ms: 0 polls, 1..1000 waits. NULL buffer is allowed only at capacity=0.
 * Status 1 reports required length without removing the message or its FD.
 * Success transfers out_fd (-1 if absent) exactly once; the caller closes it.
 * On other statuses out_len=0/out_fd=-1, except status 1 sets out_len.
 * out_len/out_fd must be non-NULL even for a zero-capacity query. */
uint32_t skvoz_network_next_event(uint64_t handle, uint8_t *out_buffer,
                                 size_t capacity, size_t *out_len,
                                 int32_t *out_fd, uint32_t timeout_ms);

/* Stop polling before destroy. Retires the handle and closes undelivered FDs.
 * Success confirms the same actor stopped and joined. Concurrent calls wait
 * behind the current <=1000 ms poll.
 * The handle is retired even if actor shutdown fails with INTERNAL.
 * A stale/destroyed handle returns SKVOZ_NETWORK_CLOSED. */
uint32_t skvoz_network_destroy(uint64_t handle);

#ifdef __cplusplus
}
#endif
#endif
