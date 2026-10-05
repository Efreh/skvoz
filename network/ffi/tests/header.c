#include "skvoz_network.h"

_Static_assert(sizeof(uint64_t) == 8, "handle width");
_Static_assert(sizeof(uint32_t) == 4, "status and request id width");
_Static_assert(sizeof(int32_t) == 4, "descriptor width");

int main(void) {
    uint64_t handle = 9;
    uint32_t request_id = 9;
    size_t length = 9;
    int32_t fd = 9;
    if (skvoz_network_abi_version() != SKVOZ_NETWORK_ABI_VERSION)
        return 1;
    if (skvoz_network_create(NULL, 0, -1, &handle) !=
            SKVOZ_NETWORK_INVALID_ARGUMENT || handle != 0)
        return 2;
    if (skvoz_network_request(0, (const uint8_t *)"{}", 2, -1,
                              &request_id) != SKVOZ_NETWORK_CLOSED || request_id != 0)
        return 3;
    if (skvoz_network_next_event(0, NULL, 0, &length, &fd, 0) !=
            SKVOZ_NETWORK_CLOSED || length != 0 || fd != -1)
        return 4;
    if (skvoz_network_destroy(0) != SKVOZ_NETWORK_CLOSED)
        return 5;
    return 0;
}
