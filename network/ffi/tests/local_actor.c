#define _POSIX_C_SOURCE 200809L
#include "skvoz_network.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

/* This local consumer requires only the unavailable-broker fixture; no live
 * NATS, Docker, privileged helper or TUN fixture is used. */
static uint64_t owner;

static void cleanup(void) {
    if (owner) skvoz_network_destroy(owner);
}

static void fail(const char *phase) {
    fprintf(stderr, "FFI local actor failed: %s\n", phase);
    exit(1);
}

static double now(void) {
    struct timespec value;
    if (clock_gettime(CLOCK_MONOTONIC, &value)) fail("clock");
    return (double)value.tv_sec + (double)value.tv_nsec / 1e9;
}

int main(int argc, char **argv) {
    if (argc != 2) return 2;
    atexit(cleanup);
    uint8_t bytes[SKVOZ_NETWORK_JSON_MAX + 1];
    FILE *file = fopen(argv[1], "rb");
    if (!file) fail("fixture open");
    size_t length = fread(bytes, 1, sizeof(bytes), file);
    if (ferror(file) || fclose(file) || length > SKVOZ_NETWORK_JSON_MAX)
        fail("fixture read");
    if (skvoz_network_create(bytes, length, -1, &owner) || !owner)
        fail("create");
    const char *hello = "{\"v\":1,\"id\":1,\"op\":\"HELLO\",\"args\":{\"api\":1,\"network\":4},\"fd_count\":0}";
    uint32_t id = 0;
    if (skvoz_network_request(owner, (const uint8_t *)hello, strlen(hello), -1, &id)
            || id != 1)
        fail("HELLO admission");
    double deadline = now() + 2;
    int found = 0;
    while (now() < deadline && !found) {
        size_t required = 0, repeated = 0;
        int32_t fd = 9;
        uint32_t status = skvoz_network_next_event(owner, NULL, 0, &required, &fd, 100);
        if (status == SKVOZ_NETWORK_TIMEOUT) continue;
        if (status != SKVOZ_NETWORK_INSUFFICIENT_BUFFER || fd != -1 ||
                !required || required > SKVOZ_NETWORK_JSON_MAX)
            fail("first size query");
        if (skvoz_network_next_event(owner, NULL, 0, &repeated, &fd, 0) !=
                SKVOZ_NETWORK_INSUFFICIENT_BUFFER || repeated != required || fd != -1)
            fail("retained size query");
        if (skvoz_network_next_event(owner, bytes, required, &repeated, &fd, 0) ||
                repeated != required || fd != -1)
            fail("message delivery");
        bytes[repeated] = '\0';
        if (strstr((const char *)bytes, "\"id\":1,") &&
                strstr((const char *)bytes, "\"error\":null") &&
                strstr((const char *)bytes, "\"api\":1"))
            found = 1;
    }
    if (!found) fail("HELLO waited for broker readiness");
    uint64_t stale = owner;
    if (skvoz_network_destroy(owner)) fail("shutdown");
    owner = 0;
    if (skvoz_network_destroy(stale) != SKVOZ_NETWORK_CLOSED)
        fail("generation retired");
    if (skvoz_network_request(stale, (const uint8_t *)hello, strlen(hello), -1, &id) !=
            SKVOZ_NETWORK_CLOSED || id != 0)
        fail("stale request");
    puts("FFI LOCAL PASS: same actor HELLO, retained buffer queries, shutdown, stale generation");
    return 0;
}
