#define _POSIX_C_SOURCE 200809L
#include "skvoz_network.h"
#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

static uint64_t server_handle, client_handle;
static char message[SKVOZ_NETWORK_JSON_MAX + 1];

static void cleanup(void) {
    if (client_handle) skvoz_network_destroy(client_handle);
    if (server_handle) skvoz_network_destroy(server_handle);
}

static void fail(const char *phase) {
    fprintf(stderr, "FFI conformance failed: %s\n", phase);
    exit(1);
}

static double now(void) {
    struct timespec value;
    if (clock_gettime(CLOCK_MONOTONIC, &value)) fail("clock");
    return (double)value.tv_sec + (double)value.tv_nsec / 1e9;
}

static uint64_t create(const char *path) {
    unsigned char bytes[SKVOZ_NETWORK_JSON_MAX + 1];
    FILE *file = fopen(path, "rb");
    if (!file) fail("config open");
    size_t length = fread(bytes, 1, sizeof(bytes), file);
    if (ferror(file) || fclose(file) || length > SKVOZ_NETWORK_JSON_MAX)
        fail("config read");
    uint64_t handle = 0;
    if (skvoz_network_create(bytes, length, -1, &handle) || !handle)
        fail("create");
    return handle;
}

static void request(uint64_t handle, const char *op, const char *json,
                    uint32_t expected_id) {
    uint32_t id = 0;
    uint32_t result = skvoz_network_request(handle, (const uint8_t *)json,
                                            strlen(json), -1, &id);
    if (result || id != expected_id) {
        fprintf(stderr, "FFI request admission: role=%s op=%s expected_id=%u returned_id=%u result=%u\n",
                handle == server_handle ? "server" : "client", op,
                expected_id, id, result);
        fail("request admission");
    }
}

/* Every delivery first probes twice with no buffer. Even FD responses must
 * remain queued and must not transfer a descriptor during these queries. */
static int next(uint64_t handle, int *fd) {
    size_t length = 0, repeated = 0;
    int32_t output_fd = 7;
    uint32_t result = skvoz_network_next_event(handle, NULL, 0, &length,
                                               &output_fd, 100);
    if (result == SKVOZ_NETWORK_TIMEOUT) return 0;
    if (result != SKVOZ_NETWORK_INSUFFICIENT_BUFFER || output_fd != -1 ||
            !length || length > SKVOZ_NETWORK_JSON_MAX)
        fail("size query");
    if (skvoz_network_next_event(handle, NULL, 0, &repeated, &output_fd, 0) !=
            SKVOZ_NETWORK_INSUFFICIENT_BUFFER || repeated != length || output_fd != -1)
        fail("message retained");
    if (skvoz_network_next_event(handle, (uint8_t *)message, length, &repeated,
                                 &output_fd, 0) || repeated != length)
        fail("message delivery");
    message[length] = '\0';
    *fd = output_fd;
    return 1;
}

static int response(uint64_t handle, uint32_t id) {
    char needle[32];
    snprintf(needle, sizeof(needle), "\"id\":%u,", id);
    double deadline = now() + 10;
    while (now() < deadline) {
        int fd;
        if (!next(handle, &fd)) continue;
        if (strstr(message, needle)) {
            if (!strstr(message, "\"error\":null")) fail("response error");
            return fd;
        }
        if (fd != -1) { close(fd); fail("unexpected event descriptor"); }
    }
    fail("response deadline");
    return -1;
}

static void hello_ready(uint64_t handle) {
    request(handle, "HELLO", "{\"v\":1,\"id\":1,\"op\":\"HELLO\",\"args\":{\"api\":1,\"network\":4},\"fd_count\":0}", 1);
    int hello = 0, ready = 0;
    double deadline = now() + 30;
    while (now() < deadline && (!hello || !ready)) {
        int fd;
        if (!next(handle, &fd)) continue;
        if (fd != -1) { close(fd); fail("startup descriptor"); }
        if (strstr(message, "\"id\":1,") && strstr(message, "\"error\":null"))
            hello = 1;
        if (strstr(message, "\"event\":\"RUNTIME_STATE\"") &&
                strstr(message, "\"state\":\"ready\"")) ready = 1;
        if (strstr(message, "\"state\":\"closed\"")) fail("startup terminal");
    }
    if (!hello || !ready) fail("startup readiness deadline");
}

static int open_tcp(const char *address, uint32_t id) {
    char json[512];
    int count = snprintf(json, sizeof(json), "{\"v\":1,\"id\":%u,\"op\":\"OPEN_TCP\",\"args\":{\"host\":\"%s\",\"port\":4444},\"fd_count\":0}", id, address);
    if (count < 0 || (size_t)count >= sizeof(json)) fail("OPEN length");
    request(client_handle, "OPEN_TCP", json, id);
    int fd = response(client_handle, id);
    if (fd < 0 || !(fcntl(fd, F_GETFD) & FD_CLOEXEC)) fail("owned CLOEXEC socket");
    int flags = fcntl(fd, F_GETFL);
    if (flags < 0 || fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0)
        fail("caller socket nonblocking");
    return fd;
}

static void write_all(int fd, const unsigned char *bytes, size_t length) {
    size_t offset = 0;
    double deadline = now() + 10;
    while (offset < length && now() < deadline) {
        ssize_t count = write(fd, bytes + offset, length - offset);
        if (count > 0) offset += (size_t)count;
        else if (count < 0 && (errno == EAGAIN || errno == EINTR)) {
            struct pollfd item = {fd, POLLOUT, 0};
            if (poll(&item, 1, 100) < 0 && errno != EINTR) fail("write poll");
        } else fail("socket write");
    }
    if (offset != length) fail("write deadline");
}

int main(int argc, char **argv) {
    if (argc != 4) return 2;
    atexit(cleanup);
    server_handle = create(argv[1]);
    client_handle = create(argv[2]);
    hello_ready(server_handle);
    hello_ready(client_handle);
    request(client_handle, "START_PROXY", "{\"v\":1,\"id\":2,\"op\":\"START_PROXY\",\"args\":{\"http_bind\":null,\"socks_bind\":null},\"fd_count\":0}", 2);
    if (response(client_handle, 2) != -1) fail("START descriptor");
    int fd = open_tcp(argv[3], 3);
    unsigned char bytes[32771], received[4096];
    for (size_t i = 0; i < sizeof(bytes); i++) bytes[i] = (unsigned char)(i * 31 + i / 255);
    write_all(fd, bytes, sizeof(bytes));
    if (shutdown(fd, SHUT_WR)) fail("half close");
    size_t offset = 0;
    double deadline = now() + 10;
    int eof = 0;
    while (now() < deadline && !eof) {
        ssize_t count = read(fd, received, sizeof(received));
        if (count > 0) {
            if (offset + (size_t)count > sizeof(bytes) ||
                    memcmp(bytes + offset, received, (size_t)count)) fail("byte integrity");
            offset += (size_t)count;
        } else if (!count) eof = 1;
        else if (errno == EAGAIN || errno == EINTR) {
            struct pollfd item = {fd, POLLIN, 0};
            if (poll(&item, 1, 100) < 0 && errno != EINTR) fail("read poll");
        } else fail("socket read");
    }
    if (!eof || offset != sizeof(bytes)) fail("echo EOF/length");
    close(fd);

    /* A fresh stream must survive the preceding clean half-close. Its owned FD
     * is retained across insufficient-buffer queries, then the owner is destroyed
     * while this delivered FD remains caller-owned. It must reach terminal EOF. */
    fd = open_tcp(argv[3], 4);
    uint64_t stale = client_handle;
    if (skvoz_network_destroy(client_handle)) fail("destroy");
    client_handle = 0;
    if (skvoz_network_destroy(stale) != SKVOZ_NETWORK_CLOSED) fail("stale destroy");
    deadline = now() + 3;
    eof = 0;
    while (now() < deadline && !eof) {
        ssize_t count = read(fd, received, sizeof(received));
        if (!count) eof = 1;
        else if (count < 0 && (errno == EAGAIN || errno == EINTR)) {
            struct pollfd item = {fd, POLLIN, 0};
            poll(&item, 1, 100);
        } else fail("destroy socket terminal");
    }
    close(fd);
    if (!eof) fail("destroy cleanup deadline");
    puts("FFI REAL PASS: verified TLS/NATS, retained queries, single owned FD, binary echo, half-close, destroy, stale handle");
    return 0;
}
