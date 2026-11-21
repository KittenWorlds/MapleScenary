/*
 * MapleScenary C API
 *
 * Link against libmaple_scenary.so / libmaple_scenary.dylib /
 * maple_scenary.dll or the staticlib, and include this header.
 *
 * Every function returns 0 (MAPLE_OK) on success or a negative error code.
 * Call maple_last_error_message() for a human-readable description.
 *
 * Messages are borrowed views into the shared-memory region and stay valid
 * until maple_message_free() is called.
 */
#ifndef MAPLE_SCENARY_H
#define MAPLE_SCENARY_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define MAPLE_ABI_VERSION 1u
#define MAPLE_WAIT_INFINITE UINT64_MAX

enum maple_error {
    MAPLE_OK = 0,
    MAPLE_ERR_IO = -2,
    MAPLE_ERR_ADDR_IN_USE = -3,
    MAPLE_ERR_PERMISSION_DENIED = -4,
    MAPLE_ERR_UNSUPPORTED = -5,
    MAPLE_ERR_PROTOCOL = -6,
    MAPLE_ERR_DISCONNECTED = -7,
    MAPLE_ERR_TIMEOUT = -8,
    MAPLE_ERR_WOULD_BLOCK = -9,
    MAPLE_ERR_MESSAGE_TOO_LARGE = -10,
    MAPLE_ERR_BUFFER_TOO_SMALL = -11,
    MAPLE_ERR_CLOSED = -12,
    MAPLE_ERR_INTERNAL = -13,
    MAPLE_ERR_INVALID_CONFIG = -14
};

typedef struct {
    uint64_t region_len;      /* shared region bytes, default 64 MiB */
    uint32_t ring_capacity;   /* descriptors per ring, power of two */
    uint32_t min_unit;        /* allocation granularity, power of two */
    uint64_t max_message_len; /* largest accepted payload */
    uint8_t force_stream;     /* non-zero disables shared memory */
    uint8_t _padding[7];
} maple_config;

typedef struct maple_channel maple_channel;
typedef struct maple_listener maple_listener;
typedef struct maple_message maple_message;

/* Fill `out` with the default configuration. */
void maple_config_default(maple_config *out);

/* ABI revision understood by this build. */
uint32_t maple_abi_version(void);

/* Create two connected channels inside the current process. */
int32_t maple_pair(const maple_config *config, maple_channel **out_a, maple_channel **out_b);

/* Bind a listener. Unix: path or "@abstract"; Windows: pipe name. */
int32_t maple_listen(const char *name, const maple_config *config, maple_listener **out);

/* Accept one connection. `timeout_ms` may be MAPLE_WAIT_INFINITE. */
int32_t maple_accept(const maple_listener *listener, uint64_t timeout_ms, maple_channel **out);

/* Connect to a listener. */
int32_t maple_connect(const char *name, const maple_config *config, maple_channel **out);

/* Send one payload. */
int32_t maple_send(const maple_channel *channel, uint32_t kind, const uint8_t *data, size_t len);

/* Receive one message into `*out`. */
int32_t maple_recv(const maple_channel *channel, uint64_t timeout_ms, maple_message **out);

uint32_t maple_message_kind(const maple_message *message);
size_t maple_message_len(const maple_message *message);
const uint8_t *maple_message_data(const maple_message *message);

void maple_message_free(maple_message *message);
void maple_channel_free(maple_channel *channel);
void maple_listener_free(maple_listener *listener);

/* Last failure for the calling thread; valid until the next library call. */
const char *maple_last_error_message(void);

#ifdef __cplusplus
}
#endif

#endif /* MAPLE_SCENARY_H */
