# Aeron Cluster C Client — Usage Guide

Examples of the public C cluster client API. The test file
`aeron-cluster/src/test/c/client/aeron_archive_test.cpp` contains working examples
of all APIs.

---

## Connecting to a cluster

### Synchronous connect

```c
aeron_cluster_context_t *ctx;
aeron_cluster_client_t  *cluster = NULL;

aeron_cluster_context_init(&ctx);
aeron_cluster_context_set_ingress_channel(ctx, "aeron:udp?endpoint=localhost:9010");
aeron_cluster_context_set_egress_channel(ctx, "aeron:udp?endpoint=localhost:0");

aeron_cluster_connect(&cluster, ctx);
aeron_cluster_context_close(ctx);
```

The context is copied on connect and can be closed immediately after.

### Asynchronous connect

```c
aeron_cluster_context_t       *ctx;
aeron_cluster_async_connect_t *async;
aeron_cluster_client_t        *cluster = NULL;

aeron_cluster_context_init(&ctx);
aeron_cluster_async_connect(&async, ctx);
aeron_cluster_context_close(ctx);

while (NULL == cluster)
{
    idle();
    aeron_cluster_async_connect_poll(&cluster, async);
}
```

### Multi-member ingress

```c
/* "memberId=host:port" pairs, comma-separated */
aeron_cluster_context_set_ingress_endpoints(ctx,
    "0=host0:9010,1=host1:9010,2=host2:9010");
```

---

## Sending messages

```c
/* offer returns the new stream position on success, or a negative value */
int64_t pos = aeron_cluster_offer(
    cluster,
    cluster_session_id,   /* from SessionEvent on connect */
    buffer,
    length);
```

---

## Polling egress

Define a handler for messages received from the cluster:

```c
void on_session_message(
    void *clientd,
    int64_t cluster_session_id,
    int64_t timestamp,
    const uint8_t *buffer,
    size_t length)
{
    /* handle application message */
}

void on_session_event(
    void *clientd,
    int64_t correlation_id,
    int64_t cluster_session_id,
    int32_t leader_member_id,
    aeron_cluster_client_event_code_t code,
    const char *detail,
    size_t detail_length)
{
    if (AERON_CLUSTER_CLIENT_EVENT_CODE_OK == code)
    {
        /* connected; cluster_session_id is now valid */
    }
}
```

Set the handlers on the context before connecting, then poll in your duty cycle:

```c
aeron_cluster_context_set_on_session_message(ctx, on_session_message, clientd);
aeron_cluster_context_set_on_session_event(ctx, on_session_event, clientd);

/* in duty cycle: */
aeron_cluster_poll_egress(cluster);
```

---

## Closing

```c
aeron_cluster_close(cluster);
```

---

## Authentication

```c
void encoded_credentials(
    aeron_archive_encoded_credentials_t *credentials_out,
    void *clientd)
{
    credentials_out->data   = "user:password";
    credentials_out->length = strlen("user:password");
}

aeron_cluster_context_set_credentials_supplier(
    ctx, encoded_credentials, NULL, NULL, clientd);
```

---

## Default stream IDs

| Channel | Default stream |
|---------|---------------|
| Ingress (client→cluster) | 101 |
| Egress (cluster→client) | 102 |
