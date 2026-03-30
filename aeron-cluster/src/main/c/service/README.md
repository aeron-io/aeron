# Aeron Cluster -- Service Layer (C)

This directory implements the `ClusteredServiceContainer` and `ClusteredServiceAgent`,
the C equivalent of Java's `io.aeron.cluster.service` package.

## Lifecycle

1. **Init context** -- call `aeron_cluster_service_context_init()` and configure
   callbacks (`on_session_message`, `on_session_open`, `on_session_close`,
   `on_timer_event`, `on_take_snapshot`, `on_role_change`, etc.).
2. **Conclude** -- call `aeron_cluster_service_container_conclude()` to validate
   configuration, resolve the cluster directory, and open IPC channels.
3. **Create** -- call `aeron_cluster_service_container_create()` to allocate the
   container and its internal agent.
4. **Start** -- the agent sends a `ServiceAck` to the consensus module and waits
   for `JoinLog`.
5. **do_work loop** -- poll the `BoundedLogAdapter` (up to `commitPosition`),
   process `ServiceAdapter` IPC messages, and invoke user callbacks.
6. **Close** -- call `aeron_cluster_service_container_close()` to release
   publications, subscriptions, and allocated memory.

## Key APIs

| Function | Purpose |
|----------|---------|
| `aeron_cluster_service_context_init` | Allocate and zero-fill a context |
| `aeron_cluster_service_container_conclude` | Validate and finalise config |
| `aeron_cluster_service_container_create` | Create the container + agent |
| `aeron_clustered_service_agent_do_work` | Single duty-cycle tick |
| `aeron_cluster_client_session_offer` | Send a response to a client session |
| `aeron_cluster_service_container_close` | Tear down everything |

## Minimal Usage

```c
aeron_cluster_service_context_t *ctx = NULL;
aeron_cluster_service_context_init(&ctx);

ctx->aeron_dir = "/dev/shm/aeron-default";
ctx->cluster_dir = "/tmp/cluster/node-0";
ctx->service_id = 0;
ctx->on_session_message = my_on_session_message;
ctx->on_take_snapshot = my_on_take_snapshot;

aeron_cluster_service_container_t *container = NULL;
aeron_cluster_service_container_conclude(ctx);
aeron_cluster_service_container_create(&container, ctx);

while (!stop)
{
    aeron_clustered_service_agent_do_work(container->agent);
    aeron_idle_strategy_sleeping_idle(idle, work_count);
}

aeron_cluster_service_container_close(container);
```

## Source Files

| File | Contents |
|------|----------|
| `aeron_cluster_service_container.c/h` | Container lifecycle |
| `aeron_clustered_service_agent.c/h` | Agent duty-cycle and state machine |
| `aeron_cluster_service_context.c/h` | Configuration context |
| `aeron_cluster_client_session.c/h` | Per-client session + response pub |
| `aeron_cluster_bounded_log_adapter.c/h` | Replay log up to commit position |
| `aeron_cluster_service_adapter.c/h` | IPC from consensus module |
| `aeron_cluster_service_snapshot_taker.c/h` | Snapshot serialisation |
| `aeron_cluster_service_snapshot_loader.c/h` | Snapshot deserialisation |
| `aeron_cluster_consensus_module_proxy.c/h` | IPC to consensus module |
| `aeron_cluster_recovery_state.c/h` | Recovery state counter |
| `aeron_cluster_node_state_file.c/h` | Persistent node state |
