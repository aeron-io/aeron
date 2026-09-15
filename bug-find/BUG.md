# Min/tagged multicast flow control keeps a timed-out receiver and drops a live one

`aeron-driver/src/main/java/io/aeron/driver/AbstractMinMulticastFlowControl.java:171-188`, `onIdle`.

    for (int lastIndex = receivers.length - 1, i = lastIndex; i >= 0; i--)
    {
        final Receiver receiver = receivers[i];
        if ((receiver.timeOfLastStatusMessageNs + receiverTimeoutNs) - timeNs < 0 || receiver.eosFlagged)
        {
            if (i != lastIndex)
            {
                receivers[i] = receivers[lastIndex--];   /* lastIndex only moves on a swap */
            }
            removed++;
            ...

`lastIndex` is decremented only inside the `i != lastIndex` branch. When the element being removed
*is* the current last one, `lastIndex` stays where it is, so the next removal copies that
already-removed element back into the live region. `truncateReceivers` then keeps the first
`length - removed` entries, so the resurrected dead receiver survives and a live one is discarded.

## Reproduce

`MinFcRemovalProbe.java` drives the real `MinMulticastFlowControl`: three receivers are added, the
first and the last are left to time out, then `onIdle` runs.

    javac -d classes -cp aeron-all-1.54.0-SNAPSHOT.jar MinFcRemovalProbe.java
    java --add-opens=java.base/jdk.internal.misc=ALL-UNNAMED \
        -cp classes:aeron-all-1.54.0-SNAPSHOT.jar io.aeron.driver.MinFcRemovalProbe

## Observed

    receiverTimeoutNs = 100000000
    before onIdle: receivers = 1 2 3
    after  onIdle: receivers = 3
    expected: 2 (the only receiver that is still alive)

Receiver 3 had timed out; receiver 2 had not. With the fix the survivor is 2.

## Consequences

* The retained stale receiver keeps its `lastPositionPlusWindow`, which continues to clamp
  `minLimitPosition`, so the sender's limit is governed by a receiver that has gone away.
* The live receiver is forgotten, so its next status message is treated as a brand new receiver and
  re-added, churning `receiverCount` and `hasRequiredReceivers`.
* With `fc=min,g:/N` that churn moves the publication in and out of the connected state.

Needs three or more receivers with the stale ones at the ends of the array -- an ordinary setup.

## Note

The C port is correct: `aeron_min_flow_control_strategy_on_idle` uses
`aeron_array_fast_unordered_remove(..., i, last_index)` and decrements `last_index`
unconditionally. This defect is Java-only.

## Status

Fixed on this branch: move `lastIndex--` out of the conditional. Probe included.
