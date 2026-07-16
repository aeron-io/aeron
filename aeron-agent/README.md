Aeron Agent
===

[![Javadocs](http://www.javadoc.io/badge/io.aeron/aeron-all.svg)](http://www.javadoc.io/doc/io.aeron/aeron-all)

A Java agent which when attached to a JVM will weave byte code to intercept and log events that
implement [EventCode](https://github.com/aeron-io/aeron/blob/master/aeron-agent/src/main/java/io/aeron/agent/EventCode.java),
those include [DriverEventCode](https://github.com/aeron-io/aeron/blob/master/aeron-agent/src/main/java/io/aeron/agent/DriverEventCode.java), [ArchiveEventCode](https://github.com/aeron-io/aeron/blob/master/aeron-agent/src/main/java/io/aeron/agent/ArchiveEventCode.java),
and [ClusterEventCode](https://github.com/aeron-io/aeron/blob/master/aeron-agent/src/main/java/io/aeron/agent/ClusterEventCode.java)

Events are recorded to an in-memory
[RingBuffer](https://github.com/aeron-io/agrona/blob/master/agrona/src/main/java/org/agrona/concurrent/ringbuffer/RingBuffer.java)
which is consumed and appended asynchronously to a log as defined by the system property `aeron.event.log.reader.classname`
for the reader [Agent](https://github.com/aeron-io/agrona/blob/master/agrona/src/main/java/org/agrona/concurrent/Agent.java)
which defaults to [EventLogReaderAgent](https://github.com/aeron-io/aeron/blob/master/aeron-agent/src/main/java/io/aeron/agent/EventLogReaderAgent.java).