Aeron Archive
===

[![Javadocs](http://www.javadoc.io/badge/io.aeron/aeron-all.svg)](http://www.javadoc.io/doc/io.aeron/aeron-all)

The aeron-archive is a module which enables Aeron data stream recording and replay from durable storage. 

Samples can be found [here](https://github.com/aeron-io/aeron/blob/master/aeron-samples/scripts/archive/README.md) and
systems tests [here](https://github.com/aeron-io/aeron/tree/master/aeron-system-tests/src/test/java/io/aeron/archive).

Features:

- **Record:** service can record a particular subscription, described by `<channel, streamId>`. Each resulting image
 for the subscription will be recorded under a new `recordingId`. Local network publications are recorded using the spy
 feature for efficiency. If no subscribers are active then the recording can advance the stream by setting the
 `aeron.spies.simulate.connection` system property to true.

- **Extend:** service can extend an existing recording by appending.

- **Replay:** service can replay a recorded `recordingId` from a particular `position`, and for a particular `length`
 which can be `Aeron.NULL_VALUE` for an open-ended replay. An open-ended replay will stop when it reaches the stop
 position of a recording.

- **Query:** the catalog for existing recordings, and the recorded position of an active recording.

- **Truncate:** allows a stopped recording to have its length truncated, and if truncated to the start position then it
 is effectively deleted.

- **Replay Merge:** allows a late joining subscriber of a recorded stream to replay a recording and then merge with the
 live stream for cut over if the consumer is fast enough to keep up.

- **Replicate:** recordings can be replicated from a source to destination archive with the option to follow on with
 a live stream when the source is multicast. When using replication it is necessary to configure the replication channel
 for the destination archive with `aeron.archive.replication.channel`.

- **Recording Storage Maintenance:** Manage the storage of large recordings by performing purge, detach, and delete
 operations, plus the ability to attach and migrate segments at the beginning of recordings. 

Usage
=====

Protocol
=====
Messages specification use SBE [aeron-archive-codecs.xml](https://github.com/aeron-io/aeron/blob/master/aeron-archive/src/main/resources/archive/aeron-archive-codecs.xml).
The Archive communicates via the following interfaces:

 - **Recording Events stream:** other parties can subscribe to events for the start,
 stop, and progress of recordings. These are the recording events messages specified in the codec.
 
 - **Control Request stream:** this allows clients to initiate replay or queries interactions with the archive.
 Requests have a correlationId sent on the initiating request. The `correlationId` is expected to be managed by
 the clients and is offered as a means for clients to track multiple concurrent requests. A request will typically
 involve the archive sending data back on the reply channel specified by the client on the `ConnectRequest`.

A control session can be established with the Archive after a `ConnectRequest`. Operations happen within
the context of such a ControlSession which is allocated a `controlSessionId`.

Recording Progress Events
----
Aeron clients wishing to observe the Archive recordings lifecycle can do so by subscribing to the recording events
channel. The messages are described in the codec. To fully capture the state of the Archive a client could subscribe
to these events as well as query for the full list of descriptors.

Recording Signal Events
----
On a control session signals can be tracked for when a recording starts and stop plus other operations like extend,
replicate, and live merge.

Recording Durability
----
An archive can be instructed to record streams, i.e. `<channel, streamId>` pairs. These streams are recorded with the
file sync level the archive has been launched with. Progress is reported on the recording events stream.

- `aeron.archive.file.sync.level=0`: for normal writes to the OS page cache for background writing to disk.
- `aeron.archive.file.sync.level=1`: for forcing the dirty data pages to disk. 
- `aeron.archive.file.sync.level=2`: for forcing the dirty data pages and file metadata to disk.

When setting file sync level greater than zero it is also important to sync the archive catalog with the
 `aeron.archive.catalog.file.sync.level` to the same value.

Recordings will be assigned a `recordingId` and a full description of the stream is captured in the Archive Catalog.
The Catalog chronicles the contents of an archive as `RecordingDescriptor`s which can be queried.

The progress of active recordings can be tracked using `AeronStat` to view the `rec-pos` counter for each stream.

Persisted Format
=====
The Archive is backed by 3 file types, all of which are expected to reside in the `archiveDir`.

 -  **Catalog (one per archive):** The catalog contains records of recording descriptors. The descriptors can
 be queried as described above. See the codec schema for full descriptor details.
 
 - **Recording Segment Files (many per recorded stream):** This is where the recorded data is kept.
 Recording segments follow the naming convention of: `<recordingId>-<segment base position>.rec`
 The Archive copies data as is from the recorded Image. As such the files follow the same convention
 as Aeron data streams. Data starts at `startPosition`, which translates into the offset
 `startPosition % termBufferLength` in the first segment file. From there one can read fragments
 as described by the `DataHeaderFlyweight` up to the `stopPosition`. Segment length is a multiple of `termBufferLength`.
 
  - **Mark File:** This file contains the archive distinct error log and heartbeat timestamp to ensure two or more
 archives do not run in the same directory.

Migration
=====
The Archive may need to be migrated between major versions. This migration will be evident if attempting
to run `ArchiveTool` with the `describe` command on the archive directory. A previous version will
only be readable by a previous version of `ArchiveTool`. To migrate the archive, please follow
the steps below.

- Shutdown the Archive and ensure all recordings have a stop position.
- Take a backup of the Archive directory.
- Run `ArchiveTool` command `migrate`. Information on versions, etc. will be displayed. Errors
will also be displayed.
- Run `ArchiveTool` command `verify` to check for validity.

APIs
=====
The synchronous C and C++ wrapper APIs are complete.  The async APIs have yet to be implemented.
