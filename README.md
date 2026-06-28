Aeron
=====

[![GitHub](https://img.shields.io/github/license/aeron-io/Aeron.svg)](https://github.com/aeron-io/aeron/blob/master/LICENSE)
[![Javadocs](https://www.javadoc.io/badge/io.aeron/aeron-all.svg)](https://www.javadoc.io/doc/io.aeron/aeron-all)

[![Actions Status](https://github.com/aeron-io/aeron/workflows/Continuous%20Integration/badge.svg)](https://github.com/aeron-io/aeron/actions)
[![CodeQL Status](https://github.com/aeron-io/aeron/workflows/CodeQL/badge.svg)](https://github.com/aeron-io/aeron/actions)

Efficient reliable UDP unicast, UDP multicast, and IPC message transport. Java, C, and C++ clients are available in this
repository, and a [.NET client](https://github.com/AdaptiveConsulting/Aeron.NET) is available. All
clients can exchange messages across machines, or on the same machine via IPC, very efficiently. Message streams can be
recorded by the [Archive](https://github.com/aeron-io/aeron/tree/master/aeron-archive) module to persistent storage
for later, or real-time, replay. Aeron [Cluster](https://github.com/aeron-io/aeron/tree/master/aeron-cluster)
provides support for fault-tolerant services as replicated state machines based on the
[Raft](https://raft.github.io/) consensus algorithm.

Performance is the key focus. A design goal for Aeron is to be the highest throughput with the lowest and most
predictable latency of any messaging system. Aeron integrates with
[Simple Binary Encoding (SBE)](https://github.com/aeron-io/simple-binary-encoding) for the best possible message
encoding and decoding performance. Many of the data structures used in the creation of Aeron have been factored out to
the [Agrona](https://github.com/aeron-io/agrona) project.

For details of usage, protocol specification, FAQ, etc., please check out the
[Wiki](https://github.com/aeron-io/aeron/wiki).

For the latest version information and changes, see the [Change Log](https://github.com/aeron-io/aeron/blob/master/CHANGELOG.adoc)
with Java **downloads** at [Maven Central](http://search.maven.org/#search%7Cga%7C1%7Caeron).

Aeron is owned and operated by Adaptive Financial Consulting. Originally created by Martin Thompson and Todd Montgomery, the Aeron team joined Adaptive in 2022.

For Business users, to get started with Aeron Premium, please visit [Aeron.io](https://aeron.io)

We provide a range of services, including:
* Training for development and operations with Aeron and Aeron Cluster.
* Consulting, for example, if you’re not sure how to design your system or need help tuning your system.
* We also offer a number of proprietary enhancements on top of Aeron and Aeron Cluster, such as kernel bypass with DPDK for increased performance, and blazing fast encryption with ATS.
* If you’re building a new trading system, we have experienced Aeron developers who can help.

Please get in touch at [sales@aeron.io](mailto:sales@aeron.io?subject=Aeron) if you would like to learn more about any of these.

### How do I use Aeron?

1. [Java Programming Guide](https://github.com/aeron-io/aeron/wiki/Java-Programming-Guide)
1. [C++11 Programming Guide](https://github.com/aeron-io/aeron/wiki/Cpp-Programming-Guide)
1. [Best Practices Guide](https://github.com/aeron-io/aeron/wiki/Best-Practices-Guide)
1. [Monitoring and Debugging](https://github.com/aeron-io/aeron/wiki/Monitoring-and-Debugging)
1. [Configuration Options](https://github.com/aeron-io/aeron/wiki/Configuration-Options)
1. [Channel Specific Configuration](https://github.com/aeron-io/aeron/wiki/Channel-Configuration)
1. [Aeron Archive (Durable/Persistent Stream Storage)](https://github.com/aeron-io/aeron/wiki/Aeron-Archive)
1. [Aeron Cluster (Fault Tolerant Services)](https://github.com/aeron-io/aeron/tree/master/aeron-cluster)
1. [Aeron Docs](https://aeron.io/docs/)

### How does Aeron work?

1. [Transport Protocol Specification](https://github.com/aeron-io/aeron/wiki/Transport-Protocol-Specification)
1. [Design Overview](https://github.com/aeron-io/aeron/wiki/Design-Overview)
1. [Design Principles](https://github.com/aeron-io/aeron/wiki/Design-Principles)
1. [Flow Control Semantics](https://github.com/aeron-io/aeron/wiki/Flow-and-Congestion-Control)
1. [Media Driver Operation](https://github.com/aeron-io/aeron/wiki/Media-Driver-Operation)

### How do I hack on Aeron?

1. [Hacking on Aeron](https://github.com/aeron-io/aeron/wiki/Hacking-on-Aeron)
1. [Performance Testing](https://github.com/aeron-io/aeron/wiki/Performance-Testing)
1. [Building Aeron](https://github.com/aeron-io/aeron/wiki/Building-Aeron)

License (See LICENSE file for full license)
-------------------------------------------
Copyright 2014-2025 Real Logic Limited.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.  
