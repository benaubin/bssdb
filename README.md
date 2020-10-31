# BSSDB

An embedded database library for KV databases, designed for modern hardware, distributed systems and data with natural sharding.

This is an extremely early stage, without any tests or attention paid to stability. Please don't use it just yet (but I'd love your help building it).

## Goals

- Support running many small-to-mid sized isolated databases in a single process
- Scale well for naturally sharded data
- Make replicating writes (even across devices) really fast when only one writing thread exists globally (by only requiring a search to perform a write on the leader, not replicas)
- Support an efficient encoding for sending changes and snapshots over the network
- Be very performant on modern SSDs
- Point-in-time consistent reads
- Atomic writes within a database

## Non-goals

- Concurrent writer transactions
- Optimizing performance on hard drives (bssd makes liberal use of random reads and writes)
- Optimizing performance for large databases that can't be sharded

## Licensing

MIT license

Note: We use `rio` for Linux IO (to take advantage of io_uring), which is GPL'd.
Your use of this library may be subject to the GPL, where the GPL applies.

### Alternative backronyms:

- Ben's super substandard database.
- Basic singe-threaded sendable-write database
- Boring slow sad database
