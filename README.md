# BSSDB

B-Tree on Seakless Storage Database.

## Goals

- Support running many small-to-mid sized isolated databases in a single process
- Scale well for naturally sharded data
- Make replicating writes (even across devices) really fast when only one writing thread exists globally
- Support a packed encoding for sending changes and snapshots over the network
- Be very performant on SSDs
- Consistent reads
- Atomic write batches

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