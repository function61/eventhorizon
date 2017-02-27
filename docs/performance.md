Current performance
-------------------

BoltDB-backed WAL:

- 24 000 lines/sec (100 line batches) (240 tx/sec)
- 2 400 lines/sec (10 line batches) (240 tx/sec)


Plans for performance
---------------------

BoltDB is not write-optimized. Use something else for better write optimization,
or even implement a custom WAL with specific optimizations for append-only data.
