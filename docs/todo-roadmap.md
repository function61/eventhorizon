Long term roadmap
-----------------

- Test writing a huge amount of data, like 100 GB
- Test creating a huge amount of open streams
- Test horizontal scalability by measuring throughput while ramping up node count to ten-twenty?
- [Create power off simulation torture test suite](https://superuser.com/questions/1187364/simulating-file-corruption-on-linux-programmatically-for-db-durability-testing)
- Special WAL optimized for our append-only use case
- HA mode with Raft + BoltDB


Short-term TODO
---------------

- At-rest encryption
- Have subdir structure for storages as not to have too many files in one dir
- Have each line have a type prefix (meta line, regular line, binary line etc.)
- Previous block sha256
- Stats about stream (# of lines, # of bytes etc.)
- "Training wheels"? i.e. separate append-only log for backup until we trust
  the mechanics of this as working?
- Remove panic()s
- Rename chunk -> block

