# DBShark

DBShark is an embedded key-value B+tree-based database inspired by [boltdb](https://github.com/boltdb/bolt).

> [!WARNING]
> This project is a work-in-progress.

Some of key features are:
- Embedded
- Key-value store
- Range query
- Serializable isolation level
- Crash-safe
- Transactions are atomic
- Allow rollback
- Concurrent read transaction
- Write transactions are exclusive

Limitation:
- Currently, it's only working for linux
- No delete operation yet
- Only allow single instance
