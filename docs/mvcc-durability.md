# MVCC Durability Notes

When `async_fsync` is enabled the pager now exposes a durable watermark LSN recorded in `wal.dwm`. Crash recovery replays WAL frames only up to this LSN, while `sombra mvcc-status` prints both the latest committed and durable LSNs plus their lag to help SREs monitor backlog.
