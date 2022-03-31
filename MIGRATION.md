Backend.AI Migration Guide
==========================

## General

* The migration should be done while the managers and agents are shut down.
* This guide only describes additional steps to follow other than the code/package upgrades.

## 21.09 to 22.03

* `alembic upgrade head` is required to migrate the PostgreSQL database schema.
  - The `keypairs.concurrency_used` column is dropped and it will use Redis to keep track of it.
  - The `kernels.last_stat` column is still there but it will get updated only when the kernels terminate.
    There is a backup option to restore prior behavior of periodic sync: `debug.periodic-sync-stats` in
    `manager.toml`, though.

* The Redis container used with the manager should be reconfigured to use a persistent database.
  The Docker official image uses `/data` as the directory to store RDB/AOF files.
  In HA setup, it is recommended to enable AOF by `appendonly yes` in the Redis configuration to make it
  recoverable after hardware failures.

  Consult [the official doc](https://redis.io/docs/manual/persistence/) for more details.

* Configure an explicit cron job to execute `backend.ai mgr clear-history -r {retention}` which trims old
  sessions execution logs from the PostgreSQL and Redis databases to avoid indefinite grow of disk and
  memory usage of the manager.

  The retention argument may be given as human-readable duration expressions, such as `30m`, `6h`, `3d`,
  `2w`, `3mo`, and `1yr`.  If there is no unit suffix, the value is interpreted as seconds.
  It is recommended to schedule this command once a day.

## 21.03 to 21.09

* `alembic upgrade head` is required to migrate the PostgreSQL database schema.
