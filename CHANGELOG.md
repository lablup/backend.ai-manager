Changes
=======

<!--
    You should *NOT* be adding new change log entries to this file, this
    file is managed by towncrier. You *may* edit previous change logs to
    fix problems like typo corrections or such.

    To add a new change log entry, please refer
    https://pip.pypa.io/en/latest/development/contributing/#news-entries

    We named the news folder "changes".

    WARNING: Don't drop the last line!
-->

.. towncrier release notes start

21.03.6 (2021-06-07)
--------------------

### Features
* Add an internal warning logs for excessive number of concurrent DB transactions ([#435](https://github.com/lablup/backend.ai-manager/issues/435))
* Add a common session environment variable `BACKENDAI_SESSION_NAME` for improved prompts and user acquaintance of which container they use ([#443](https://github.com/lablup/backend.ai-manager/issues/443))


21.03.5 (2021-05-17)
--------------------

### Features
* Add `PRE_AUTH_MIDDLEWARE` hook for cookie-based SSO plugins ([#420](https://github.com/lablup/backend.ai-manager/issues/420))

### Fixes
* Optimize read-only GraphQL queries to use read-only transaction isolation level, which greatly reduces the database loads when using GUI ([#431](https://github.com/lablup/backend.ai-manager/issues/431))


21.03.4 (2021-05-14)
--------------------

### Features
* Add an API endpoint to share/unshare a group virtual folder directly to specific users. This is to allow specified users (usually teachers with user account) can upload data/materials to a virtual folder while it is shared as read-only for other group users. ([#419](https://github.com/lablup/backend.ai-manager/issues/419))

### Fixes
* Change the `KeyPair.num_queries` GQL field to use Redis instead of the `keypairs.num_queries` DB column to avoid excessive DB writes ([#421](https://github.com/lablup/backend.ai-manager/issues/421)) ([#425](https://github.com/lablup/backend.ai-manager/issues/425))
* Improve stability and synchronization of container-databse states
  - Now all DB transactions use the "SERIALIZABLE" isolation level with explicit retries.
  - Now DB transactions that includes only SELECT queries are marked as "read-only" so that
    the PostgreSQL engine could optimize concurrent access with the new isolation level.
    All future codes should use `beegin_readonly()` method from our own subclassed SQLAlchemy
    engine instance replacing all existing `db` context variables.
  - Remove excessive database updates due to keypair API query counts and kernel API query counts.
    The keypair API query count is re-written to use Redis with one month retention. (#421)
    Now just calling an API does not trigger updates in the PostgreSQL database.
  - Fix unnecessary database updates for agent heartbeats.
  - Split many update-only DB transactions into smaller units, such as resource recalculation.
  - Use PostgreSQL advisory locks to make the scheduling decision process as a critical section.
  - Fix some of variable binding issues with nested functions inside loops.
  - Apply event message coalescing to prevent event bursts (e.g., `DoScheduleEvent` fired after
    enqueueing new session requests) which hurts the database performance and potentially
    break the transaction isolation guarantees.
* Further refine the stability update with improved database transaction retries and the latest SQLAlchemy 1.4.x updates within the last month ([#429](https://github.com/lablup/backend.ai-manager/issues/429))
* Fix a regression that destroying a cluster session generates duplicate session termination events ([#430](https://github.com/lablup/backend.ai-manager/issues/430))

### Miscellaneous
* Temporarily pin `pytest-asyncio` to 0.14.0 due to regression of handling event loops for fixtures ([#423](https://github.com/lablup/backend.ai-manager/issues/423))


21.03.3 (2021-04-13)
--------------------

### Features
* Rewrite the session scheduler to avoid HoL blocking ([#415](https://github.com/lablup/backend.ai-manager/issues/415))
  - Skip over sessions in the queue if they fail to satisfy predicates for multiple retries -> 1st case of HoL blocking: a rogue pending session blocks everything in the same scaling group
  - You may configure the maximum number of retries in the `config/plugins/scheduler/fifo/num_retries_to_skip` etcd key.
  - Split the scheduler into two async loops for scheduling decision and session spawning by inserting "SCHEDULED" status between "PENDING" and "PREPARING" statuses -> 2nd case of HoL blocking: failure isolation with each task

### Fixes
* Adjust the firing rate of `DoPrepareEvent` to follow and alternate with the scheduler execution ([#418](https://github.com/lablup/backend.ai-manager/issues/418))


21.03.2 (2021-04-02)
--------------------

### Fixes
* Fix a regression of spawning multi-node cluster sessions due to DB API changes related to setting transaction isolation levels ([#416](https://github.com/lablup/backend.ai-manager/issues/416))


21.03.1 (2021-03-31)
--------------------

### Fixes
* Fix a missing reference fix for renaming of `gateway` to `manager.api` ([#409](https://github.com/lablup/backend.ai-manager/issues/409))
* Refactor the manager CLI initialization steps and promote `generate-keypair` as a regular `mgr` subcommand ([#411](https://github.com/lablup/backend.ai-manager/issues/411))
* Fix an internal API mismatch for our SQLAlchemy custom enum types ([#412](https://github.com/lablup/backend.ai-manager/issues/412))
* Fix a regression in session cancellation and kernel status updates after SQLAlchemy v1.4 upgrade ([#413](https://github.com/lablup/backend.ai-manager/issues/413))

### Miscellaneous
* Fix the examples for the storage proxy URL configurations in the `manager.config` module ([#410](https://github.com/lablup/backend.ai-manager/issues/410))
* Update sample configurations for etcd ([#414](https://github.com/lablup/backend.ai-manager/issues/414))


21.03.0 (2021-03-29)
--------------------

The v21.03.0 release is an integrated update for [all features and fixes already applied to v20.09 series](https://github.com/lablup/backend.ai-manager/blob/20.09/CHANGELOG.md) as additional patches, except that it now runs on top of Python 3.9.

### Features
* Add `usage` field to the `StorageVolume` graph object schema to provide the total capacity and usage ([#389](https://github.com/lablup/backend.ai-manager/issues/389))
* Add `cloneable` parameter in list_folders, update_vfolder_options and clone function in class vfolder ([#393](https://github.com/lablup/backend.ai-manager/issues/393))
* Add `is_dir` parameter to the rename_file API function to provide renaming directories in Vfolders ([#397](https://github.com/lablup/backend.ai-manager/issues/397))
* Add BACKENDAI_KERNEL_IMAGE environment variable inside a compute container. ([#401](https://github.com/lablup/backend.ai-manager/issues/401))

### Fixes
* Improve daemon shutdown stability using aiotools v1.2 ([#386](https://github.com/lablup/backend.ai-manager/issues/386))
* Fix a regression in the idle timeout checker with session creation race fixes ([#387](https://github.com/lablup/backend.ai-manager/issues/387))
* Prevent indefinite hang of the scheduler while handling agent/container launch failures and fix resource calculation when processing multiple asynchronous errors ([#388](https://github.com/lablup/backend.ai-manager/issues/388))
* Update dependencies including aiojobs, pytest, mypy, pyzmq, python-snappy, and PyYAML. ([#390](https://github.com/lablup/backend.ai-manager/issues/390))
* Refactor out `gateway.events` and move common facilities to `common.events` to improve static typing of events and reduce possibility of serialization/deserialization bugs ([#392](https://github.com/lablup/backend.ai-manager/issues/392))
* Remove b-prefix in "push_background_task_events" function ([#394](https://github.com/lablup/backend.ai-manager/issues/394))
* Explicitly use `yaml.safe_load()` for all YAML loader invocations to prevent potential future mistakes to set an unsafe loader, whcih may allow remote code execution ([#395](https://github.com/lablup/backend.ai-manager/issues/395))
* Update uvloop to 0.15.1 for better Python 3.8/3.9 support (and drop Python 3.5/3.6 support) ([#398](https://github.com/lablup/backend.ai-manager/issues/398))
* Update pyzmq to v22 series to reduce its wheel distribution size and fix a fork-safety bug introduced in v20. ([#399](https://github.com/lablup/backend.ai-manager/issues/399))
* Refactor internal dict-based state variables to be statically typed using explicitly defined attr classes, including the nested `aiohttp.web.Application` objects, the context object of Graphene resolvers, and the context object of Click. ([#400](https://github.com/lablup/backend.ai-manager/issues/400))
* Unable to fetch virtual folders list for superadmins. ([#402](https://github.com/lablup/backend.ai-manager/issues/402))
* Scheduler for scaling group was entirely broken if only sub containers are running with main container is terminated. ([#403](https://github.com/lablup/backend.ai-manager/issues/403))
* Idle timeout was not working since there was a type error on saving compute session's last_access time. ([#404](https://github.com/lablup/backend.ai-manager/issues/404))
* Follow-up fixes for #400 ([#405](https://github.com/lablup/backend.ai-manager/issues/405))
* Upgrade SQLAlchemy to v1.4 for native asyncio support and better transaction/concurrency handling ([#406](https://github.com/lablup/backend.ai-manager/issues/406))
* Add PostgreSQL version check to adjust connection arguments ([#407](https://github.com/lablup/backend.ai-manager/issues/407))
* Restructure the API layer by moving `ai.backend.gateway` modules to `ai.backend.manager.api` to make it consistent with other Backend.AI projects.
  - The module field of API-related log entries will now have `ai.backend.manager.api.` prefix instead of `ai.backend.gateway.`
  - There is no changes in the TOML configuration but the `BACKEND_GATEWAY_NPROC` environment variable is renamed to `BACKEND_MANAGER_NPROC`.
  - You must update the license activator plugin (only the Enterprise edition users).
  - You may need to update SSO/webapp plugins to adapt with the new import paths. ([#408](https://github.com/lablup/backend.ai-manager/issues/408))
