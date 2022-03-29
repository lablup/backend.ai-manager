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

<!-- towncrier release notes start -->

21.03.32 (2022-03-29)
---------------------

### Features
* Add `POST_AUTHORIZE` notify hook to support plugins that needs to do some operations just after the authorization step. ([#552](https://github.com/lablup/backend.ai-manager/issues/552))

### Fixes
* Fix a long-standing critical bug that leaked kernel creation coroutine tasks and made some sessions stuck at PREPARING ([#558](https://github.com/lablup/backend.ai-manager/issues/558))


21.03.31 (2022-03-07)
---------------------

### Fixes
* Prevent potential blocking of mutating database transactions when vfolder clone operations take too long time, by making clone operation async (background task) with transactions ([#539](https://github.com/lablup/backend.ai-manager/issues/539))
* Fix a wrong registry name in the sample etcd config ([#540](https://github.com/lablup/backend.ai-manager/issues/540))
* Correctly skip the legacy kernel images in the Docker Hub registry, prefixed with `kernel-`, while updating the image metadata. ([#545](https://github.com/lablup/backend.ai-manager/issues/545))
* Fix a bug that retried transactions even for non-serialization failures, causing excessive database overheads ([#547](https://github.com/lablup/backend.ai-manager/issues/547))


21.03.30 (2022-02-14)
---------------------

### Fixes
* Apply "read-only" attribute to a broader range of database transactions to improve overall performance ([#529](https://github.com/lablup/backend.ai-manager/issues/529))
* Force resource usage recalculation when session creation is failed to prevent failed session's resource slot not returning. ([#531](https://github.com/lablup/backend.ai-manager/issues/531))


21.03.29 (2022-01-26)
---------------------

### Fixes
* Reduce possibility for sessions stuck at PREPARING by reordering spwaning of postprocessing tasks and RPC calls ([#518](https://github.com/lablup/backend.ai-manager/issues/518))
* Prevent `AttributeError` in proxying app requests by declaring `down_task` in `TCPProxy` class explicitly. ([#519](https://github.com/lablup/backend.ai-manager/issues/519))
* Reduce possibility of new sessions to get stuck in the PREPARING status by improving synchronization of session/kernel creation trackers ([#522](https://github.com/lablup/backend.ai-manager/issues/522))
* Migrate aiodataloader to aiodataloader-ng, which is managed by us ([#525](https://github.com/lablup/backend.ai-manager/issues/525))


21.03.28 (2022-01-13)
---------------------

### Fixes
* Improve kernel creation stability by applying improved transaction retries to more database queries including predicate checks ([#514](https://github.com/lablup/backend.ai-manager/issues/514))


21.03.27 (2022-01-11)
---------------------

### Fixes
* Use a fixed value as the node ID in `EventDispatcher` instances, either auto-generated from the hostname or manually configured `manager.id` value of `manager.toml`.
  - **IMPORTANT: An explicit admin/developer action is required** to fix up the corrupted Redis database and configuration. Check out the description of [lablup/backend.ai-manager#513](https://github.com/lablup/backend.ai-manager/pull/513) for details. ([#513](https://github.com/lablup/backend.ai-manager/issues/513))


21.03.26 (2022-01-10)
---------------------

### Features
* Move 'max_containers_per_session' policy check from predicates to registry.enqueue_session ([#504](https://github.com/lablup/backend.ai-manager/issues/504))

### Fixes
* Fix "too many sessions matched" error when the given session name has an exact match with additional prefix matches ([#506](https://github.com/lablup/backend.ai-manager/issues/506))
* Update mypy to 0.930 and fix newly discovered type errors ([#508](https://github.com/lablup/backend.ai-manager/issues/508))
* Update type annotations and correct typing errors additionally found by pyright and latest mypy ([#509](https://github.com/lablup/backend.ai-manager/issues/509))
* Fix a potential reason of hang-up while shutting down the manager service, by explicitly handling cancellations in global timers better ([#510](https://github.com/lablup/backend.ai-manager/issues/510))


21.03.25 (2021-12-15)
---------------------

### Fixes
* Remove premature optimization that caches Callosum RPC Peer connections to reduce ZeroMQ handshake latencies because long-running idle connections may get silently expired by network middleboxes and unexpected hang-ups ([#497](https://github.com/lablup/backend.ai-manager/issues/497))
* Fix a regression of the usage stats aggregation API due to difference of aiopg and asyncpg behavior on `rowcount` of SELECT query results, by replacing `.rowcount` to `len()` ([#502](https://github.com/lablup/backend.ai-manager/issues/502))
* Revert introduction of busy-wait polling loop for advisory locks by #483 and rollback to blocking advisory locks in #482, while preserving the refactoring work in #483. ([#503](https://github.com/lablup/backend.ai-manager/issues/503))


21.03.24 (2021-11-08)
---------------------

### Features
* Add optional manually-assigned agent list to session creation API ([#469](https://github.com/lablup/backend.ai-manager/issues/469))
* Upgrade to aioredis v2 ([#478](https://github.com/lablup/backend.ai-manager/issues/478))
* Return manager version information in status API. This enables clients to display the current version of the manager. ([#491](https://github.com/lablup/backend.ai-manager/issues/491))
* Allow (non-superadmin) users to query/update per-vfolder quotas on their own. To help a client determine availability of per-vfolder quota option, now the response of the vfolder host list API includes volume information such as capabilities from storage proxy. ([#492](https://github.com/lablup/backend.ai-manager/issues/492))
* update_quota API returns the quota value actually set for client's reference. ([#493](https://github.com/lablup/backend.ai-manager/issues/493))
* Add a new `get_usage` API for superadmins to query the usage of an arbitrary vfolder, while users can query their vfolder usage with `get_info` API ([#494](https://github.com/lablup/backend.ai-manager/issues/494))

### Fixes
* Improve stability of session/kernel event notification APIs ([#495](https://github.com/lablup/backend.ai-manager/issues/495))


21.03.23 (2021-10-21)
---------------------

### Features
* Add the get/set APIs for size-based quota of vfolder hosts via storage proxy ([#474](https://github.com/lablup/backend.ai-manager/issues/474))
* Limit the maximum configurable value of per-vfolder quota when creating a vfolder to the size quota specified in the keypair resource policy (`max_vfolder_size`) ([#488](https://github.com/lablup/backend.ai-manager/issues/488))
* Properly implement vfolder's max_size property with storage proxy (21.03.1+ required) and change the unit of the field from KBytes to MBytes. ([#489](https://github.com/lablup/backend.ai-manager/issues/489))

### Fixes
* Fix an error in creating a virtual folder when quota is not delivered. ([#490](https://github.com/lablup/backend.ai-manager/issues/490))


21.03.22 (2021-10-07)
---------------------

### Fixes
* Filter out images with malformed tags from the response of the image list API ([#486](https://github.com/lablup/backend.ai-manager/issues/486))
* Remove `deferrable=True` option from the DB transaction to read session usage statistics. Since the manager now keeps repeatedly creating implicitly started DB transactions to acquire advisory locks (#482) and deferrable transactions barely can be started. ([#487](https://github.com/lablup/backend.ai-manager/issues/487))


21.03.21 (2021-10-05)
---------------------

### Features
* Allow configuration of the TCP keepalive timeout for the manager-to-agent RPC layer via etcd ([#485](https://github.com/lablup/backend.ai-manager/issues/485))

### Fixes
* More realistic resource preset fixture. ([#481](https://github.com/lablup/backend.ai-manager/issues/481))
* Replace aioredlock with `pg_advisory_lock` because aioredlock is no longer actively maintained and causes lots of synchronization issues ([#482](https://github.com/lablup/backend.ai-manager/issues/482))
* A follow-up fix for #482 to silence bogus DB API error upon service shutdown ([#483](https://github.com/lablup/backend.ai-manager/issues/483))
* Apply TCP keepalive options to ZeroMQ sockets for RPC channels ([#484](https://github.com/lablup/backend.ai-manager/issues/484))


21.03.20 (2021-09-02)
---------------------

### Features
* Add an Etcd option to set MTU in creating an overlay network for a cluster session to support improved performance for multi-node cluster training. ([#475](https://github.com/lablup/backend.ai-manager/issues/475))

### Fixes
* Add the missing extra requirements tag of SQLAlchemy to install greenlet and asyncpg correctly ([#470](https://github.com/lablup/backend.ai-manager/issues/470))
* Always set the scaling group when creating sessions to prevent use of non-allowed scaling groups ([#472](https://github.com/lablup/backend.ai-manager/issues/472))
* Fix a regression of `Agent.batch_load()` GraphQL resolver due to internal argument name changes ([#476](https://github.com/lablup/backend.ai-manager/issues/476))


21.03.19 (2021-08-23)
---------------------

### Features
* Add queryfilter/queryorder support for keypairs' (full_name, num_queries), users' (uuid), and kernels' (id, agent(s)) column. ([#464](https://github.com/lablup/backend.ai-manager/issues/464))

### Fixes
* Remove duplicate codes of `mount_map` check and add alias name check for `mount_map`. ([#461](https://github.com/lablup/backend.ai-manager/issues/461))
* Un-allocated resources were not excluded from the criteria of the utilization-based idle checker. ([#463](https://github.com/lablup/backend.ai-manager/issues/463))
* Fix missing timestamp updates when terminating sessions ([#465](https://github.com/lablup/backend.ai-manager/issues/465))
* Include the exact list of missing/invalid vfolders when returning `VFolderNotFound` error ([#466](https://github.com/lablup/backend.ai-manager/issues/466))
* Return the correct ID of the manager for the request to get the manager status. ([#468](https://github.com/lablup/backend.ai-manager/issues/468))

### Miscellaneous
* Update package dependencies ([#462](https://github.com/lablup/backend.ai-manager/issues/462))


21.03.18 (2021-07-22)
---------------------

### Features
* Make `ilike` operator (equivalent to SQL's `ILIKE`) available in the queryfilter to allow case-insensitive string matching ([#458](https://github.com/lablup/backend.ai-manager/issues/458))

### Fixes
* Fix handling of value transforms with array values in queryfilter binary expressions ([#459](https://github.com/lablup/backend.ai-manager/issues/459))
* Let the idle timeout checkr skip batch-type sessions as they do not have any interaction whose absence are translated to idleness ([#460](https://github.com/lablup/backend.ai-manager/issues/460))


21.03.17 (2021-07-20)
---------------------

### Fixes
* Fix a critical bug due to a missing column from the select targets of join SQL queries in the new batched GQL object resolvers for `Group.by_user` and `ScalingGroup.by_group`, which only happens with non-admin user accounts ([#457](https://github.com/lablup/backend.ai-manager/issues/457))


21.03.16 (2021-07-20)
---------------------

### Fixes
* Fix a regression of session concurrency limit checks due to transaction retry refactoring in #429 ([#455](https://github.com/lablup/backend.ai-manager/issues/455))
* Partially revert and fix #454 which introduced default connection settings of deadlock/lock/transaction-idle timeouts for PostgreSQL, to make it working with AWS RDS ([#456](https://github.com/lablup/backend.ai-manager/issues/456))


21.03.15 (2021-07-19)
---------------------

### Features
* Add lock-related DB connection settings for better DB stability when the Manager does not release a lock and/or idle for a long time after acquiring a lock. ([#454](https://github.com/lablup/backend.ai-manager/issues/454))


21.03.14 (2021-07-19)
---------------------

### Fixes
* Fix missing order_key, order_asc -> order changes in paginated list GQL queries ([#453](https://github.com/lablup/backend.ai-manager/issues/453))


21.03.13 (2021-07-19)
---------------------

### Breaking Changes
* Removed never-used `order_key` and `order_asc` arguments in GraphQL pagination queries in favor of the new generic `order` argument ([#449](https://github.com/lablup/backend.ai-manager/issues/449))

### Features
* Make an explicit error message upon `IntegrityError` due to missing scaling groups when handling agent heartbeats. ([#443](https://github.com/lablup/backend.ai-manager/issues/443))
* Now all paginated list GraphQL queries have optional `filter` and `order` arguments where the client may specify the filtering/ordering conditions using a simple mini-language expression ([#449](https://github.com/lablup/backend.ai-manager/issues/449))
* Add aiomonitor module for manager ([#450](https://github.com/lablup/backend.ai-manager/issues/450))
* Add `groups_by_name` GraphQL query to directly get group(s) from the given name ([#452](https://github.com/lablup/backend.ai-manager/issues/452))

### Fixes
* Apply missing batching of database queries for the `Group.scaling_groups` GraphQL field resolver. ([#451](https://github.com/lablup/backend.ai-manager/issues/451))
* Apply batching to user group resolution in GraphQL queries ([#452](https://github.com/lablup/backend.ai-manager/issues/452))


21.03.12 (2021-07-13)
---------------------

### Fixes
* Add a new GQL endpoint `/admin/gql` (in addition to existing `/admin/graphql`) which uses the standard-compliant response format ([#448](https://github.com/lablup/backend.ai-manager/issues/448))


21.03.11 (2021-07-13)
---------------------

### Fixes
* Handle failure of acquiring postgres advisory locks in the scheduler gracefully, by translating them as logged cancellations ([#444](https://github.com/lablup/backend.ai-manager/issues/444))
* Handle missing kernel log gracefully by adding a message about unavailability instead of panicking ([#445](https://github.com/lablup/backend.ai-manager/issues/445))
* Fix the regression of batch-type sessions by moving `startup_command` invocation to agents ([#447](https://github.com/lablup/backend.ai-manager/issues/447))


21.03.10 (2021-06-28)
---------------------

### Fixes
* Do not collect the data for utilization idle checker when the current time does not exceed the sum of the last collect time and the interval of the checker. ([#441](https://github.com/lablup/backend.ai-manager/issues/441))


21.03.9 (2021-06-18)
--------------------

### Features
* A new idle timeout checker to support utilization-based garbage collection of sessions. ([#432](https://github.com/lablup/backend.ai-manager/issues/432))

### Fixes
* Handle missing root context gracefully with explicit warning during initialization of the intrinsic error monitor plugin ([#439](https://github.com/lablup/backend.ai-manager/issues/439))


21.03.8 (2021-06-14)
--------------------

* A hotfix release to fix some mistakes in [#436](https://github.com/lablup/backend.ai-manager/issues/436)


21.03.7 (2021-06-13)
--------------------

### Features
* Add a environment variable `BACKENDAI_ACCESS_KEY` for identifying the session owner inside the session containers ([#437](https://github.com/lablup/backend.ai-manager/issues/437))

### Fixes
* Rewrite internal database connection and transaction management for GraphQL query and mutation processing, which improves overall stability and performance ([#436](https://github.com/lablup/backend.ai-manager/issues/436))


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
