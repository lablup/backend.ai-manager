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

## 21.09.7 (2022-02-14)

### Features
* Allow vfolder mounting to arbitrary path, excluding pre-existing folders of '/' like '/bin'. ([#516](https://github.com/lablup/backend.ai-manager/issues/516))

### Fixes
* Prevent redis pool depletion in proxying streaming app requests by executing the entire connection tracker script in a non-bursty manner. ([#526](https://github.com/lablup/backend.ai-manager/issues/526))
* Apply "read-only" attribute to a broader range of database transactions to improve overall performance ([#529](https://github.com/lablup/backend.ai-manager/issues/529))
* Force resource usage recalculation when session creation is failed to prevent failed session's resource slot not returning. ([#531](https://github.com/lablup/backend.ai-manager/issues/531))


## 21.09.6 (2022-01-26)

### Fixes
* Reduce possibility for sessions stuck at PREPARING by reordering spwaning of postprocessing tasks and RPC calls ([#518](https://github.com/lablup/backend.ai-manager/issues/518))
* Prevent `AttributeError` in proxying app requests by declaring `down_task` in `TCPProxy` class explicitly. ([#519](https://github.com/lablup/backend.ai-manager/issues/519))
* Reduce possibility of new sessions to get stuck in the PREPARING status by improving synchronization of session/kernel creation trackers ([#522](https://github.com/lablup/backend.ai-manager/issues/522))
* Migrate aiodataloader to aiodataloader-ng, which is managed by us ([#525](https://github.com/lablup/backend.ai-manager/issues/525))

### Miscellaneous
* Change the default values of max concurrent sessions (30 -> 5) and idle timeout (600 -> 3600) in keypair resource policy fixture to conform to the preferable defaults. ([#512](https://github.com/lablup/backend.ai-manager/issues/512))


## 21.09.5 (2022-01-13)

### Fixes
* Improve kernel creation stability by applying improved transaction retries to more database queries including predicate checks ([#514](https://github.com/lablup/backend.ai-manager/issues/514))


## 21.09.4 (2022-01-11)

### Fixes
* Use a fixed value as the node ID in `EventDispatcher` instances, either auto-generated from the hostname or manually configured `manager.id` value of `manager.toml`.
  - **IMPORTANT: An explicit admin/developer action is required** to fix up the corrupted Redis database and configuration. Check out the description of [lablup/backend.ai-manager#513](https://github.com/lablup/backend.ai-manager/pull/513) for details. ([#513](https://github.com/lablup/backend.ai-manager/issues/513))


## 21.09.3 (2022-01-10)

### Features
* Add `session.start_service` API to support wsproxy v2 ([#479](https://github.com/lablup/backend.ai-manager/issues/479))
* Move 'max_containers_per_session' policy check from predicates to registry.enqueue_session ([#504](https://github.com/lablup/backend.ai-manager/issues/504))
* Add support for session renaming ([#505](https://github.com/lablup/backend.ai-manager/issues/505))

### Fixes
* Fix "too many sessions matched" error when the given session name has an exact match with additional prefix matches ([#506](https://github.com/lablup/backend.ai-manager/issues/506))
* Update mypy to 0.930 and fix newly discovered type errors ([#508](https://github.com/lablup/backend.ai-manager/issues/508))
* Update type annotations and correct typing errors additionally found by pyright and latest mypy ([#509](https://github.com/lablup/backend.ai-manager/issues/509))
* Fix a potential reason of hang-up while shutting down the manager service, by explicitly handling cancellations in global timers better ([#510](https://github.com/lablup/backend.ai-manager/issues/510))


## 21.09.2 (2021-12-15)

### Features
* Update CRUD of session template and correct typo of example-session-templates.json ([#480](https://github.com/lablup/backend.ai-manager/issues/480))
* Add `mgr clear-history` cli command to delete old records from the kernels table and clear up the actual disk space. ([#498](https://github.com/lablup/backend.ai-manager/issues/498))
* Add a new GQL mutation to modify the `schedulable` attribute of agents ([#500](https://github.com/lablup/backend.ai-manager/issues/500))

### Fixes
* Remove premature optimization that caches Callosum RPC Peer connections to reduce ZeroMQ handshake latencies because long-running idle connections may get silently expired by network middleboxes and unexpected hang-ups ([#497](https://github.com/lablup/backend.ai-manager/issues/497))
* Fix a regression of the usage stats aggregation API due to difference of aiopg and asyncpg behavior on `rowcount` of SELECT query results, by replacing `.rowcount` to `len()` ([#502](https://github.com/lablup/backend.ai-manager/issues/502))
* Revert introduction of busy-wait polling loop for advisory locks by #483 and rollback to blocking advisory locks in #482, while preserving the refactoring work in #483. ([#503](https://github.com/lablup/backend.ai-manager/issues/503))


## 21.09.1 (2021-11-11)

### Fixes
* Upgrade aiohttp from 3.7 to 3.8 series ([#496](https://github.com/lablup/backend.ai-manager/issues/496))


## 21.09.0 (2021-11-08)

### Features
* Add optional manually-assigned agent list to session creation API ([#469](https://github.com/lablup/backend.ai-manager/issues/469))
* Upgrade to aioredis v2 ([#478](https://github.com/lablup/backend.ai-manager/issues/478))
* Allow configuration of the TCP keepalive timeout for the manager-to-agent RPC layer via etcd ([#485](https://github.com/lablup/backend.ai-manager/issues/485))
* Limit the maximum configurable value of per-vfolder quota when creating a vfolder to the size quota specified in the keypair resource policy (`max_vfolder_size`) ([#488](https://github.com/lablup/backend.ai-manager/issues/488))
* Properly implement vfolder's max_size property with storage proxy (21.03.1+ required) and change the unit of the field from KBytes to MBytes. ([#489](https://github.com/lablup/backend.ai-manager/issues/489))
* Return manager version information in status API. This enables clients to display the current version of the manager. ([#491](https://github.com/lablup/backend.ai-manager/issues/491))
* Allow (non-superadmin) users to query/update per-vfolder quotas on their own. To help a client determine availability of per-vfolder quota option, now the response of the vfolder host list API includes volume information such as capabilities from storage proxy. ([#492](https://github.com/lablup/backend.ai-manager/issues/492))
* update_quota API returns the quota value actually set for client's reference. ([#493](https://github.com/lablup/backend.ai-manager/issues/493))
* Add a new `get_usage` API for superadmins to query the usage of an arbitrary vfolder, while users can query their vfolder usage with `get_info` API ([#494](https://github.com/lablup/backend.ai-manager/issues/494))

### Fixes
* More realistic resource preset fixture. ([#481](https://github.com/lablup/backend.ai-manager/issues/481))
* Replace aioredlock with `pg_advisory_lock` because aioredlock is no longer actively maintained and causes lots of synchronization issues ([#482](https://github.com/lablup/backend.ai-manager/issues/482))
* A follow-up fix for #482 to silence bogus DB API error upon service shutdown ([#483](https://github.com/lablup/backend.ai-manager/issues/483))
* Apply TCP keepalive options to ZeroMQ sockets for RPC channels ([#484](https://github.com/lablup/backend.ai-manager/issues/484))
* Filter out images with malformed tags from the response of the image list API ([#486](https://github.com/lablup/backend.ai-manager/issues/486))
* Remove `deferrable=True` option from the DB transaction to read session usage statistics. Since the manager now keeps repeatedly creating implicitly started DB transactions to acquire advisory locks (#482) and deferrable transactions barely can be started. ([#487](https://github.com/lablup/backend.ai-manager/issues/487))
* Fix an error in creating a virtual folder when quota is not delivered. ([#490](https://github.com/lablup/backend.ai-manager/issues/490))
* Improve stability of session/kernel event notification APIs ([#495](https://github.com/lablup/backend.ai-manager/issues/495))

### Miscellaneous
* Update missing licenses and add project links in DEPENDENCIES.md ([#471](https://github.com/lablup/backend.ai-manager/issues/471))


## 21.09.0a2 (2021-09-28)

### Features
* Add the get/set APIs for size-based quota of vfolder hosts via storage proxy ([#474](https://github.com/lablup/backend.ai-manager/issues/474))
* Add an Etcd option to set MTU in creating an overlay network for a cluster session to support improved performance for multi-node cluster training. ([#475](https://github.com/lablup/backend.ai-manager/issues/475))

### Fixes
* Always set the scaling group when creating sessions to prevent use of non-allowed scaling groups ([#472](https://github.com/lablup/backend.ai-manager/issues/472))
* Rearrange the order of checking vfolder mount aliases to fix emptiness and null checks to come at the right order ([#473](https://github.com/lablup/backend.ai-manager/issues/473))
* Fix a regression of `Agent.batch_load()` GraphQL resolver due to internal argument name changes ([#476](https://github.com/lablup/backend.ai-manager/issues/476))


## 21.09.0a1 (2021-08-25)

### Breaking Changes
* Removed never-used `order_key` and `order_asc` arguments in GraphQL pagination queries in favor of the new generic `order` argument ([#449](https://github.com/lablup/backend.ai-manager/issues/449))

### Features
* Rewrite the session scheduler to avoid HoL blocking ([#415](https://github.com/lablup/backend.ai-manager/issues/415))
   - Skip over sessions in the queue if they fail to satisfy predicates for multiple retries -> 1st case of HoL blocking: a rogue pending session blocks everything in the same scaling group
   - You may configure the maximum number of retries in the `config/plugins/scheduler/fifo/num_retries_to_skip` etcd key.
   - Split the scheduler into two async loops for scheduling decision and session spawning by inserting "SCHEDULED" status between "PENDING" and "PREPARING" statuses -> 2nd case of HoL blocking: failure isolation with each task
* Add an API endpoint to share/unshare a group virtual folder directly to specific users. This is to allow specified users (usually teachers with user account) can upload data/materials to a virtual folder while it is shared as read-only for other group users. ([#419](https://github.com/lablup/backend.ai-manager/issues/419))
* Add `PRE_AUTH_MIDDLEWARE` hook for cookie-based SSO plugins ([#420](https://github.com/lablup/backend.ai-manager/issues/420))
* Add update_full_name API to rename user's full_name regardless of role. ([#424](https://github.com/lablup/backend.ai-manager/issues/424))
* Modify the loading process so that the scheduler can be loaded reflecting scheduler_opts. ([#428](https://github.com/lablup/backend.ai-manager/issues/428))
* A new idle timeout checker to support utilization-based garbage collection of sessions. ([#432](https://github.com/lablup/backend.ai-manager/issues/432))
* Add a common session environment variable `BACKENDAI_SESSION_NAME` for improved prompts and user acquaintance of which container they use ([#433](https://github.com/lablup/backend.ai-manager/issues/433))
* Add an internal warning logs for excessive number of concurrent DB transactions ([#435](https://github.com/lablup/backend.ai-manager/issues/435))
* Add a environment variable `BACKENDAI_ACCESS_KEY` for identifying the session owner inside the session containers ([#437](https://github.com/lablup/backend.ai-manager/issues/437))
* Make an explicit error message upon `IntegrityError` due to missing scaling groups when handling agent heartbeats. ([#443](https://github.com/lablup/backend.ai-manager/issues/443))
* Now all paginated list GraphQL queries have optional `filter` and `order` arguments where the client may specify the filtering/ordering conditions using a simple mini-language expression ([#449](https://github.com/lablup/backend.ai-manager/issues/449))
* Add aiomonitor module for manager ([#450](https://github.com/lablup/backend.ai-manager/issues/450))
* Add `groups_by_name` GraphQL query to directly get group(s) from the given name ([#452](https://github.com/lablup/backend.ai-manager/issues/452))
* Add lock-related DB connection settings for better DB stability when the Manager does not release a lock and/or idle for a long time after acquiring a lock. ([#454](https://github.com/lablup/backend.ai-manager/issues/454))
* Make `ilike` operator (equivalent to SQL's `ILIKE`) available in the queryfilter to allow case-insensitive string matching ([#458](https://github.com/lablup/backend.ai-manager/issues/458))
* Add queryfilter/queryorder support for keypairs' (full_name, num_queries), users' (uuid), and kernels' (id, agent(s)) column. ([#464](https://github.com/lablup/backend.ai-manager/issues/464))

### Fixes
* Fix a missing reference fix for renaming of `gateway` to `manager.api` ([#409](https://github.com/lablup/backend.ai-manager/issues/409))
* Refactor the manager CLI initialization steps and promote `generate-keypair` as a regular `mgr` subcommand ([#411](https://github.com/lablup/backend.ai-manager/issues/411))
* Fix an internal API mismatch for our SQLAlchemy custom enum types ([#412](https://github.com/lablup/backend.ai-manager/issues/412))
* Fix a regression in session cancellation and kernel status updates after SQLAlchemy v1.4 upgrade ([#413](https://github.com/lablup/backend.ai-manager/issues/413))
* Fix a regression of spawning multi-node cluster sessions due to DB API changes related to setting transaction isolation levels ([#416](https://github.com/lablup/backend.ai-manager/issues/416))
* Adjust the firing rate of `DoPrepareEvent` to follow and alternate with the scheduler execution ([#418](https://github.com/lablup/backend.ai-manager/issues/418))
* Change the `KeyPair.num_queries` GQL field to use Redis instead of the `keypairs.num_queries` DB column to avoid excessive DB writes ([#421](https://github.com/lablup/backend.ai-manager/issues/421))
* Improve stability and synchronization of container-databse states ([#425](https://github.com/lablup/backend.ai-manager/issues/425))
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
* Optimize read-only GraphQL queries to use read-only transaction isolation level, which greatly reduces the database loads when using GUI ([#431](https://github.com/lablup/backend.ai-manager/issues/431))
* Fix `owner_access_key` related issues in creating and terminating the session ([#434](https://github.com/lablup/backend.ai-manager/issues/434))
  - Remove automatic removal of owner_access_key in check_api_params() since all API handlers supporting it has explicit trafaret definition of it
  - Add `owner_access_key` in checking API parameter during session termination
* Rewrite internal database connection and transaction management for GraphQL query and mutation processing, which improves overall stability and performance ([#436](https://github.com/lablup/backend.ai-manager/issues/436))
* Handle missing root context gracefully with explicit warning during initialization of the intrinsic error monitor plugin ([#439](https://github.com/lablup/backend.ai-manager/issues/439))
* Do not collect the data for utilization idle checker when the current time does not exceed the sum of the last collect time and the interval of the checker. ([#441](https://github.com/lablup/backend.ai-manager/issues/441))
* Handle failure of acquiring postgres advisory locks in the scheduler gracefully, by translating them as logged cancellations ([#444](https://github.com/lablup/backend.ai-manager/issues/444))
* Handle missing kernel log gracefully by adding a message about unavailability instead of panicking ([#445](https://github.com/lablup/backend.ai-manager/issues/445))
* Fix the regression of batch-type sessions by moving `startup_command` invocation to agents ([#447](https://github.com/lablup/backend.ai-manager/issues/447))
* Add a new GQL endpoint `/admin/gql` (in addition to existing `/admin/graphql`) which uses the standard-compliant response format ([#448](https://github.com/lablup/backend.ai-manager/issues/448))
* Apply missing batching of database queries for the `Group.scaling_groups` GraphQL field resolver. ([#451](https://github.com/lablup/backend.ai-manager/issues/451))
* Apply batching to user group resolution in GraphQL queries ([#452](https://github.com/lablup/backend.ai-manager/issues/452))
* Fix missing order_key, order_asc -> order changes in paginated list GQL queries ([#453](https://github.com/lablup/backend.ai-manager/issues/453))
* Fix a regression of session concurrency limit checks due to transaction retry refactoring in #429 ([#455](https://github.com/lablup/backend.ai-manager/issues/455))
* Partially revert and fix #454 which introduced default connection settings of deadlock/lock/transaction-idle timeouts for PostgreSQL, to make it working with AWS RDS ([#456](https://github.com/lablup/backend.ai-manager/issues/456))
* Fix a critical bug due to a missing column from the select targets of join SQL queries in the new batched GQL object resolvers for `Group.by_user` and `ScalingGroup.by_group`, which only happens with non-admin user accounts ([#457](https://github.com/lablup/backend.ai-manager/issues/457))
* Fix handling of value transforms with array values in queryfilter binary expressions ([#459](https://github.com/lablup/backend.ai-manager/issues/459))
* Let the idle timeout checkr skip batch-type sessions as they do not have any interaction whose absence are translated to idleness ([#460](https://github.com/lablup/backend.ai-manager/issues/460))
* Remove duplicate codes of `mount_map` check and add alias name check for `mount_map`. ([#461](https://github.com/lablup/backend.ai-manager/issues/461))
* Un-allocated resources were not excluded from the criteria of the utilization-based idle checker. ([#463](https://github.com/lablup/backend.ai-manager/issues/463))
* Fix missing timestamp updates when terminating sessions ([#465](https://github.com/lablup/backend.ai-manager/issues/465))
* Include the exact list of missing/invalid vfolders when returning `VFolderNotFound` error ([#466](https://github.com/lablup/backend.ai-manager/issues/466))
* Return the correct ID of the manager for the request to get the manager status. ([#468](https://github.com/lablup/backend.ai-manager/issues/468))
* Add the missing extra requirements tag of SQLAlchemy to install greenlet and asyncpg correctly ([#470](https://github.com/lablup/backend.ai-manager/issues/470))

### Miscellaneous
* Fix the examples for the storage proxy URL configurations in the `manager.config` module ([#410](https://github.com/lablup/backend.ai-manager/issues/410))
* Update sample configurations for etcd ([#414](https://github.com/lablup/backend.ai-manager/issues/414))
* Temporarily pin `pytest-asyncio` to 0.14.0 due to regression of handling event loops for fixtures ([#423](https://github.com/lablup/backend.ai-manager/issues/423))
* Update package dependencies ([#462](https://github.com/lablup/backend.ai-manager/issues/462))


## Older changelogs

* [21.03](https://github.com/lablup/backend.ai-manager/blob/21.03/CHANGELOG.md)
* [20.09](https://github.com/lablup/backend.ai-manager/blob/20.09/CHANGELOG.md)
* [20.03](https://github.com/lablup/backend.ai-manager/blob/20.03/CHANGELOG.md)
