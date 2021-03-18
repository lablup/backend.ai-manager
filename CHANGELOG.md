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

20.09.11 (2021-03-18)
---------------------

### Fixes
* Refactor internal dict-based state variables to be statically typed using explicitly defined attr classes, including the nested `aiohttp.web.Application` objects, the context object of Graphene resolvers, and the context object of Click. ([#400](https://github.com/lablup/backend.ai-manager/issues/400))
* Idle timeout was not working since there was a type error on saving compute session's last_access time. ([#404](https://github.com/lablup/backend.ai-manager/issues/404))


20.09.10 (2021-03-08)
---------------------

### Fixes
* Scheduler for scaling group was entirely broken if only sub containers are running with main container is terminated. ([#403](https://github.com/lablup/backend.ai-manager/issues/403))


20.09.9 (2021-03-05)
--------------------

### Features
* Add BACKENDAI_KERNEL_IMAGE environment variable inside a compute container. ([#401](https://github.com/lablup/backend.ai-manager/issues/401))

### Fixes
* Unable to fetch virtual folders list for superadmins. ([#402](https://github.com/lablup/backend.ai-manager/issues/402))


20.09.8 (2021-02-22)
--------------------

### Fixes
* Update pyzmq to v22 series to reduce its wheel distribution size and fix a fork-safety bug introduced in v20. ([#399](https://github.com/lablup/backend.ai-manager/issues/399))


20.09.7 (2021-02-16)
--------------------

### BREAKING
* You must upgrade manager to 20.09.7+, agent to 20.09.5+, and common to 20.09.4+ altogether at once to make your cluster running correctly!

### Features
* Add `cloneable` parameter in list_folders, update_vfolder_options and clone function in class vfolder ([#393](https://github.com/lablup/backend.ai-manager/issues/393))
* Add `is_dir` parameter to the rename_file API function to provide renaming directories in Vfolders ([#397](https://github.com/lablup/backend.ai-manager/issues/397))

### Fixes
* Refactor out `gateway.events` and move common facilities to `common.events` to improve static typing of events and reduce possibility of serialization/deserialization bugs ([#392](https://github.com/lablup/backend.ai-manager/issues/392))
* Remove b-prefix in "push_background_task_events" function ([#394](https://github.com/lablup/backend.ai-manager/issues/394))
* Explicitly use `yaml.safe_load()` for all YAML loader invocations to prevent potential future mistakes to set an unsafe loader, whcih may allow remote code execution ([#395](https://github.com/lablup/backend.ai-manager/issues/395))
* Update uvloop to 0.15.1 for better Python 3.8/3.9 support (and drop Python 3.5/3.6 support) ([#398](https://github.com/lablup/backend.ai-manager/issues/398))


20.09.6 (2021-02-01)
--------------------

### Fixes
* Update dependencies including aiojobs, pytest, mypy, pyzmq, python-snappy, and PyYAML. ([#390](https://github.com/lablup/backend.ai-manager/issues/390))


20.09.5 (2021-01-20)
--------------------

### Features
* Add `usage` field to the `StorageVolume` graph object schema to provide the total capacity and usage ([#389](https://github.com/lablup/backend.ai-manager/issues/389))

### Fixes
* Improve daemon shutdown stability using aiotools v1.2 ([#386](https://github.com/lablup/backend.ai-manager/issues/386))
* Fix a regression in the idle timeout checker with session creation race fixes ([#387](https://github.com/lablup/backend.ai-manager/issues/387))
* Prevent indefinite hang of the scheduler while handling agent/container launch failures and fix resource calculation when processing multiple asynchronous errors ([#388](https://github.com/lablup/backend.ai-manager/issues/388))


20.09.4 (2021-01-08)
--------------------

### Features
* Add more indexes to reduce the latency for `kernels.cluster_role` involving queries ([#385](https://github.com/lablup/backend.ai-manager/issues/385))


20.09.3 (2021-01-04)
--------------------

### Fixes
* Fix reloading of new container registry configs when rescanning, which is a regression due to registry abstraction for Harbor v2 support ([#382](https://github.com/lablup/backend.ai-manager/issues/382))
* Fix `cluster_idx` for single-node sessions with only one container (`main0` should be `main1` to be consistent with multi-node sessions) ([#383](https://github.com/lablup/backend.ai-manager/issues/383))


20.09.2 (2020-12-30)
--------------------

### Fixes
* Fix the duplicate passed/failed predicate list in `status_data` ([#379](https://github.com/lablup/backend.ai-manager/issues/379))
* Fix a missing kernel ID field during cancel-termination upon failed creation of multi-container sessions ([#380](https://github.com/lablup/backend.ai-manager/issues/380))


20.09.1 (2020-12-28)
--------------------

### Fixes
* Put aiodocker in the default dependency, not testing. ([#377](https://github.com/lablup/backend.ai-manager/issues/377))
* Remove use of deprecated asyncio API (`asyncio.Task.{all_tasks,current_task}` -> `asyncio.{all_tasks,current_task}`) to run with Python 3.9 ([#378](https://github.com/lablup/backend.ai-manager/issues/378))


20.09.0 (2020-12-27)
--------------------

### Features
* Implement `max_container_per_session` keypair resource policy as a part of predicate checks ([#376](https://github.com/lablup/backend.ai-manager/issues/376))

### Fixes
* Additional stabilization of multi-node multi-container support ([#376](https://github.com/lablup/backend.ai-manager/issues/376))
  - Fix destruction of multi-container sessions when using multiple nodes
  - Adopt `MultiAgentError` to represent and handle partial failures of multi-container session creation


20.09.0rc3 (2020-12-26)
-----------------------

### Fixes
* Stabilize multi-container sessions further. ([#373](https://github.com/lablup/backend.ai-manager/issues/373))
  - Include predicate check results in `kernels.status_data` for better user-side diagnosis
  - Fix further race condition in the session-level creation routines, by adopting `session_creation_id` like the `kernel_creation_id` introduced in #374.
  - Remove the no longer used `agents.clusterized` column as it is no longer included in agent heartbeats after k8s branch refactoring.
  - Use dual DB connections to rollback agent-specific (resource) updates but to keep kernel-specific (status) updates when scheduling failures occur.
  - Fix a potential duplicate of DB cursors because use-after-free of a DB connection in the manager scheduler.
  - Relax row-level locks in scheduler in favor of the `REPEATABLE READ` isolation level.
  - Synchronize the candidate agent list when iterating over multiple kernels for a session in the multi-node mode, because they should now be updated
    according to the transaction savepoints.
  - Fix blocked proceeding to the next scaling group when a scheduling failure occurs in a scaling group.
  - Shield the DB queries in the events queue handlers and now interrupting the manager service works better.


20.09.0rc2 (2020-12-24)
-----------------------

### Fixes
* Fix races of kernel creation events by attaching a unique creation request ID to distinguish and catch the events by the caller manager instance ([#374](https://github.com/lablup/backend.ai-manager/issues/374))


20.09.0rc1 (2020-12-23)
-----------------------

### Fixes
* Unable to request watcher API due to incorrect reference to watcher endpoint. ([#370](https://github.com/lablup/backend.ai-manager/issues/370))
* Replace deprecated reference of kernels.c.role with kernels.c.cluster_role in legacy compute session list query. ([#371](https://github.com/lablup/backend.ai-manager/issues/371))
* Fix various bugs related to multi-node execution paths and provide richer error information with agent-side errors as a new JSON column `status_data` in the kernels table ([#372](https://github.com/lablup/backend.ai-manager/issues/372))


20.09.0b6 (2020-12-21)
----------------------

### Features
* Add a new API path for per-agent hardware metadata queries ([#366](https://github.com/lablup/backend.ai-manager/issues/366))


20.09.0b5 (2020-12-20)
----------------------

### Features
* Support destroying PREPARING/TERMINATING/ERROR sessions when forced parameter is delivered as True. ([#363](https://github.com/lablup/backend.ai-manager/issues/363))

### Fixes
* Fix a bug that prevents purge users and/or groups due to access to vfroot filesystem from manager directly, instead of delegating the tasks to storage-proxy. ([#361](https://github.com/lablup/backend.ai-manager/issues/361))
* Fix a race condition that sometimes corrupts the kernel status to be stuck at PREPARING even when kernels are successfully created ([#368](https://github.com/lablup/backend.ai-manager/issues/368))


20.09.0b4 (2020-12-19)
----------------------

### Fixes
* Fix a regression in image list graph queries due to a missing renaming of local_config ([#367](https://github.com/lablup/backend.ai-manager/issues/367))


20.09.0b3 (2020-12-18)
----------------------

### Fixes
* Update aiotools to v1.1.1 to fix a potential memory leak in long-lived taskgroup instances ([#362](https://github.com/lablup/backend.ai-manager/issues/362))
* Fix a hang-up issue when shutting down the gateway daemon with running service-port streaming sessions ([#365](https://github.com/lablup/backend.ai-manager/issues/365))

### Miscellaneous
* Update dependencies and adapt with the new pip resolver ([#359](https://github.com/lablup/backend.ai-manager/issues/359))
* Migrate to GitHub Actions completely for all CI jobs including tests and PyPI deployment ([#364](https://github.com/lablup/backend.ai-manager/issues/364))


20.09.0b2 (2020-12-07)
----------------------

### Features
* Add support for Harbor v2 and generalize internal abstractions of container registries, making it easier to add new registries in the future ([#357](https://github.com/lablup/backend.ai-manager/issues/357))

### Fixes
* Update fixtures to put the default registry as `cr.backend.ai` and remove unused ones ([#358](https://github.com/lablup/backend.ai-manager/issues/358))


20.09.0b1 (2020-12-02)
----------------------

### Fixes
* Improve handling of storage-manager invocation failures using a new `VFolderOperationFailed` API exception. ([#352](https://github.com/lablup/backend.ai-manager/issues/352))
* Fix missing replacement of `config_server` to `shared_config` in GraphQL context objects and references via `request.app['registry'].config_server` in some API handlers ([#354](https://github.com/lablup/backend.ai-manager/issues/354))
* Fix a regression of fetching `live_stat` GraphQL field values ([#355](https://github.com/lablup/backend.ai-manager/issues/355))
* Improve the error message for image not found errors when creating new sessions so that users could guess the reason quicklier ([#356](https://github.com/lablup/backend.ai-manager/issues/356))


20.09.0a4 (2020-11-16)
----------------------

### Features
* New idle checker to automatically kill sessions using various criteria ([#341](https://github.com/lablup/backend.ai-manager/issues/341))

### Fixes
* Implement and use a new global timer which works regardless of the number of manager instances to sustain periodic tasks such as session scheduling and idle checks ([#341](https://github.com/lablup/backend.ai-manager/issues/341))
* Replace legacy sess_id to session_name. ([#348](https://github.com/lablup/backend.ai-manager/issues/348))
* Fix intermittent resource warnings from aiopg by replacing all `fetchone()` with `first()` and `scalar()` that closes the database cursor automatically always ([#349](https://github.com/lablup/backend.ai-manager/issues/349))
* Update compute_plugins info from heartbeat for an ALIVE agent. ([#350](https://github.com/lablup/backend.ai-manager/issues/350))
* Fix broken signout. ([#353](https://github.com/lablup/backend.ai-manager/issues/353))


20.09.0a3 (2020-11-03)
----------------------

### Fixes
* Fix a regression in container-logs API since session name/ID matching generalization ([#346](https://github.com/lablup/backend.ai-manager/issues/346))
* Improve error handling in `gateway.wsproxy.TCPProxy` and use only aiohttp-supported WebSocket close codes to prevent client-side break-up ([#347](https://github.com/lablup/backend.ai-manager/issues/347))


20.09.0a2 (2020-10-30)
----------------------

### Features
* Add a new API `/session/{session_ref}/shutdown-service` for shutting down running in-container services ([#327](https://github.com/lablup/backend.ai-manager/issues/327))
* Add hooking point for AUTHORIZE with FIRST_COMPLETED requirement. ([#339](https://github.com/lablup/backend.ai-manager/issues/339))

### Fixes
* Unable to invite user to a vfolder, which is shared with another user. ([#340](https://github.com/lablup/backend.ai-manager/issues/340))
* Fix a regression of the manager status API due to internal etcd data structure changes, by returning the first-seen manager ID (for HA scenarios) to keep the compatibility of API semantics ([#342](https://github.com/lablup/backend.ai-manager/issues/342))
* Fix a regression in paginated list GraphQL query for vfolders due to missing code updates ([#343](https://github.com/lablup/backend.ai-manager/issues/343))
* Ensure removal of vfolders from the database when storage-proxy returns an error (e.g., not found) ([#344](https://github.com/lablup/backend.ai-manager/issues/344))
* Update dependencies (aiohttp~=3.7.0, trafaret~=2.1). ([#345](https://github.com/lablup/backend.ai-manager/issues/345))


20.09.0a1 (2020-10-06)
----------------------

### Breaking Changes
* The latest API version is now bumped to `v6.20200815`. ([#0](https://github.com/lablup/backend.ai-manager/issues/0))
* Configuration/DB changes are required for storage proxies ([#312](https://github.com/lablup/backend.ai-manager/issues/312))
  - To use vfolders, now the storage proxy must be installed and configured.
  - The "volumes" key in the etcd must include storage proxy configurations,
    as demonstrated in the `config/sample.volume.json` file.
  - All vfolder hosts are now in the format of "{proxy-name}:{volume-name}"
    where proxy-name is the key in the etcd and volume-name values are retrieved from
    the storage proxies at runtime.
  - All `allowed_vfolder_hosts` configurations in the database must be updated to
    the above new vfolder host format.
  - Clients should use the same vfolder host format when making API requests.

### Features
* Add support for multi-container sessions ([#217](https://github.com/lablup/backend.ai-manager/issues/217))
* Add a generic query filter expression language parser (`ai.backend.manager.models.minilang.queryfilter`) for GraphQL paginated list queries using [the Lark parser framework](https://github.com/lark-parser/lark) ([#305](https://github.com/lablup/backend.ai-manager/issues/305))
* Add support for storage proxies ([#312](https://github.com/lablup/backend.ai-manager/issues/312), [#337](https://github.com/lablup/backend.ai-manager/issues/337))
  - Storage proxies have a multi-backend architecture, so now storage-specific optimizations such as per-directory quota, performance measurements and fast metadata scanning all becomes available to Backend.AI users.
  - Offload and unify vfolder upload/download operations to storage proxies via the HTTP ranged queries and the tus.io protocol.
  - Support multiple storage proxies configured via etcd, and each storage proxy may provide multiple volumes in the mount points shared with agents.
  - Now the manager instances don't have mount points for the storage volumes, and mount/fstab management APIs skip the manager-side queries and manipulations.
* Include user information (full_name) in keypair gql query. ([#313](https://github.com/lablup/backend.ai-manager/issues/313))
* Add an endpoint that allows users to leave a shared virtual folder ([#317](https://github.com/lablup/backend.ai-manager/issues/317))
* Make the `mgr dbshell` command to be smarter to auto-detect the halfstack db container and use the container-provided psql command for maximum compatibility, with an optinal ability to set the container ID or name explicitly via `--psql-container` option and forward additional arguments to the psql command ([#318](https://github.com/lablup/backend.ai-manager/issues/318))
* Script to migrate /vroot/local to ex. /vroot/vfs structure according with new Storage Proxy implementation. ([#319](https://github.com/lablup/backend.ai-manager/issues/319))
* Make the maximum websocket message size configurable, which affects the operation of streaming APIs including service-port proxy ([#320](https://github.com/lablup/backend.ai-manager/issues/320))
* Add a new vfolder API to clone a vfolder and a property to vfolders for specifying cloneable or not ([#323](https://github.com/lablup/backend.ai-manager/issues/323), [#338](https://github.com/lablup/backend.ai-manager/issues/338))
* Add `quota` argument when creating vfolders (currently only supported in the xfs storage backend) ([#325](https://github.com/lablup/backend.ai-manager/issues/325))
* Add support for listing/updating/deleting/creating domain dotfiles and group dotfiles ([#329](https://github.com/lablup/backend.ai-manager/issues/329))
* Make vfolder's mkdir to accept and deliver parents and exist_ok option. ([#336](https://github.com/lablup/backend.ai-manager/issues/336))

### Fixes
* Prevent purging a user, group, and domain if they have active sessions and/or
  their bound resources, such as virtual folders, are being used by other sessions. ([#306](https://github.com/lablup/backend.ai-manager/issues/306))
* Fix conversion of string query params to int/bool ([#307](https://github.com/lablup/backend.ai-manager/issues/307))
* Do not create invitation if target user is already shared the folder. ([#308](https://github.com/lablup/backend.ai-manager/issues/308))
* Ignore missing tags in the Docker Hub when scanning the metadata for public Docker images, which happens after deleting images from the hub ([#309](https://github.com/lablup/backend.ai-manager/issues/309))
* Fix a regression of the GQL query to fetch the information about a single compute session ([#311](https://github.com/lablup/backend.ai-manager/issues/311))
* Return shared memory in preset list API. ([#314](https://github.com/lablup/backend.ai-manager/issues/314))
* Fix a regression in the server-side error logs query API due to a typo in the DB column name ([#315](https://github.com/lablup/backend.ai-manager/issues/315))
* Fix ErrorMonitor plugin instance to match the updates in plugin class of backend.ai-common ([#316](https://github.com/lablup/backend.ai-manager/issues/316))
* Deduplicate explicitly raised internal server errors in the daemon logs ([#322](https://github.com/lablup/backend.ai-manager/issues/322))
* Log detailed error messages when graphQl exceptions are raised. ([#324](https://github.com/lablup/backend.ai-manager/issues/324))
* Fix a regression of statistics API due to a wrong default value type ([#326](https://github.com/lablup/backend.ai-manager/issues/326))
* Fix vfolder mounts for compute sessions using agent host paths provided by the storage proxy ([#328](https://github.com/lablup/backend.ai-manager/issues/328))
* Hand over recursive parameter to storage proxy when deleting files. ([#330](https://github.com/lablup/backend.ai-manager/issues/330))
* Validate service port declarations before starting the session ([#333](https://github.com/lablup/backend.ai-manager/issues/333))
* Improve pickling of wrapped HTTP exceptions (`BackendError` and its derivatives) ([#334](https://github.com/lablup/backend.ai-manager/issues/334))
* Show the container name when using the `mgr dbshell` cli command ([#335](https://github.com/lablup/backend.ai-manager/issues/335))


20.03.0 (2020-07-28)
--------------------

### Features
* Add purge mutations for users, groups, and domains. ([#302](https://github.com/lablup/backend.ai-manager/issues/302))

### Fixes
* Raise explicit exception when no keypair is found during login. ([#297](https://github.com/lablup/backend.ai-manager/issues/297))
* Adds context for the call to from_row in simple_db_mutate_returning_item. ([#300](https://github.com/lablup/backend.ai-manager/issues/300))
* The session creation API now returns the session UUID as part of its response, in addition to the meaningless client-provided session alias (name) ([#303](https://github.com/lablup/backend.ai-manager/issues/303))

### Miscellaneous
* Return explicit message if user needs verification during login. ([#301](https://github.com/lablup/backend.ai-manager/issues/301))


20.03.0rc1 (2020-07-23)
-----------------------

### Breaking Changes
* Replace user's boolean-based `is_active` field to a enum-based `status` field to represent multiple user statuses with lifecycle state transitions, changing the signup API and related DB columns.
* Change the argument type of `user_id` in `user_from_uuid` GraphQL query to the generic `ID` ([#299](https://github.com/lablup/backend.ai-manager/issues/299))

### Fixes
* Fix misuse of a legacy API in the `etcd move-subtree` manager command, which has unintentionally flattened nested dicts when inserting the data to the new location ([#295](https://github.com/lablup/backend.ai-manager/issues/295))
* Mask sensitive keys when logging API parameters for debugging ([#296](https://github.com/lablup/backend.ai-manager/issues/296))
* A set of API behavior fixes ([#299](https://github.com/lablup/backend.ai-manager/issues/299))
  - Fix missing conversion of `user_id` argument to UUID type when resolving the `user_from_uuid` GraphQL query
  - Exclude cancelled sessions from the active session count in the manager status API
  - Fix missing update for user's `status_info` as "admin-requested" when marked as deleted by administrators

### Miscellaneous
* Refactor session usage api. ([#298](https://github.com/lablup/backend.ai-manager/issues/298))


20.03.0b2 (2020-07-02)
----------------------

### Breaking Changes
* Apply the plugin API v2 -- all stats/error/webapp/hook plugins must be updated along with the manager ([#291](https://github.com/lablup/backend.ai-manager/issues/291))

### Features
* Group-shared vfolders named ".local" (while other dot-prefixed names are not allowed for group vfolders) now have per-user subdirectories (using user UUIDs) for per-user persistent package installation reusable across different sessions ([#224](https://github.com/lablup/backend.ai-manager/issues/224))
* Download a directory from vfolder as an archive. ([#278](https://github.com/lablup/backend.ai-manager/issues/278))
* Execute POST_SIGNUP hook after sign up. ([#286](https://github.com/lablup/backend.ai-manager/issues/286))
* Pass Accept-Language header to post-signup hook. ([#287](https://github.com/lablup/backend.ai-manager/issues/287))
* Add a feature to kick off user who is shared virtual folder. ([#288](https://github.com/lablup/backend.ai-manager/issues/288))
* Add a global announcement get/set API which stores an arbitrary string in the "manager/announcement" etcd key. ([#289](https://github.com/lablup/backend.ai-manager/issues/289))
* Add option to reserve batch session. ([#290](https://github.com/lablup/backend.ai-manager/issues/290))
* Add modified_at column to users and keypairs table. ([#293](https://github.com/lablup/backend.ai-manager/issues/293))
* Add super-admin APIs to temporarily exclude specific agents from scheduling and include later by changing the `schedulable` column of the `agents` table. ([#294](https://github.com/lablup/backend.ai-manager/issues/294))

### Fixes
* Destroying sessions now returns periodically collected statistics instead of last-moment statistics ([#279](https://github.com/lablup/backend.ai-manager/issues/279))
* Apply batching to statistics synchroniztion to minimize time used inside DB transactions by batching and splitting transaction blocks ([#280](https://github.com/lablup/backend.ai-manager/issues/280))
* Add fallback if any keys under last_stat have None value during statistics calculation. ([#282](https://github.com/lablup/backend.ai-manager/issues/282))
* Fix the responses for `compute_session` GQL query when there are no or multiple matching sessions ([#283](https://github.com/lablup/backend.ai-manager/issues/283))
* ([#284](https://github.com/lablup/backend.ai-manager/issues/284))
  - Fix too many scheduler invocation with stale periodic scheduling ticks after manager restarts/maintenance
  - Split DB transactions for schedulers as fine-grained, small-sized transactions to cope with a large number of pending sessions
* Ensure resolving of kernel IDs before doing complex operations in `AgentRegistry` to prevent potential races against the unique constraints about kernel statuses and user-defined session names under heavy loads ([#292](https://github.com/lablup/backend.ai-manager/issues/292))


20.03.0b1 (2020-05-12)
----------------------

### Breaking Changes
* Now it runs on Python 3.8 or higher only.
* Rename all APIs to use "session" instead of "kernel" ([#216](https://github.com/lablup/backend.ai-manager/issues/216))
* API v5 support
  - All session-related GraphQL schema is rewritten to reflect multi-container sessions and kernel -> session parameter and API renamings. ([#263](https://github.com/lablup/backend.ai-manager/issues/263))

### Features
* Un-numbered features
  - Now all CLI commands are accessible via ``backend.ai mgr``. [(lablup/backend.ai#101)](https://github.com/lablup/backend.ai/issues/101)
  - Add more convenient manager CLI commands: `etcd quote`, `etcd unquote`, `etcd move-subtree`
  - Add `--short` and `--installed` options to the `etcd list-images` command.
  - Add a minimum-occupied slot first scheduler.
* Now our manager-to-agent RPC uses [Callosum](https://github.com/lablup/callosum) instead of aiozmq, supporting Python 3.8 natively. ([#209](https://github.com/lablup/backend.ai-manager/issues/209))
* Add vfolder large-file upload APIs using the tus.io protocol. ([#210](https://github.com/lablup/backend.ai-manager/issues/210))
* User-customizable per-scaling-group session queue scheduler plugins and three intrinsic plugins: FIFO, LIFO, and the DRF (dominant resource fairness) scheduler. ([#212](https://github.com/lablup/backend.ai-manager/issues/212))
* User-manageable session templates written in YAML to reuse session creation parameters. ([#213](https://github.com/lablup/backend.ai-manager/issues/213))
* ResourceSlots are now more permissive so that agents with different sets of accelerator plugins can now coexist in a single cluster. ([#214](https://github.com/lablup/backend.ai-manager/issues/214))
* Add pre-open service ports to allow user-written applications listening on a container port and make such ports accessible via the stream proxy API. ([#221](https://github.com/lablup/backend.ai-manager/issues/221))
* Auto-fill the minimum resource slots for intrinsic slots only to allow execution of sessions with different set of accelerators simultaneously. ([#234](https://github.com/lablup/backend.ai-manager/issues/234))
* Error logging API and an intrinsic plugin to store all unhandled exceptions in agents and managers.
  (Agent exceptions are passed via the event bus) ([#235](https://github.com/lablup/backend.ai-manager/issues/235))
* Reduce possibility of aioredlock locking errors. ([#236](https://github.com/lablup/backend.ai-manager/issues/236))
* Favor CPU agents for session creation requests without accelerators ([#241](https://github.com/lablup/backend.ai-manager/issues/241))
* Add user-config APIs to update/get keypair bootstrap script ([#253](https://github.com/lablup/backend.ai-manager/issues/253))
* Accept session UUIDs and their prefixes in addition to client-specified IDs (names) ([#258](https://github.com/lablup/backend.ai-manager/issues/258))
* API v5 support ([#263](https://github.com/lablup/backend.ai-manager/issues/263))
  - Add full implementation of the background-task tracking API
  - Apply background-task tracking API to the image rescanning API
  - Add paginated UserList and KeyPairList GraphQL queries
* Extend vfolder to have usage mode and innate permission ([#265](https://github.com/lablup/backend.ai-manager/issues/265))
* Add shared memory column to resource preset ([#267](https://github.com/lablup/backend.ai-manager/issues/267))

### Fixes
* Fix regression of the docker registry list fetch API (`/config/docker-registries`) ([#259](https://github.com/lablup/backend.ai-manager/issues/259))
* Fix errors on fetching server logs and vfolder list ([#264](https://github.com/lablup/backend.ai-manager/issues/264))
* Remove unwanted empty key while setting etcd config with nested dict. ([#275](https://github.com/lablup/backend.ai-manager/issues/275))

### Miscellaneous
* Internally refactored the main function for easier writing of future unit tests by composing different resource cleanup contexts in a modular way, using aiohttp's cleanup contexts.
* Now all changelogs are managed using [towncrier](https://github.com/twisted/towncrier) and migrated to Markdown syntax. ([#262](https://github.com/lablup/backend.ai-manager/issues/262))
* Refactor `batch_result()` and `batch_multiresult()` used for optimizing SQL-based GraphQL queries ([#263](https://github.com/lablup/backend.ai-manager/issues/263))
* Return NaN when group-level resource status is not allowed to the client. ([#268](https://github.com/lablup/backend.ai-manager/issues/268))
* Update flake8 to a prerelease version supporting Python 3.8 syntaxes ([#277](https://github.com/lablup/backend.ai-manager/issues/277))
