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
