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

## 22.03.0a2 (2022-03-29)

### Features
* Support inter-session dependencies by introducing a new parameter `dependencies` to the session creation APIs and implementing a scheduler predicate check to ensure all dependency (batch-type) sessions to become successful ([#528](https://github.com/lablup/backend.ai-manager/issues/528))
* Migrate keypair concurrency check from PostgreSQL to Redis ([#535](https://github.com/lablup/backend.ai-manager/issues/535))
* Add `POST_AUTHORIZE` notify hook to support plugins that needs to do some operations just after the authorization step. ([#552](https://github.com/lablup/backend.ai-manager/issues/552))

### Fixes
* Use Redis to persistently store and update per-kernel statistics while keeping the `kernels.last_stat` db column as a backup, with a debug option to periodically sync them ([#532](https://github.com/lablup/backend.ai-manager/issues/532))
* Give `load_aliases` argument when it is needed to query aliases relationship. ([#551](https://github.com/lablup/backend.ai-manager/issues/551))
* Fix a regression introduced in #541 by correcting a missing replacement of `sqlalchemy.engine.Connection` with `sqlalchemy.orm.Session` as `ImageRow` is now an ORM object ([#553](https://github.com/lablup/backend.ai-manager/issues/553))
* Recalculate `kernels.occupied_slots` column with actually allocated resource slots value. ([#554](https://github.com/lablup/backend.ai-manager/issues/554))
* Reduce the occurrence of hang-up during manager shutdown greatly and force-kill hanging worker processes to eliminate it ([#555](https://github.com/lablup/backend.ai-manager/issues/555))
* Fix a long-standing critical bug that leaked kernel creation coroutine tasks and made some sessions stuck at PREPARING ([#558](https://github.com/lablup/backend.ai-manager/issues/558))
* Alter the `agents.architecture` column type from `CHAR` to `String` to prevent implicit addition of trailing whitespaces ([#559](https://github.com/lablup/backend.ai-manager/issues/559))
* Fix up newly found type errors for updating `common.etcd` using `etcetra` ([#566](https://github.com/lablup/backend.ai-manager/issues/566))


## 22.03.0a1 (2022-03-14)

### Breaking Changes
* Now it requires Python 3.10 or higher to run. ([#550](https://github.com/lablup/backend.ai-manager/issues/550))

### Features
* Add `session.start_service` API to support wsproxy v2 ([#479](https://github.com/lablup/backend.ai-manager/issues/479))
* Update CRUD of session template and correct typo of example-session-templates.json ([#480](https://github.com/lablup/backend.ai-manager/issues/480))
* Add `mgr clear-history` cli command to delete old records from the kernels table and clear up the actual disk space. ([#498](https://github.com/lablup/backend.ai-manager/issues/498))
* Add a new GQL mutation to modify the `schedulable` attribute of agents ([#500](https://github.com/lablup/backend.ai-manager/issues/500))
* Move 'max_containers_per_session' policy check from predicates to registry.enqueue_session ([#504](https://github.com/lablup/backend.ai-manager/issues/504))
* Add support for session renaming ([#505](https://github.com/lablup/backend.ai-manager/issues/505))
* Allow modifying `scaling_group` field of an agent via the GraphQL mutation query, including an RPC call to the agent to update its own local configuration ([#511](https://github.com/lablup/backend.ai-manager/issues/511))
* Allow vfolder mounting to arbitrary path, excluding pre-existing folders of '/' like '/bin'. ([#516](https://github.com/lablup/backend.ai-manager/issues/516))
* Check the allowed session types per scaling group as part of scheduler predicate checks and structurize the `scheduler_opts` column by introducing `StructuredJSONBColumn` which applies trafarets on raw/Python value conversion ([#523](https://github.com/lablup/backend.ai-manager/issues/523))
* Make shielded async functions to spawn inside `aiotools.PersistentTaskGroup` to ensure proper cancellation on shutdown ([#533](https://github.com/lablup/backend.ai-manager/issues/533))
* Allow mounting subpath of vfolders if specified as relative paths appended to vfolder names and improve storage error propagation.  Also introduce `StructuredJSONObjectColumn` and `StructuredJSONObjectListColumn` to define database columns based on `common.types.JSONSerializableMixin`. ([#537](https://github.com/lablup/backend.ai-manager/issues/537))
* Add support for multi-architecture images, allowing heterogeneous agents in a single cluster with scheduler updates to match architectures of images and agents.  Also migrate the manager's image database from etcd to the PostgreSQL database. ([#541](https://github.com/lablup/backend.ai-manager/issues/541))
* Allow superadmins to list vfolders as specific users ([#544](https://github.com/lablup/backend.ai-manager/issues/544))
* Add a new vfolder move-file API which works like the `mv` shell command within a vfolder and improve storage proxy error propagation ([#548](https://github.com/lablup/backend.ai-manager/issues/548))

### Fixes
* Remove premature optimization that caches Callosum RPC Peer connections to reduce ZeroMQ handshake latencies because long-running idle connections may get silently expired by network middleboxes and unexpected hang-ups ([#497](https://github.com/lablup/backend.ai-manager/issues/497))
* Fix a regression of the usage stats aggregation API due to difference of aiopg and asyncpg behavior on `rowcount` of SELECT query results, by replacing `.rowcount` to `len()` ([#502](https://github.com/lablup/backend.ai-manager/issues/502))
* Revert introduction of busy-wait polling loop for advisory locks by #483 and rollback to blocking advisory locks in #482, while preserving the refactoring work in #483. ([#503](https://github.com/lablup/backend.ai-manager/issues/503))
* Fix "too many sessions matched" error when the given session name has an exact match with additional prefix matches ([#506](https://github.com/lablup/backend.ai-manager/issues/506))
* Update mypy to 0.930 and fix newly discovered type errors ([#508](https://github.com/lablup/backend.ai-manager/issues/508))
* Update type annotations and correct typing errors additionally found by pyright and latest mypy ([#509](https://github.com/lablup/backend.ai-manager/issues/509))
* Fix a potential reason of hang-up while shutting down the manager service, by explicitly handling cancellations in global timers better ([#510](https://github.com/lablup/backend.ai-manager/issues/510))
* Use a fixed value as the node ID in `EventDispatcher` instances, either auto-generated from the hostname or manually configured `manager.id` value of `manager.toml`.
  - **IMPORTANT: An explicit admin/developer action is required** to fix up the corrupted Redis database and configuration. Check out the description of [lablup/backend.ai-manager#513](https://github.com/lablup/backend.ai-manager/pull/513) for details. ([#513](https://github.com/lablup/backend.ai-manager/issues/513))
* Improve kernel creation stability by applying improved transaction retries to more database queries including predicate checks ([#514](https://github.com/lablup/backend.ai-manager/issues/514))
* Fix get_wsproxy_version API raising GenericNotFound when user's current domain isn't associated with target scaling group ([#517](https://github.com/lablup/backend.ai-manager/issues/517))
* Reduce possibility for sessions stuck at PREPARING by reordering spwaning of postprocessing tasks and RPC calls ([#518](https://github.com/lablup/backend.ai-manager/issues/518))
* Prevent `AttributeError` in proxying app requests by declaring `down_task` in `TCPProxy` class explicitly. ([#519](https://github.com/lablup/backend.ai-manager/issues/519))
* Reduce possibility of new sessions to get stuck in the PREPARING status by improving synchronization of session/kernel creation trackers ([#522](https://github.com/lablup/backend.ai-manager/issues/522))
* Migrate aiodataloader to aiodataloader-ng, which is managed by us ([#525](https://github.com/lablup/backend.ai-manager/issues/525))
* Prevent redis pool depletion in proxying streaming app requests by executing the entire connection tracker script in a non-bursty manner. ([#526](https://github.com/lablup/backend.ai-manager/issues/526))
* Apply "read-only" attribute to a broader range of database transactions to improve overall performance ([#529](https://github.com/lablup/backend.ai-manager/issues/529))
* Allow admins to take actions on behalf of "inactivated" keypairs, such as terminating an RUNNING compute session created by the inactive keypair. ([#530](https://github.com/lablup/backend.ai-manager/issues/530))
* Force resource usage recalculation when session creation is failed to prevent failed session's resource slot not returning. ([#531](https://github.com/lablup/backend.ai-manager/issues/531))
* Upgrade Callosum to resolve installation error on Ubuntu-20.04/aarch64 ([#534](https://github.com/lablup/backend.ai-manager/issues/534))
* Fix `get_wsproxy_version()` returning 404 when target scaling group is allowed to individual keypair/user group rather than user's associated domain ([#538](https://github.com/lablup/backend.ai-manager/issues/538))
* Prevent potential blocking of mutating database transactions when vfolder clone operations take too long time, by making clone operation async (background task) with transactions ([#539](https://github.com/lablup/backend.ai-manager/issues/539))
* Fix a wrong registry name in the sample etcd config ([#540](https://github.com/lablup/backend.ai-manager/issues/540))
* Disallow empty values in `allowed_session_types` of scheduler options and fix the predicate check to scan all scaling groups ([#542](https://github.com/lablup/backend.ai-manager/issues/542))
* Handle multi-architecture image manifest properly. ([#543](https://github.com/lablup/backend.ai-manager/issues/543))
* Correctly skip the legacy kernel images in the Docker Hub registry, prefixed with `kernel-`, while updating the image metadata. ([#545](https://github.com/lablup/backend.ai-manager/issues/545))
* Fix a bug that retried transactions even for non-serialization failures, causing excessive database overheads ([#547](https://github.com/lablup/backend.ai-manager/issues/547))
* Fix alembic migration script to correctly create or drop a new index, `ix_keypairs_resource_policy`, during DB upgrade or downgrade, repectively. ([#549](https://github.com/lablup/backend.ai-manager/issues/549))

### Miscellaneous
* Change the default values of max concurrent sessions (30 -> 5) and idle timeout (600 -> 3600) in keypair resource policy fixture to conform to the preferable defaults. ([#512](https://github.com/lablup/backend.ai-manager/issues/512))
* Update CI workflows to install the matching version of PR and release branches of `backend.ai-cli` ([#520](https://github.com/lablup/backend.ai-manager/issues/520))
* Upgrade aiotools to 1.5 series for improvements of `TaskGroup` and `PersistentTaskGroup` ([#546](https://github.com/lablup/backend.ai-manager/issues/546))


## Older changelogs

* [21.09](https://github.com/lablup/backend.ai-manager/blob/21.09/CHANGELOG.md)
* [21.03](https://github.com/lablup/backend.ai-manager/blob/21.03/CHANGELOG.md)
* [20.09](https://github.com/lablup/backend.ai-manager/blob/20.09/CHANGELOG.md)
* [20.03](https://github.com/lablup/backend.ai-manager/blob/20.03/CHANGELOG.md)
