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
