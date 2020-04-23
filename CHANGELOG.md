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

19.09.19 (2020-03-16)
---------------------

* NEW: Add "forced" option to the session destruction API ([#250](https://github.com/lablup/backend.ai-manager/issues/250))

* NEW: Backport "etcd forget-image" manager CLI command from the master and add "forget_image" GraphQL
  mutation for super-admins.

* IMPROVE: Support project/repository/tag pagination in rescan-images for Docker registries with a large
  number of images and tags ([#252](https://github.com/lablup/backend.ai-manager/issues/252))

* FIX: Strip owner-access-key argument when checking API params for input validation
  with query parameters ([#251](https://github.com/lablup/backend.ai-manager/issues/251))

* FIX: Potential SQL syntax error during synchronizing real-time statistics from Redis to DB caused when
  no statistics data is available.

* FIX: Add missing privilege checks for some of GraphQL mutation APIs and improve logging for GraphQL
  queries and mutations using graphene middleware. ([#254](https://github.com/lablup/backend.ai-manager/issues/254))

19.09.18 (2020-03-08)
---------------------

* MAINTENANCE: Update backend.ai-common dependency.

19.09.17 (2020-02-27)
---------------------

* MAINTENANCE: Update dependency packages.

19.09.16 (2020-02-27)
---------------------

* FIX: Fix potential memory leaks in streaming APIs ([#243](https://github.com/lablup/backend.ai-manager/issues/243))

* IMPROVE: Favor CPU agents for session creation requests without accelerators ([#242](https://github.com/lablup/backend.ai-manager/issues/242))

19.09.15 (2020-02-18)
---------------------

* FIX: Add a check for reserved names when creating vfolder and user dotfiles ([#238](https://github.com/lablup/backend.ai-manager/issues/238))

* FIX: Remove a bogus error when livestat is missing while agents are starting up ([#233](https://github.com/lablup/backend.ai-manager/issues/233))

* MAINTENANCE: Update dependencies and add missing asynctest test dependency for Python 3.6/3.7 ([#239](https://github.com/lablup/backend.ai-manager/issues/239))

19.09.14 (2020-02-11)
---------------------

* FIX: Missing agent address updates upon receiving heartbeats while the agents are alive. ([#231](https://github.com/lablup/backend.ai-manager/issues/231))

19.09.13 (2020-02-10)
---------------------

* NEW: Include the agent IDs where each image is installed when fetching the image list ([#222](https://github.com/lablup/backend.ai-manager/issues/222))

* NEW: API for managing user-specific dotfiles, which are automatically populated for all new sessions.
  ([#220](https://github.com/lablup/backend.ai-manager/issues/220))

* FIX: Missing error logging for InternalServerError raised by service-port streaming API handler

* FIX: Make the tus.io upload handler to use asynchronous file I/O ([#228](https://github.com/lablup/backend.ai-manager/issues/228), lablup/backend.ai#106)

* MAINTENANCE: Improve internal CI/CD pipelines ([#225](https://github.com/lablup/backend.ai-manager/issues/225))

19.09.12 (2020-01-17)
---------------------

* FIX: Potential exhaustion of DB connection pool due to long file I/O operations in the vfolder API
  ([#219](https://github.com/lablup/backend.ai-manager/issues/219))

19.09.11 (2020-01-16)
---------------------

* FIX: Potential blocking due to file I/O contention in the vfolder API. ([#219](https://github.com/lablup/backend.ai-manager/issues/219))

* IMPROVE: Update the batch user creation script.

19.09.10 (2020-01-09)
---------------------

* IMPROVE: Include agent IDs in the usage API

19.09.9 (2019-12-18)
--------------------

* IMPROVE: Skip containers and images with a unsupported (future) kernelspec version.
  (lablup/backend.ai#80)

19.09.8 (2019-12-16)
--------------------

* NEW: Keypair-specific SSH keypair to access intrinsic SSH service ports ([#211](https://github.com/lablup/backend.ai-manager/issues/211))

  - By default they are random-generated and can only be regenerated.
  - Manual configuration will be supported in the future.
  - Only used if ".ssh" vfolder is not mounted.

* FIX: Corruption of the runtime path when importing an image due to inadvertent resolving
  of the path in the host-side.

* IMPROVE: Include statistics for running kernels (in addition to terminated ones)
  in the usage API.

19.09.7 (2019-12-03)
--------------------

* NEW: Add an API for self-updating the user password ([#208](https://github.com/lablup/backend.ai-manager/issues/208))

* FIX: Missing status filtering for `ComputeSession.batch_load_detail()` GQL resolver.

* IMPROVE: Add the amount of GPU allocated and GPU memory to the usage statistics API.

* IMPROVE: Explicitly return HTTP 404 when file was not found for the download-single API.

19.09.6 (2019-11-11)
--------------------

* FIX: vfolder were not listed when there are pending invitations for the current user.

* IMPROVE: Allow superadmin to delete any vfolder.

* NEW: Include group-level resource statistics and add an option to exclude them in check-presets API.

* NEW: Add an API to download a single file from /home/work of containers.

19.09.5 (2019-11-04)
--------------------

* IMPROVE: vfolders now have their creators information and relevant GQL queries include them.

* FIX/IMPROE: Improve scaling-group related queries.

* FIX: An error when returning resource presets after creation

* FIX: Ensure "USER root" for auto-generated imported image since some images (e.g., NGC Matlab) use
  custom user IDs.

* MAINTENANCE: Update dependencies and pin trafaret to v1.2.x since its new v2.0 release breaks the
  backward compatibility.

19.09.4 (2019-10-15)
--------------------

* OPTIMIZE: Large file transfers are 20% to 3x faster via vfolder download API and SFTP by increasing
  the network buffer sizes.

* FIX: Wrong content-encoding header in the vfolder download API

19.09.3 (2019-10-14)
--------------------

* FIX: ManagerStatus check error during startup when clean installed.

* FIX: Usage API reports wrong data due to internal reuse of list objects which should have been copied.

* FIX: Passing stringified bool values to boolean API parameters (e.g., "yes", "1", "no", "0", etc.)

19.09.2 (2019-10-11)
--------------------

* FIX: Use the canonical, normalized version number for the backend.ai-common setup dependency to silence
  pip warnings during installation.

19.09.1 (2019-10-10)
--------------------

* FIX: Regression of code execution API due to internal manager-to-agent RPC argument ordering.
  You MUST update the agent to 19.09.1 as well!

19.09.0 (2019-10-07)
--------------------

* FIX: Reconnection and cancellation of manager status watchers. Now the manager survives etcd restarts.

* FIX: Deprecate `ManagerStatus.PREPARING` and `ManagerStatus.TERMINATED` with relevant updates for
  HA setup

* FIX: Wrap more Redis operations in GQL resolvers with retries

19.09.0rc4 (2019-10-04)
-----------------------

This is the last preview, feature-freeze release for v19.09 series.
Stability updates will follow in the v19.09.0 and possibly a few more v19.09.x releases.

* NEW: Image import ([#171](https://github.com/lablup/backend.ai-manager/issues/171)) - currently this is limited to import Python-based kernels only.
  This is implemented on top of batch tasks, with some specialization to prevent security issues
  due to direct access to agent host's Docker daemon.  Importing as service-port only image support will
  be added in future releases.

* NEW: Batch tasks ([#199](https://github.com/lablup/backend.ai-manager/issues/199)) - the kernel creation API now has an extra "type" parameter (default:
  "interactive") which can be set "batch" with another new parameter "startupCommand".  The batch-type
  sessions (batch tasks) run the startup command immdediately after starting and automatically terminates
  after finishing it, with a success/failure result recorded in the database retrievable as the "result"
  column.

  The execution logs are stored in the ".logs" user vfolder if present, which will be mounted all
  user-owned compute sessions automatically.  This log can be retrieved at any time using the new
  `kernel/_/logs` API (task-logs API).

* IMPROVE: Allow admins to create sessions on behalf of other users.

* SECURITY-FIX: Privilege escalation because domain-admins could run sessions on behalf of super-admins
  in the same domain.

* Various bug fixes and improvements.

19.09.0rc3 (2019-09-25)
-----------------------

* FIX: status field parsing in legacy GraphQL queries (non-paginated compute_sessions)

* FIX: scaling group's remaining resource must be capped by the user's keypair resource limits.

* FIX: Sign-up plugin hook check

19.09.0rc2 (2019-09-24)
-----------------------

* FIX: Corruption of kernels.concurrency_used in specific scheduling conditions

* IMPROVE: Terminating PENDING sessions and permanent scheduling failures makes the sessions
  to be CANCELLED.

* NEW: Support specifying multiple status values to compute_sessions and compute_session_list
  GraphQL queries so that clients can display sessions of multiple statuses in a single view.

  - Since GraphQL does not allow union of scalar types, use comma-separated string in the
    status field of those queries. This keeps the backward compatibility.

  - Now the default ordering is "greatest(created_at, terminated_at, status_changed)" in the
    descending order.  "alembic upgrade" is required to create appropriate database indexes.

* FIX: Missing generation of "kernel_cancelled" and "kernel_terminating" events

* FIX: Server hang-up when shutting down with clients to wait for PENDING sessions to start up

* FIX: Missing "reason" field when users terminate sessions

19.09.0rc1 (2019-09-23)
-----------------------

* NEW: Support for high availability (#125, #192) with zero reconfiguration when fail-over
  of the manager.

  - The manager may have multiple nodes now. Adding/removing nodes just work, as long as
    the client configurations for "multi-endpoints" get updated accordingly.

  - There is no central master of the manager fleet. All manager instances are equivalent.

  - Intermittent disruptions over Redis connections (e.g., due to fail-over of Redis master)
    no longer make both manager/agent to hang up or go into undefined states.

* NEW: Job queueing (#192, #180, #189), so that excessive job execution no longer raises
  errors but those requests are "queued".
  The current scheduling is FIFO but more scheduling options will be added in the future.

  - Now the kernels have PENDING and CANCELLED status.  Any permanent errors before RUNNING status
    makes the kernel to transition into the CANCELLED status.

  - Each status change is recorded with explicit timestamp and a human-readable "status_info" which
    can be retrieved by clints via GQL.

* NEW: event monitoring API for session lifecycles so that now clients can get to know
  whether the session is pulling a new docker image or just hanging up (#84, #110)

* Various bug fixes related to role/active checks and updates in user maangement (#193, #194 and many
  one-off commits)

19.09.0b14 (2019-09-17)
-----------------------

* NEW: A superadmin API to list all vfolder hosts and docker registires.

* UPDATE: resource/check-presets API is updated to return per-scaling-group remainings and
  group/domain resource limits. ([#184](https://github.com/lablup/backend.ai-manager/issues/184))

* UPDATE: Compute session GQL queries now include the `resource_opts` field.

* Minor bug fixes.

19.09.0b13 (2019-09-09)
-----------------------

* NEW: Add option to specify the amount of shared memory via `resource_opts` parameter
  in the kernelc reation config API (lablup/backend.ai#52)

* UPDATE: Enhance vfolder download APIs to support ranged HTTP requests for partial downloads and
  fix the browser-side fetch() API content decoding error due to the default behavior of
  aiohttp.web.FileResponse implementation.

* Alembic migrations are now distributed as a part of the source and wheel package.
  Set `script_location = ai.backend.manager.models:alembic` in your alembic.ini configuration file.

* Various bug fixes for GQL APIs and statistics.

* Update dependencies including aiohttp 3.6, wheel, twine, setuptools, and typing-extensions.

19.09.0b12 (2019-09-03)
-----------------------

* Various bug fixes for GQL scoped permission handling

* NEW: bugx fixes and mount option support for vfolder mount API ([#183](https://github.com/lablup/backend.ai-manager/issues/183))

19.09.0b11 (2019-08-30)
-----------------------

* NEW: superadmin APIs for mount/unmount vfolder hosts ([#183](https://github.com/lablup/backend.ai-manager/issues/183))

* FIX: resource usage API validation error when it is used with URL query strings

19.09.0b10 (2019-08-27)
-----------------------

* FIX: plain users could see other users' sessions due to a missing
  access-key filtering condition in the GQL loader implementation
  for `compute_sessions` query.

* FIX: an unexpected error at creating a new user when there is no default group.
  Changed to add the user to the default group only when it exists.

* Add `mem_allocated` field to group usage statistics

* Various bug fixes for config/get and config/set APIs

19.09.0b9 (2019-08-21)
----------------------

* Minor fix in logging of singup/singout request handlers

19.09.0b8 (2019-08-19)
----------------------

* FIX: Mitigate race condition when checking keypair/group/domain resource limits ([#180](https://github.com/lablup/backend.ai-manager/issues/180))

  - KNOWN ISSUE: The current fix only covers a single-process deployment of the manager.

* NEW: Introduce "is_installed" filtering condition to the "images" GraphQL query.

* NEW: Watcher APIs to control agents remotely ([#179](https://github.com/lablup/backend.ai-manager/issues/179))

* Pin the pyzmq version 18.1.0 (lablup/backend.ai#47)

* NEW: Support for Harbor registry ([#177](https://github.com/lablup/backend.ai-manager/issues/177))

19.09.0b7 (2019-08-14)
----------------------

* Update resource stat API to provide extra unit hints. ([#176](https://github.com/lablup/backend.ai-manager/issues/176))

19.09.0b6 (2019-08-14)
----------------------

* NEW: Add option to change underlying event loop implementation.

* Updated signup/login hook support.

* CHANGE: In the response of kernel creation API, service port information only expose
  the name and protocol pairs, since port numbers are useless in the client-side.

19.09.0b5 (2019-08-05)
----------------------

* NEW: Scaling groups to partition agents into differently scheduled groups (#73, #167)

* NEW: Image lists are now filtered by docker registries allowed for each domain. ([#170](https://github.com/lablup/backend.ai-manager/issues/170))

* NEW: "/auth/role" API to get the current user's role/privilege information

* CHANGE: GraphQL queries are now unified for all levels of users!

  - The allow/deny decision is made per each query and mutation.

* FIX: `refresh_session()` was not called to keep service port connections.
