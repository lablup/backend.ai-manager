Changes
=======

19.12.0b1 (2020-01-xx)
----------------------

* IMPROVE: Now our manager-to-agent RPC uses `Callosum <https://github.com/lablup/callosum>_` instead of
  aiozmq, supporting Python 3.8 natively. (#209, #79)

* Internally refactored the main function for easier writing of future unit tests by composing different
  resource cleanup contexts in a modular way.

19.12.0a2 (2019-12-31)
----------------------

* NEW: User-manageable session templates written in YAML to reuse session creation parameters. (#213)

* NEW: User-customizable per-scaling-group session queue scheduler plugins and three intrinsic plugins:
  FIFO, LIFO, and the DRF (dominant resource fairness) scheduler. (#212)

19.12.0a1 (2019-12-26)
----------------------

* MAINTENANCE: Now it runs on Python 3.8 or higher.

* IMPROVE: ResourceSlots are now more permissive so that agents with different sets of
  accelerator plugins can now coexist in a single cluster. (#214)

* NEW: more convenient etcd commands: ``quote``, ``unquote``, ``move-subtree``

* IMPROVE: "--short" and "--installed" options added to ``etcd list-images`` command.

19.09.13 (2020-02-10)
---------------------

* NEW: Include the agent IDs where each image is installed when fetching the image list (#222)

* NEW: API for managing user-specific dotfiles, which are automatically populated for all new sessions.
  (#220)

* FIX: Missing error logging for InternalServerError raised by service-port streaming API handler

* FIX: Make the tus.io upload handler to use asynchronous file I/O (#228, lablup/backend.ai#106)

* MAINTENANCE: Improve internal CI/CD pipelines (#225)

19.09.12 (2020-01-17)
---------------------

* FIX: Potential exhaustion of DB connection pool due to long file I/O operations in the vfolder API
  (#219)

19.09.11 (2020-01-16)
---------------------

* FIX: Potential blocking due to file I/O contention in the vfolder API. (#219)

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

* NEW: Keypair-specific SSH keypair to access intrinsic SSH service ports (#211)

  - By default they are random-generated and can only be regenerated.
  - Manual configuration will be supported in the future.
  - Only used if ".ssh" vfolder is not mounted.

* FIX: Corruption of the runtime path when importing an image due to inadvertent resolving
  of the path in the host-side.

* IMPROVE: Include statistics for running kernels (in addition to terminated ones)
  in the usage API.

19.09.7 (2019-12-03)
--------------------

* NEW: Add an API for self-updating the user password (#208)

* FIX: Missing status filtering for ``ComputeSession.batch_load_detail()`` GQL resolver.

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

* FIX: Deprecate ``ManagerStatus.PREPARING`` and ``ManagerStatus.TERMINATED`` with relevant updates for
  HA setup

* FIX: Wrap more Redis operations in GQL resolvers with retries

19.09.0rc4 (2019-10-04)
-----------------------

This is the last preview, feature-freeze release for v19.09 series.
Stability updates will follow in the v19.09.0 and possibly a few more v19.09.x releases.

* NEW: Image import (#171) - currently this is limited to import Python-based kernels only.
  This is implemented on top of batch tasks, with some specialization to prevent security issues
  due to direct access to agent host's Docker daemon.  Importing as service-port only image support will
  be added in future releases.

* NEW: Batch tasks (#199) - the kernel creation API now has an extra "type" parameter (default:
  "interactive") which can be set "batch" with another new parameter "startupCommand".  The batch-type
  sessions (batch tasks) run the startup command immdediately after starting and automatically terminates
  after finishing it, with a success/failure result recorded in the database retrievable as the "result"
  column.

  The execution logs are stored in the ".logs" user vfolder if present, which will be mounted all
  user-owned compute sessions automatically.  This log can be retrieved at any time using the new
  ``kernel/_/logs`` API (task-logs API).

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
  group/domain resource limits. (#184)

* UPDATE: Compute session GQL queries now include the ``resource_opts`` field.

* Minor bug fixes.

19.09.0b13 (2019-09-09)
-----------------------

* NEW: Add option to specify the amount of shared memory via ``resource_opts`` parameter
  in the kernelc reation config API (lablup/backend.ai#52)

* UPDATE: Enhance vfolder download APIs to support ranged HTTP requests for partial downloads and
  fix the browser-side fetch() API content decoding error due to the default behavior of
  aiohttp.web.FileResponse implementation.

* Alembic migrations are now distributed as a part of the source and wheel package.
  Set ``script_location = ai.backend.manager.models:alembic`` in your alembic.ini configuration file.

* Various bug fixes for GQL APIs and statistics.

* Update dependencies including aiohttp 3.6, wheel, twine, setuptools, and typing-extensions.

19.09.0b12 (2019-09-03)
-----------------------

* Various bug fixes for GQL scoped permission handling

* NEW: bugx fixes and mount option support for vfolder mount API (#183)

19.09.0b11 (2019-08-30)
-----------------------

* NEW: superadmin APIs for mount/unmount vfolder hosts (#183)

* FIX: resource usage API validation error when it is used with URL query strings

19.09.0b10 (2019-08-27)
-----------------------

* FIX: plain users could see other users' sessions due to a missing
  access-key filtering condition in the GQL loader implementation
  for ``compute_sessions`` query.

* FIX: an unexpected error at creating a new user when there is no default group.
  Changed to add the user to the default group only when it exists.

* Add ``mem_allocated`` field to group usage statistics

* Various bug fixes for config/get and config/set APIs

19.09.0b9 (2019-08-21)
----------------------

* Minor fix in logging of singup/singout request handlers

19.09.0b8 (2019-08-19)
----------------------

* FIX: Mitigate race condition when checking keypair/group/domain resource limits (#180)

  - KNOWN ISSUE: The current fix only covers a single-process deployment of the manager.

* NEW: Introduce "is_installed" filtering condition to the "images" GraphQL query.

* NEW: Watcher APIs to control agents remotely (#179)

* Pin the pyzmq version 18.1.0 (lablup/backend.ai#47)

* NEW: Support for Harbor registry (#177)

19.09.0b7 (2019-08-14)
----------------------

* Update resource stat API to provide extra unit hints. (#176)

19.09.0b6 (2019-08-14)
----------------------

* NEW: Add option to change underlying event loop implementation.

* Updated signup/login hook support.

* CHANGE: In the response of kernel creation API, service port information only expose
  the name and protocol pairs, since port numbers are useless in the client-side.

19.09.0b5 (2019-08-05)
----------------------

* NEW: Scaling groups to partition agents into differently scheduled groups (#73, #167)

* NEW: Image lists are now filtered by docker registries allowed for each domain. (#170)

* NEW: "/auth/role" API to get the current user's role/privilege information

* CHANGE: GraphQL queries are now unified for all levels of users!

  - The allow/deny decision is made per each query and mutation.

* FIX: ``refresh_session()`` was not called to keep service port connections.

19.06.0b4 (2019-07-24)
----------------------

* CHANGE: vfolder (storage) names may have a single dot prefix (e.g., ".local").

* FIX: inversion of docker-registry.ssl-verify option

* Updated kernel's get_info REST API to work with latest compute session models. (#160)

* Extend support for group/shared vfolders and invitation-related APIs. (#149, #166)

19.06.0b3 (2019-07-17)
----------------------

* CHANGE: Accept typeless resource slots for resource policy configurations
  (lablup/backend.ai-common#7)

* FIX: Register public interface only when the app exists

19.06.0b2 (2019-07-15)
----------------------

* Add the user signup endpoint and related plugins support

19.03.4 (2019-08-14)
--------------------

- Fix refresh_session() callback not invoked properly due to type mismatch of the function returned
  by functools.partial against a coroutine function.

- Fix admin_required() permission check decorator.

19.03.3 (2019-07-17)
--------------------

- CHANGE/BACKPORT: Accept typeless resource slots for resource policy configurations
  (lablup/backend.ai-common#7)

19.06.0b1 (2019-07-14)
----------------------

* The API version is now "v4.20190615" (latest prior was "v4.20190315").

* NEW: Add an API for manually recalculating resource usage for keypair and agents (#161)

* NEW: Add an API for token-based streaming download from vfolders (#159)

* NEW: Add "config/get", "config/set", "config/delete" APIs for administrators to manipulate etcd
  configurations.

* NEW: Add resource statistics API for admins (#154, #156, #157)

* NEW: vfolder now has two types: per-user and per-group (#148, #152)

* BREAKING CHANGE: configurations are now read from TOML files (#155)

  - Redis address is no longer configured in the manager-side config.
    It must be set as "config/redis/addr" (and "config/redis/password" optionally) in the etcd directly.

* BREAKING CHANGE: "etcd/resource-slots" -> "config/resource-slots"

* Now etcd user/password authentication works with automatic auth-token refreshes and reconnections.

* Alembic migrations are updated to have self-contained table definitions so that they are not affetced
  by the current version of manager models.

19.06.0a1 (2019-06-03)
----------------------

* Add support for extended live/on-termination collection of updated resource metrics.
  (#151, lablup/backend.ai-agent#109)

* Add domain and group models to partition resource usage by different customer and user sets.
  Also add "superadmin" level for administrators who have the access/manipulation privilege across all
  domains.  (#148)

  - Without explicit creation of domains and groups, all users and kernels belong to the "default" domain
    and the "default" group.  This applies to the DB migration as well.

  - Currently, the user IDs and keypairs are 1:1 mapped.

  - Users are no longer able to see the agent information and only domain admins and superadmins can do.

  - Add a new API: "/auth/authorize" to allow implementation of token-based 3rd-party authorization.
    Currently the returned token is just the API keypair associated with the user, but later we plan to
    support JWT as well.

  - Explicit group association is required when launching new kernels.

19.03.2 (2019-07-12)
--------------------

- NEW: Add a new API for downloading large files from vfolders via streaming based on JWT-based
  authentication. (#159)

- NEW: Add a new API for recalculating keypair/agent resource usage when there are database
  synchronization errors. (#161)

- CHANGE: Allow users to provide their own custom access key and secret key when creating or
  modifying their keypairs (for human-readable keys)

19.03.1 (2019-04-21)
--------------------

- Fix various non-critical warnings and exceptions that occurs when users
  disconnect abruptly (e.g., closing browsers connected to container service ports)

- Ensure that the event subscriber coroutine keep continuing when it receives
  corrupted messages and fails to parse them. (#146)
  This has caused intermittent but permanent agent-lost timeouts in public network
  environments.

19.03.0 (2019-04-10)
--------------------

- NEW: resource preset API which provides a way to check resource availability
  of specific resource configurations

- NEW: vfolder/_/hosts API to retrieve vfolder hosts accessible by the user

- CHANGE: The root API also returns the manager version as well as API version.

- Fix empty alias list when querying images.

- Fix GQL/DB-related bugs and improve migration experience.

- Fix consistency corruption of keypairs.concurrency_used field.

19.03.0rc2 (2019-03-25)
-----------------------

- NEW: Add an explicit "owner_access_key" query parameter to all session-related APIs
  (under /kernel/ prefix) so that admininstrators can perform API requests such as
  termination on sessions owned by other users.

- NEW: Add a new API for renaming vfolders (#82)

- CHANGE: Now idle timeouts are configured by keypair resource policies. (#92)

- CHANGE: Rename "--redis-auth" option to "--redis-password" and its
  environment variable equivalent as well.

- Now non-admin users are able to query their own keypairs and resource policies via
  the GraphQL API.

- Improve stability with many concurrent clients and lossy connections by shielding
  DB-access coroutines to prevent DB connection pool corruption. (#140)

- Increase the default rate-limit for keypairs from 1,000 to 30,000 for better GUI
  integration.

- Reduce chances for timeout errors when there are bursty session creation requests.

- Other bug fixes and improvements.

19.03.0rc1 (2019-02-25)
-----------------------

- NEW: It now supports authentication with etcd and Redis for better security.

  - NOTE: etcd authentication is unusable yet in productions due to a missing
    implementation of auto-refreshing auth tokens in the upstream etcd3 binding
    library.

- Implement GQL mutations for KeyPairResourcePolicy.

- Fix vfolder listing queries in all places to consider invited vfolders and owned
  vfolders correctly.

- Add missing "compute_session_list" GQL field to the user-mode GQL schema.

- Minor bug fixes and improvements.

19.03.0b9 (2019-02-15)
----------------------

- NEW: Add pagination support to the GraphQL API (#132)

- CHANGE: Unspecified (or zero'ed) per-image resource limits are now treated as
  *unlimited*.

- Implement RW/RO permissions when mounting shared vfolders (#82)

- Fix various bugs including CLI commands for image aliases, the session restart
  API, skipping SSL certificate verification in CLI commands, fixture population with
  enum values and already-inserted rows, and session termination hang-up in specific
  environments where locally bound sockets are not accessible via the node's
  network-local IP address.

19.03.0b8 (2019-02-08)
----------------------

- NEW: resource policy for keypairs (#134)

  - Now admins can limit the maximum number of concurrent session, virtual folders,
    and the total resource slots used by each access key.

  - IMPORTANT: DB migration is required (if you upgrade from prior beta versions).

    Before migrating, you *MUST BACKUP* the existing keypairs table if you want to
    preserve the "concurrency_limit" column, as it will be reset to 30 using a
    "default" keypair resource policy.  Also, the default policy allows unlimited
    resource slots to preserve the previous behavior while it limits the number of
    vfolders to 10 per access key and enables only the "local" vfolder host.  You
    need to adjust those settings using the dbshell (SQL)!

  - NOTE: Fancy GraphQL mutation APIs for the resource policies (and their CLI/GUI
    counterparts) will come in the next version.

  - NOTE: Currently the vfolder size limit is not enforced since it is not
    implemented yet.

- Support big integers (up to 53 bits or 8192 TiB) when serializing various
  statistics fields in the GraphQL API. (#133)

- Add "--skip-sslcert-validation" CLI option and "BACKEND_SKIP_SSLCERT_VALIDATION"
  environment variable for setups using privately-signed SSL certificates

19.03.0b7 (2019-02-03)
----------------------

- Fix various issues related to resource slot type *changes*.

  - Ignore unknown slots except when the user explicitly requests one.

  - Always reset resource slot types when processing heartbeats.

    IMPORTANT: You must install the same set of accelerator plugins across all your
    agent nodes so that they report the same set of resource slot types even when
    some agents does not have support for specific accelerator plugins.  Also,
    plugins are required to return "disabled" plugin instance which specified the
    resource slot types but returns no available devices.

- Add a small API to get currently known resource slots from clients:
  "<ENDPOINT>/etcd/resource-slots"

- Now "occupied_slots" field and "available_slots" field in the Admin GraphQL APIs
  returns a consistent set of keys from the known resource slot types.

19.03.0b6 (2019-01-31)
----------------------

- Various small-but-required bug fixes

  - When signing API requests, it now uses ``raw_path`` instead of ``rel_url``
    to preserve the URL-encoded query string intact.

  - Large kernel iamges scanned from registries caused a graphene error due to
    out-of-range 32-bit signed integers in the "size_bytes" field.  Adopted a custom
    BigInt scalar to coerce big integers to Javascript floats since modern JS engines
    mostly support up to 52-bit floating point numbers.

    *NOTE:* The next ECMAScript standard will support explicit big numbers with the
    "n" suffix, which is experimentally implemented in the V8 engine last year.
    (https://developers.google.com/web/updates/2018/05/bigint)

  - An aiohttp API compatibility issue in the vfolder download handler.

  - Fix the missing "installed" field value in GraphQL's "images" query.

  - Fix a missing check for "is_active" status of keypairs during API request
    authentication.

19.03.0b5 (2019-01-31)
----------------------

- Fix various migration issues related to JSON fields and SQL.

19.03.0b4 (2019-01-30)
----------------------

- Add "installed" field to GraphQL image/images query results so that
  the client could know whether if an image has any agent that locally has it.

- Remove aiojobs.atomic decorators from gateway.kernel API handlers to prevent
  blocking due to long agent-side operations such as image pulling.

- Fix a regression in the query/batch mode code execution due to old codes
  in the websocket handlers.

19.03.0b3 (2019-01-30)
----------------------

- Add missing support for legacy GraphQL "image" / "images" queries.

- Add "--min" switch to "set-image-resource-limit" manager CLI command.

- Fix missing metrics in some cases.

- Fix a logical error preventing session creation when min/max are same.

19.03.0b2 (2019-01-30)
----------------------

- Support legacy GraphQL clients by interpolating new JSON-based resource fields.

- Fix interpretation of private docker image references without explicit repository
  subpaths. Previously it was assume to be under "lablup/" always.

19.03.0b1 (2019-01-30)
----------------------

- BIG: Support for dynamic resource slots and full private Docker registries. (#127)
  Now all resource-related fields in APIs/DB are JSON.

- Support running multiple managers on the same host by randomizing internal IPC
  socket addresses.  This also improves the security a little.

- Support bodyless (query params intead) GET requests for vfolder/kernel file
  download APIs.

19.03.0a2 (2019-01-21)
----------------------

- Bump API version from v4.20181215 to v4.20190115 to allow clients to distinguish
  streaming execution API support.

- Fix the backend.ai-common dependency version follow the 19.03 series.

19.03.0a1 (2019-01-18)
----------------------

- Add support for NVIDIA GPU Cloud images.

- Internally changed a resource slot name from "gpu" to "cuda".
  Still the API and database uses the old name for backward-compatibility.

18.12.0 (2019-01-06)
--------------------

- Version numbers now follow year.month releases like Docker.
  We plan to release stable versions on every 3 months (e.g., 18.12, 19.03, ...).

- NEW: Support TPU (Tensor Processing Units) in Google Clouds.

- Clean up log messages for devops & IT admins.

- Add PyTorch v1.0 image metadata.

18.12.0a4 (2018-12-26)
----------------------

- manager.cli.etcd: Improve interoperability with installer scripts.

18.12.0a3 (2018-12-21)
----------------------

- Technical release to fix the backend.ai-common dependency version.

18.12.0a2 (2018-12-21)
----------------------

- NEW: Add an admin GraphQL scheme to fetch the currently registered list of
  kernel images.

- CHANGE: Change fixtures from a Python module to static JSON files.
  Now the example keypair fixture reside in the sample-configs directory.

  - ``python -m ai.backend.manager.cli fixture populate`` is changed to accept
    a path to the fixture JSON file.

  - ``python -m ai.backend.manager.cli fixture list`` is now deprecated.

- CHANGE: The process monitoring tools will now show prettified process names for
  Backend.AI's daemon processes which exhibit the role and key configurations (e.g.,
  namespace) at a glance.

- Improve support for using custom/private Docker registries.

18.12.0a1 (2018-12-14)
----------------------

- NEW: App service ports!  You can start a compute session and directly connect to a
  service running inside it, such as Jupyter Notebook! (#121)

- Extended CORS support for web browser clients.

- Monitoring tools are separated as plugins.

1.4.7 (2018-11-24)
------------------

- Technical release to fix an internal merge error.

1.4.6 (2018-11-24)
------------------

- Fix various bugs.

  - Fix kernel restart regression bug.
  - Fix code execution with API v4 requests.
  - Fix auth test URLs.
  - Fix Server response headers in subapps.

1.4.5 (2018-11-22)
------------------

- backport: Accept API v4 requests (lablup/backend.ai#30)
  In API v4, the authentication signature always uses an emtpy string
  as the request body element to allow easier implementation of streaming
  and proxies.

- Fix handling of empty/unspecified execute API options (#116)

- Fix storing of fractional resources reported by agents

- Update image metadata/aliases for TensorFlow 1.12 and PyTorch

1.4.4 (2018-11-09)
------------------

- Update the default image metadata/aliases to include latest deep learning kernels.

1.4.3 (2018-11-06)
------------------

- Fix creation of GPU sessions with GPU resource limits unspecified in the
  client-side.  The problem was due to a combination of misconfiguration
  (image-metadata.yml) and mishandling of "None" values with valid dictionary keys.

- Update coding style rules and the flake8 package.

1.4.2 (2018-11-01)
------------------

- Fix a critical regression bug of tracking available memory (RAM) of agents due to
  changes to relative resource shares from absolute resource amounts.

- Backport a temporary patch to limit the maximum number of kernel execution records
  returned by the admin GraphQL API (until we have a proper pagination support).

- Update the list of our public kernel images as we add support for latest TensorFlow
  versions including v1.10 and v1.11 series.  More to come!

1.4.1 (2018-10-17)
------------------

- Support CORS (cross-origin resource sharing) for browser-based API clients (#99).

- Fix the agent revival detection routine to update agent's address and region
  for movable demo devices (#100).

- Update use of deprecate APIs in our dependencies such as aiohttp and aiodocker.

- Let the config server to refresh configuration values from etcd once a minute.

1.4.0 (2018-09-30)
------------------

- Expanded virtual folder APIs

  - Downloading and uploading large files from virtual folders via streaming (#70)
  - Inviting other users and accepting such invitations with three-level permissions
    (read-only, read-write, read-write-delete) for collaboration via virtual folders
    (#80)
  - Now it requires explicit "recursive" option to remove directories (#89)
  - New "mkdir" API to create empty directories (#89)

- Support listing files in the session's main container. (#63)

- All API endpoints are now available *without* version prefixes, as we migrate
  to the vanilla aiohttp v3.4 release. (#78)

- Change `user_id` column type of `keypairs` model from integer to string.
  Now it can be used to store the user emails, UUIDs, or whatever identifiers
  depending on the operator's environment.

  Clients must be upgrade to 1.3.7 or higher to use string `user_id` properly.
  (The client will auto-detect the type by trying type casting.)

1.3.12 (2018-10-17)
-------------------

- Add CORS support (Hotfix #99 backported from v1.4 and master)

1.3.11 (2018-06-07)
-------------------

- Drop custom-patched aiohttp and update it to official v3.3 release. (#78)

- Fix intermittent failures in streaming uploads of small files.

- Fix an internal "infinity integer" representation to have correct 64-bit maximum
  unsgined value.

1.3.10 (2018-05-01)
-------------------

- Fix a regression bug when restarting kernels.

1.3.9 (2018-04-12)
------------------

- Limit the default number of worker processes to avoid unnecessarily many workers in
  many-core systems and database connection exhaustion errors (lablup/backend.ai#17)

- Upgrade aiotools to v0.6.0 release.

- Ensure aiohttp's shutdown handlers to have access to databases during their
  execution, by moving connection pool cleanups to the aiohttp's cleanup handler.

1.3.8 (2018-04-06)
------------------

- Fix bugs in resolving image tags and aliases (#71)

1.3.7 (2018-04-04)
------------------

- Improve database initialization during setup by auto-detecting existing or fresh
  new databases in the CLI's "schema oneshot" command. (#69)

1.3.6 (2018-04-04)
------------------

- Further SQL transaction fixes

- Change the access key string of the non-admin example keypair

1.3.5 (2018-03-23)
------------------

- Further improve synchronization when destroying and restarting kernels.

- Change the agent load balancer to favor CPUs first to spread kernels evenly.
  (In the future versions, this will be made configurable and customizable.)

1.3.4 (2018-03-23)
------------------

- Improve synchronization when executing codes right after creating kernels by
  ensuring all DB operations (incl. read-only ops) to be inside (nested)
  transactions.

1.3.3 (2018-03-20)
------------------

- Improve vfolder APIs to handle sub-directories correctly when uploading and use
  the configured mount directory ("volumes/_mount" key in our etcd namespace).

1.3.2 (2018-03-15)
------------------

- Technical release to fix backend.ai-common depedency version.

1.3.1 (2018-03-14)
------------------

- Allow separate upgrade of the manager from v1.2 to v1.3 by extrapolating a new
  "kernel_host" field in the return value of the internal krenel creation RPC call.

1.3.0 (2018-03-08)
------------------

- Now the Backend.AI gateway uses a modular architecture where you can add 3rd-party
  extensions as aiohttp.web.Application and middlewares via ``BACKEND_EXTENSIONS``
  environment variable. (#65)

- Adopt aiojobs as the main coroutine task scheduler. (#65)
  Using this, improve handler/task cancellation as well.

- Public non-authorized APIs become accessible without "Date" HTTP header set. (#65)

- Upgrade aiohttp to v3.0 release. (#64)

- Improve dockerization support. (#62)

- Fix "X-Method-Override" support that was interfering with RFC-7807-style error
  reporting.  Also return correct HTTP status code when failed route resolution.

1.2.2 (2018-02-14)
------------------

- Add metadata/aliases for TensorFlow v1.5 kernel images to the default sample configs.

- Polish CI and test suites.

- Add etcd put/get/del manager CLI commands to get rid of the necessity of an extra
  etcdcli binary during installation. (lablup/backend.ai#15)

1.2.1 (2018-01-30)
------------------

- Minor update to fix dependency versions.

1.2.0 (2018-01-30)
------------------

**NOTICE**

- From this release, the manager and agent versions will go together, which indicates
  the compatibility of them, even when either one has relatively little improvements.

**CHANGES**

- The gateway server now consider per-agent image availability when scheduling a new
  kernel. (#29)

- The execute API now returns exitCode value of underlying subprocesses in the batch
  mode. (#60)

- The gateway server is now fully horizontally-scalable.
  There is no states shared via multiprocessing shared memory and all such states are
  now managed by a separate Redis instance.

- Improve logging: it now provides multiprocess-safe file-based rotating logs. (#10)

- Fix the Admin API error when filtering agents by their status due to a missing
  method parameter in ``Agent.batch_load()``.

1.1.0 (2018-01-06)
------------------

**NOTICE**

- Requires alembic database migration for upgrading.

**API CHANGES**

- The semantic for client session token changes. (#56, #58)
  Clients may reuse the same session token across different sessions if only a single
  session is running at a time.
  The manager now returns an explicit error if the client request is going to violate
  this constraint.

- In the API responses, Rate-Limit-Reset is gone away and now we have
  Rate-Limit-Window value instead. (#55)

  Since we use a rolling counter, there is no explicit reset point but you are now
  guaranteed to send at most N requests for the last 15 minutes (where N is the
  per-user rate limit) at ANY moment.

- When continuing or sending user-inputs via the execute API, you
  must set the mode field to "continue" or "input" respectively.

- You no longer have to specify a random run ID on the first request of a run during
  session; if the field is set to null, the server will assign a new run ID
  automatically.  Note that you STILL have to specify the run ID on subsequent
  requests for the run. (#59)

  All API responses now include its corresponding run ID regardless of whether it is
  given by the client or assigned by the server, which eases client-side
  demultiplexing of concurrent executions.

**OTHER IMPROVEMENTS**

- Fix atomicity of rate-limiting calculation in multi-core setups. (#55)

- Remove simplejson from dependencies in favor of the standard library.
  The stdlib has been updated to support all required features and use
  an internal C-based module for performance.

1.0.4 (2017-12-19)
------------------

- Minor update for execute API: allow explicit continue/input mode values.

- Mitigate connection failures after a DB failover event. (#35)

1.0.3 (2017-11-29)
------------------

- Add virtual folder!

- Update aioredis to v1.0.0 release.

- Remove "mode" argument when calling agent RPC "get completions" calls.

1.0.2 (2017-11-14)
------------------

- Fix synchronization issues when restarting kernels

- Fix missing database column errors when restarting streaming sessions

- Fix a missing null check when registering new agents or updating existing ones

1.0.1 (2017-11-08)
------------------

- Now we use a new kernel image naming and tagging scheme.
  Check out the comments in the sample image alias configuration
  at the repository root (image-aliases.sample.yml)

- Now the manager fully controls the resource allocation in agents
  when creating a new kernel session.

- Updated aiohttp to v2.3.2

- Various bug fixes and improvements

1.0.0 (2017-10-17)
------------------

- This release is replaced with v1.0.1 due to many bugs.

0.9.11 (2017-09-08)
-------------------

**NOTICE**

- The package name will be changed to "backend.ai-manager" and the import
  paths will become ``ai.backend.manager.something``.

**CHANGES**

- Let it accept "BackendAI" API requests as well for future compatibility.
  (#39)

0.9.10 (2017-07-18)
-------------------

**FIX**

- Fix the wrong version range of an optional depedency package "datadog"

0.9.9 (2017-07-18)
------------------

**IMPROVEMENTS**

- Improve packaging so that setup.py has the source list of dependencies
  whereas requirements.txt has additional/local versions from exotic
  sources.

- Support exception/event logging with Sentry.

0.9.8 (2017-07-07)
------------------

**FIX**

- Revert authorization in terminal pty streaming due to regression.

0.9.7 (2017-06-29)
------------------

**NEW**

- Add support for the batch-mode API with compiled languages such as
  C/C++/Java/Rust.

- Add support for the file upload API for use with the batch-mode API.
  (up to 20 files per request and 1 MiB per each file)

**IMPROVEMENTS**

- Upgrade aiohttp to v2.2.0.

0.9.6 (2017-05-09)
------------------

- Make the list of GPU instances configurable.
  (Later, this will be automatically detected without explicit configurations)

0.9.5 (2017-04-07)
------------------

- Add support for PyTorch kernels.

- Fix continuous API failures when faulty agents wrongly reports their status.

- Upgrade aiohttp to v2.

0.9.4 (2017-03-19)
------------------

- Improve packaging: auto-converted README.md as long description and unified
  requirements.txt and setup.py dependencies.

0.9.3 (2017-03-14)
------------------

- Fix internal API mismatch bug in web termainl.

0.9.2 (2017-03-14)
------------------

- Fix sorna-common requirement version.

0.9.1 (2017-03-14)
------------------

**IMPROVEMENTS**

- Handle v1/v2 API requests separately.
  Now it preserves old "aggregated" stdout/stderr/media outputs for v1
  but uses the new streaming outputs for v2.
  (v1 API users can use streaming as well, but they will loose the
  ordering information of individual lines of the console output.)

0.9.0 (2017-02-27)
------------------

**FIXES**

- Fix task pending error during shutdown due to missing await for redis
  monitoring task after cancelled.

- Fix wrong active instance count in Datadog stats due to missing checks for
  shadow in ``InstanceRegistry.enumerate_instances()``.

0.8.6 (2017-01-19)
------------------

**FIXES**

- Prevent potential CPU-hogging infinite loop during Datadog stats updates.

**IMPROVEMENTS**

- Add statistics reporting via Datadog. (optional feature)

- Improve exception handling and reporting, particularly for agent-sid errors.


0.8.5 (2017-01-14)
------------------

**FIXES**

- It now copes with API requests without bodies at all: use an empty string to
  generate signatures.

- Enabled authorization checks to stream-mode APIs, which has been disabled
  for debugging and tests.
  (Though the probability of exposing kernels to other users was very low
  due to randomly generated kernel IDs.)

0.8.4 (2017-01-11)
------------------

**FIXES**

- Stabilized sporadic restarts/disconnects of agent instances, and keep the
  concurrency usage consistent.

- Increased the minimum size of aioredis connection pools to avoid rare
  deadlocks due to pool exhaustion.

0.8.3 (2017-01-10)
------------------

**FIXES**

- Make sure all errorneous responses to contain RFC 7807-style JSON-formatted
  error messages using aiohttp middleware.

0.8.1 (2017-01-10)
------------------

**FIXES**

- Assume date headers in HTTP request headers without timezone offsets
  as UTC instead of showing internal server error.

0.8.0 (2017-01-10)
------------------

**NEW**

- Deprecated legacy ZMQ interface.  The code is still there, but should
  not be used.

- Refined keypair/usage database schema.

- Implemented the streaming-mode API: web terminal!

- Restarting the kernel in the middle of web termainl session are transparently
  handled -- user's browser-side websocket connections are preserved.

- The codebase now requires Python 3.6.0 or higher.

- Internally it adopted a simple event bus to handle asynchronous docker events
  such as abnormal termination of kernels.  Now most interactions with docker
  are truly asynchronous.

0.7.4 (2016-11-29)
------------------

**FIXES**

- Legacy ZMQ interface: Revived a missing language parameter in legacy
  client-side session token generation.
  This has broken CodeOnWeb's PRACTICE page.

- Gateway: Increased timeouts when interacting with agents.
  In particular, code execution timeouts must be longer than kernel execution
  timeouts.

- Gateway: Added a missing transaction context during authorization.
  This has caused "another operation in progress" errors with concurrent API
  requests within a very short period of time (under a few tens of msec).

0.7.3 (2016-11-28)
------------------

**CHANGES**

- When launching a new kernel and accessing to an existing kernel, it scans
  only "currently alive" instances by checking shadow keys that automatically
  expires.  This makes the Sorna service sustainable with abrupt agent failures.

0.7.2 (2016-11-27)
-----------------

**CHANGES**

- When launching a new kernel, it now chooses the least loaded agent instead of
  the first-found agent with free kernel slots.

0.7.1 (2016-11-25)
------------------

Hot-fix to add missing dependencies in requirements.txt and setup.py

0.7.0 (2016-11-25)
------------------

To avoid confusion with different version numbers in other Sorna sub-projects,
we skip the version 0.6.0 in all sub-projects.

**NEW**

- Implemented most of the REST API except streaming terminals and events.

- Added database schema for user/keypair information management.
  It can be initialized using ``python -m sorna.gateway.models`` command.

**FIXES**

- Fixed duplicate kernel count decrementing when destroying kernels in legacy manager.

0.5.1 (2016-11-15)
------------------

**FIXES**

- Added a missing check for stale kernel sessions due to restarts of Sorna agents.
  This bug has impacted public tutorial/workshops and demonstrations because the
  manager does not recreate kernels at the right timing.

0.5.0 (2016-11-01)
------------------

**NEW**

- First public release.

