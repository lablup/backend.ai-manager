Changes
=======

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

