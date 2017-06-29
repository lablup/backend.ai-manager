Changes
=======

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
   shadow in InstanceRegistry.enumerate_instances()

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
   It can be initialized using `python -m sorna.gateway.models` command.

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

