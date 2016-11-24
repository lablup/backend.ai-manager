Changes
=======

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

