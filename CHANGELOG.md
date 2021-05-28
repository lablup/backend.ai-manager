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

21.09.0.dev0 (2021-05-28)
-------------------------


### Features
* Rewrite the session scheduler to avoid HoL blocking
   - Skip over sessions in the queue if they fail to satisfy predicates for multiple retries -> 1st case of HoL blocking: a rogue pending session blocks everything in the same scaling group
   - You may configure the maximum number of retries in the `config/plugins/scheduler/fifo/num_retries_to_skip` etcd key.
   - Split the scheduler into two async loops for scheduling decision and session spawning by inserting "SCHEDULED" status between "PENDING" and "PREPARING" statuses -> 2nd case of HoL blocking: failure isolation with each task ([#415](https://github.com/lablup/backend.ai-manager/issues/415))
* Add an API endpoint to share/unshare a group virtual folder directly to specific users. This is to allow specified users (usually teachers with user account) can upload data/materials to a virtual folder while it is shared as read-only for other group users. ([#419](https://github.com/lablup/backend.ai-manager/issues/419))
* Add `PRE_AUTH_MIDDLEWARE` hook for cookie-based SSO plugins ([#420](https://github.com/lablup/backend.ai-manager/issues/420))
* Add update_full_name API to rename user's full_name regardless of role. ([#424](https://github.com/lablup/backend.ai-manager/issues/424))
* Modify the loading process so that the scheduler can be loaded reflecting scheduler_opts. ([#428](https://github.com/lablup/backend.ai-manager/issues/428))

### Fixes
* Fix a missing reference fix for renaming of `gateway` to `manager.api` ([#409](https://github.com/lablup/backend.ai-manager/issues/409))
* Refactor the manager CLI initialization steps and promote `generate-keypair` as a regular `mgr` subcommand ([#411](https://github.com/lablup/backend.ai-manager/issues/411))
* Fix an internal API mismatch for our SQLAlchemy custom enum types ([#412](https://github.com/lablup/backend.ai-manager/issues/412))
* Fix a regression in session cancellation and kernel status updates after SQLAlchemy v1.4 upgrade ([#413](https://github.com/lablup/backend.ai-manager/issues/413))
* Fix a regression of spawning multi-node cluster sessions due to DB API changes related to setting transaction isolation levels ([#416](https://github.com/lablup/backend.ai-manager/issues/416))
* Adjust the firing rate of `DoPrepareEvent` to follow and alternate with the scheduler execution ([#418](https://github.com/lablup/backend.ai-manager/issues/418))
* Change the `KeyPair.num_queries` GQL field to use Redis instead of the `keypairs.num_queries` DB column to avoid excessive DB writes ([#421](https://github.com/lablup/backend.ai-manager/issues/421))
* Improve stability and synchronization of container-databse states
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
    break the transaction isolation guarantees. ([#425](https://github.com/lablup/backend.ai-manager/issues/425))
* Further refine the stability update with improved database transaction retries and the latest SQLAlchemy 1.4.x updates within the last month ([#429](https://github.com/lablup/backend.ai-manager/issues/429))
* Fix a regression that destroying a cluster session generates duplicate session termination events ([#430](https://github.com/lablup/backend.ai-manager/issues/430))
* Optimize read-only GraphQL queries to use read-only transaction isolation level, which greatly reduces the database loads when using GUI ([#431](https://github.com/lablup/backend.ai-manager/issues/431))
* Feature to implement utilization based idle session checker ([#432](https://github.com/lablup/backend.ai-manager/issues/432))

### Miscellaneous
* Fix the examples for the storage proxy URL configurations in the `manager.config` module ([#410](https://github.com/lablup/backend.ai-manager/issues/410))
* Update sample configurations for etcd ([#414](https://github.com/lablup/backend.ai-manager/issues/414))
* Temporarily pin `pytest-asyncio` to 0.14.0 due to regression of handling event loops for fixtures ([#423](https://github.com/lablup/backend.ai-manager/issues/423))


21.09.0.dev0 (2021-05-28)
-------------------------


### Features
* Rewrite the session scheduler to avoid HoL blocking
   - Skip over sessions in the queue if they fail to satisfy predicates for multiple retries -> 1st case of HoL blocking: a rogue pending session blocks everything in the same scaling group
   - You may configure the maximum number of retries in the `config/plugins/scheduler/fifo/num_retries_to_skip` etcd key.
   - Split the scheduler into two async loops for scheduling decision and session spawning by inserting "SCHEDULED" status between "PENDING" and "PREPARING" statuses -> 2nd case of HoL blocking: failure isolation with each task ([#415](https://github.com/lablup/backend.ai-manager/issues/415))
* Add an API endpoint to share/unshare a group virtual folder directly to specific users. This is to allow specified users (usually teachers with user account) can upload data/materials to a virtual folder while it is shared as read-only for other group users. ([#419](https://github.com/lablup/backend.ai-manager/issues/419))
* Add `PRE_AUTH_MIDDLEWARE` hook for cookie-based SSO plugins ([#420](https://github.com/lablup/backend.ai-manager/issues/420))
* Add update_full_name API to rename user's full_name regardless of role. ([#424](https://github.com/lablup/backend.ai-manager/issues/424))
* Modify the loading process so that the scheduler can be loaded reflecting scheduler_opts. ([#428](https://github.com/lablup/backend.ai-manager/issues/428))

### Fixes
* Fix a missing reference fix for renaming of `gateway` to `manager.api` ([#409](https://github.com/lablup/backend.ai-manager/issues/409))
* Refactor the manager CLI initialization steps and promote `generate-keypair` as a regular `mgr` subcommand ([#411](https://github.com/lablup/backend.ai-manager/issues/411))
* Fix an internal API mismatch for our SQLAlchemy custom enum types ([#412](https://github.com/lablup/backend.ai-manager/issues/412))
* Fix a regression in session cancellation and kernel status updates after SQLAlchemy v1.4 upgrade ([#413](https://github.com/lablup/backend.ai-manager/issues/413))
* Fix a regression of spawning multi-node cluster sessions due to DB API changes related to setting transaction isolation levels ([#416](https://github.com/lablup/backend.ai-manager/issues/416))
* Adjust the firing rate of `DoPrepareEvent` to follow and alternate with the scheduler execution ([#418](https://github.com/lablup/backend.ai-manager/issues/418))
* Change the `KeyPair.num_queries` GQL field to use Redis instead of the `keypairs.num_queries` DB column to avoid excessive DB writes ([#421](https://github.com/lablup/backend.ai-manager/issues/421))
* Improve stability and synchronization of container-databse states
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
    break the transaction isolation guarantees. ([#425](https://github.com/lablup/backend.ai-manager/issues/425))
* Further refine the stability update with improved database transaction retries and the latest SQLAlchemy 1.4.x updates within the last month ([#429](https://github.com/lablup/backend.ai-manager/issues/429))
* Fix a regression that destroying a cluster session generates duplicate session termination events ([#430](https://github.com/lablup/backend.ai-manager/issues/430))
* Optimize read-only GraphQL queries to use read-only transaction isolation level, which greatly reduces the database loads when using GUI ([#431](https://github.com/lablup/backend.ai-manager/issues/431))
* Feature to implement utilization based idle session checker ([#432](https://github.com/lablup/backend.ai-manager/issues/432))

### Miscellaneous
* Fix the examples for the storage proxy URL configurations in the `manager.config` module ([#410](https://github.com/lablup/backend.ai-manager/issues/410))
* Update sample configurations for etcd ([#414](https://github.com/lablup/backend.ai-manager/issues/414))
* Temporarily pin `pytest-asyncio` to 0.14.0 due to regression of handling event loops for fixtures ([#423](https://github.com/lablup/backend.ai-manager/issues/423))


Please refer the changelog of the older versions:
* [v21.03](https://github.com/lablup/backend.ai-manager/blob/21.03/CHANGELOG.md)
* [v20.09](https://github.com/lablup/backend.ai-manager/blob/20.09/CHANGELOG.md)
