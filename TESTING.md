# Sorna Server Integration Testing Guide

## Asynchrony of Gateway and Agents

### Case 0: Agent Starting

1. **sorna-gateway** is running.
2. **sorna-agent** starts.

Checkpoints:

* **sorna-gateway** receives `instance_started` event.
* **sorna-gateway** receives `instance_heartbeat` events afterwards.

### Case 1: Agent Restarting

1. **sorna-gateway** and **sorna-agent** are both running. We have a few running kernels.
2. **sorna-agent** restarts. (do a graceful shutdown using Ctrl+C and re-launch.)

Checkpoints:

* **sorna-agent** properly cleans up all running kernels and generates `kernel_terminated` events for each of them.
* **sorna-gateway**
  * receives `kernel_terminated` event(s)
  * receives `instance_terminated` event
  * receives `instance_started` event

### Case 2: Gateway Restarting

1. **sorna-gateway** and **sorna-agent** are both running. We have a few running kernels.
2. **sorna-gateway** restarts.

Checkpoints:

* First of all, **sorna-gateway** must be alive all the time, as much as possible.
* When **sorna-gateway** terminates, it should not touch both Redis and DB.
  * TODO: store `kernel_owners` in Redis (`kernel_id` to `access_key` mapping; maybe field as rdb 2?)
* After restarted,
  * It should have a "grace" period to receive heartbeats from all running agents.
    The length of the grace period should be longer than 2x heartbeat interval.
    During this period, it should reject all kernel-related API calls with HTTP 503.
  * **sorna-gateway** receives `instance_heartbeat` event(s) which may come together at once.
    From heartbeats from the same agent, it chooses the *latest* one and updates the agent status in Redis and DB.
  * For the chosen heartbeat, **sorna-gateway** should calculate and apply the difference between Redis/DB and heartbeat payloads (e.g., increment/decrement `concurrency_usage`), because agents may have timed-out kernels or newly created ones while the gateway was down.
  * After heartbeat processing during the grace period finishes, **sorna-gateway** enters "normal" mode and accepts kernel-related API calls.

### Case 3: Temporary Network Failures

1. **sorna-gateway** and **sorna-agent** are both running. We have a few running kernels.
2. **sorna-agent** is suspended. (Ctrl+Z) => for a longer time than heartbeat timeouts
3. **sorna-agent** is resumed. (`fg`)

Checkpoints:

* **sorna-gateway** generates `instance_terminated` local-dispatch event with "agent-lost" reason.
  * It should delete running kernel information and restore `concurrency_usage` for affected access keys in the DB.
* After agent resumption, **sorna-gateway** receives `instance_heartbeat` event(s) *without* `instance_started` event. Multiple duplicate events may arrive together because they might be queued up instantly after agent resumption.
  * On the first heartbeat, **sorna-gateway** sends `reset_instance` rpc-call to **sorna-agent**.
    * **sorna-agent** should destroy all kernels *without* sending back `kernel_terminated` event(s).
      (Resending them can make `concurrency_usage` negative!)
    * **sorna-gateway** should *ignore* running kernel information from subsequent `instance_heartbeat` events until `reset_instance` call returns.
      It should mark this particular agent "pending" as well until then (i.e. should not create new kernels here).
* As the result, this agent should be like a fresh new one with no kernels running.