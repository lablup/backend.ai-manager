# Backend.Ai Server Integration Testing Guide

## Asynchrony of Gateway and Agents

### Case 0: Agent Starting

1. **backend.ai-gateway** is running.
2. **backend.ai-agent** starts.

Checkpoints:

* **backend.ai-gateway** receives `instance_started` event.
* **backend.ai-gateway** receives `instance_heartbeat` events afterwards.

### Case 1: Agent Restarting

1. **backend.ai-gateway** and **backend.ai-agent** are both running. We have a few running kernels.
2. **backend.ai-agent** restarts. (do a graceful shutdown using Ctrl+C and re-launch.)

Checkpoints:

* **backend.ai-agent** properly cleans up all running kernels and generates `kernel_terminated` events for each of them.
* **backend.ai-gateway**
  * receives `kernel_terminated` event(s)
  * receives `instance_terminated` event
  * receives `instance_started` event

### Case 2: Gateway Restarting

1. **backend.ai-gateway** and **backend.ai-agent** are both running. We have a few running kernels.
2. **backend.ai-gateway** restarts.

Checkpoints:

* First of all, **backend.ai-gateway** must be alive all the time, as much as possible.
* When **backend.ai-gateway** terminates, it should not touch both Redis and DB.
  * TODO: store `kernel_owners` in Redis (`kernel_id` to `access_key` mapping; maybe field as rdb 2?)
* After restarted,
  * It should have a "grace" period to receive heartbeats from all running agents.
    The length of the grace period should be longer than 2x heartbeat interval.
    During this period, it should reject all kernel-related API calls with HTTP 503.
  * **backend.ai-gateway** receives `instance_heartbeat` event(s) which may come together at once.
    From heartbeats from the same agent, it chooses the *latest* one and updates the agent status in Redis and DB.
  * For the chosen heartbeat, **backend.ai-gateway** should calculate and apply the difference between Redis/DB and heartbeat payloads (e.g., increment/decrement `concurrency_usage`), because agents may have timed-out kernels or newly created ones while the gateway was down.
  * After heartbeat processing during the grace period finishes, **backend.ai-gateway** enters "normal" mode and accepts kernel-related API calls.

### Case 3: Temporary Network Failures

1. **backend.ai-gateway** and **backend.ai-agent** are both running. We have a few running kernels.
2. **backend.ai-agent** is suspended. (Ctrl+Z) => for a longer time than heartbeat timeouts
3. **backend.ai-agent** is resumed. (`fg`)

Checkpoints:

* **backend.ai-gateway** generates `instance_terminated` local-dispatch event with "agent-lost" reason.
  * It should delete running kernel information and restore `concurrency_usage` for affected access keys in the DB.
* After agent resumption, **backend.ai-gateway** receives `instance_heartbeat` event(s) *without* `instance_started` event. Multiple duplicate events may arrive together because they might be queued up instantly after agent resumption.
  * On the first heartbeat, **backend.ai-gateway** sends `reset_instance` rpc-call to **backend.ai-agent**.
    * **backend.ai-agent** should destroy all kernels *without* sending back `kernel_terminated` event(s).
      (Resending them can make `concurrency_usage` negative!)
    * **backend.ai-gateway** should *ignore* running kernel information from subsequent `instance_heartbeat` events until `reset_instance` call returns.
      It should mark this particular agent "pending" as well until then (i.e. should not create new kernels here).
* As the result, this agent should be like a fresh new one with no kernels running.