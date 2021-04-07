Rewrite the session scheduler to avoid HoL blocking
 - Skip over sessions in the queue if they fail to satisfy predicates for multiple retries -> 1st case of HoL blocking: a rogue pending session blocks everything in the same scaling group
 - You may configure the maximum number of retries in the `config/plugins/scheduler/fifo/num_retries_to_skip` etcd key.
 - Split the scheduler into two async loops for scheduling decision and session spawning by inserting "SCHEDULED" status between "PENDING" and "PREPARING" statuses -> 2nd case of HoL blocking: failure isolation with each task
