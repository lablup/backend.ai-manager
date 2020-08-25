Add support for storage proxies
- Storage proxies have a multi-backend architecture, so now storage-specific optimizations such as per-directory quota, performance measurements and fast metadata scanning all becomes available to Backend.AI users.
- Offload and unify vfolder upload/download operations to storage proxies via the HTTP ranged queries and the tus.io protocol.
- Support multiple storage proxies configured via etcd, and each storage proxy may provide multiple volumes in the mount points shared with agents.
- Now the manager instances don't have mount points for the storage volumes, and mount/fstab management APIs skip the manager-side queries and manipulations.
