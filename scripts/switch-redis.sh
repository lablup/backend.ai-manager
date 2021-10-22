#! /bin/bash

case "$1" in
"single")
  echo "Changing Redis config to use a single Redis instance..."
  backend.ai mgr etcd delete --prefix config/redis
  backend.ai mgr etcd put-json config/redis config/sample.etcd.redis-single.json
  ;;
"sentinel")
  echo "Changing Redis config to use a sentinel-based cluster..."
  backend.ai mgr etcd delete --prefix config/redis
  backend.ai mgr etcd put-json config/redis config/sample.etcd.redis-sentinel.json
  ;;
"cluster")
  echo "Changing Redis config to use a Redis cluster..."
  echo "[ERROR] Not implemented yet."
  ;;
*)
  echo "Unknown option. Choose from \"single\", \"sentinel\", and \"cluster\"."
  ;;
esac
