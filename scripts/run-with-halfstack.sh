#! /bin/sh

export BACKEND_DB_ADDR=localhost:8100
export BACKEND_REDIS_ADDR=localhost:8110
export BACKEND_ETCD_ADDR=localhost:8120
export BACKEND_DB_USER=postgres
export BACKEND_DB_PASSWORD=develove
export BACKEND_DB_NAME=backend

exec $@
