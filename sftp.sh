PYTHON=./bin/python3
CMD_PREFIX="$PYTHON -m ai.backend.manager.cli etcd put images/index.docker.io"
$CMD_PREFIX/lablup%2Fsftp 1
$CMD_PREFIX/lablup%2Fsftp/3.6-ubuntu18.04 sha256:dfdfba6fbfd493d9c8ca616febde883a526577c39044562a79554ba1ee1cb690
$CMD_PREFIX/lablup%2Fsftp/3.6-ubuntu18.04/labels/ai.backend.base-distro ubuntu16.04
$CMD_PREFIX/lablup%2Fsftp/3.6-ubuntu18.04/labels/ai.backend.envs.corecount OPENBLAS_NUM_THREADS,OMP_NUM_THREADS,NPROC
$CMD_PREFIX/lablup%2Fsftp/3.6-ubuntu18.04/labels/ai.backend.features "batch query uid-match user-input"
# $CMD_PREFIX/lablup%2Fsftp/3.6-ubuntu18.04/labels/ai.backend.features "batch query uid-match user-input operation"
$CMD_PREFIX/lablup%2Fsftp/3.6-ubuntu18.04/labels/ai.backend.kernelspec 1
$CMD_PREFIX/lablup%2Fsftp/3.6-ubuntu18.04/labels/ai.backend.resource.min.cpu 1
$CMD_PREFIX/lablup%2Fsftp/3.6-ubuntu18.04/labels/ai.backend.resource.min.mem 256m
$CMD_PREFIX/lablup%2Fsftp/3.6-ubuntu18.04/labels/ai.backend.runtime-path /usr/bin/python3
$CMD_PREFIX/lablup%2Fsftp/3.6-ubuntu18.04/labels/ai.backend.runtime-type python
$CMD_PREFIX/lablup%2Fsftp/3.6-ubuntu18.04/labels/ai.backend.service-ports sftp:http:9081
$CMD_PREFIX/lablup%2Fsftp/3.6-ubuntu18.04/resource/cpu/min 1
$CMD_PREFIX/lablup%2Fsftp/3.6-ubuntu18.04/resource/mem/min 256m
$CMD_PREFIX/lablup%2Fsftp/3.6-ubuntu18.04/size_bytes 1120050215
