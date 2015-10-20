#! /usr/bin/env python3

from collections import OrderedDict
from datetime import datetime
import logging
import urllib.request, urllib.error
import zmq

_logging_ctx = zmq.Context()


class LogstashHandler(logging.Handler):

    def __init__(self, endpoint):
        super().__init__()
        self.endpoint = endpoint
        self._cached_priv_ip = None
        self._cached_inst_id = None
        self._ctx = _logging_ctx
        self._sock = self._ctx.socket(zmq.PUB)
        self._sock.connect(self.endpoint)

    def emit(self, record):
        now = datetime.now().isoformat()
        tags = set()
        extra_data = dict()

        if record.exc_info:
            tags.add('has_exception')
            if self.formatter:
                extra_data['exception'] = self.formatter.formatException(record.exc_info)
            else:
                extra_data['exception'] = logging._defaultFormatter.formatException(record.exc_info)

        # This log format follows logstash's event format.
        log = OrderedDict([
            ('@timestamp', now),
            ('@version', 1),
            ('host', self._get_my_private_ip()),
            ('instance', self._get_my_instance_id()),
            ('logger', record.name),
            ('loc', '{0}:{1}:{2}'.format(record.pathname, record.funcName, record.lineno)),
            ('message', record.getMessage()),
            ('level', record.levelname),
            ('tags', list(tags)),
        ])
        log.update(extra_data)
        self._sock.send_json(log)

    def _get_my_private_ip(self):
        # See the full list of EC2 instance metadata at
        # http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-instance-metadata.html
        if self._cached_priv_ip is None:
            try:
                resp = urllib.request.urlopen('http://169.254.169.254/latest/meta-data/local-ipv4', timeout=0.2)
                self._cached_priv_ip = resp.read().decode('ascii')
            except urllib.error.URLError:
                self._cached_priv_ip = 'unavailable'
        return self._cached_priv_ip

    def _get_my_instance_id(self):
        if self._cached_inst_id is None:
            try:
                resp = urllib.request.urlopen('http://169.254.169.254/latest/meta-data/instance-id', timeout=0.2)
                self._cached_inst_id = resp.read().decode('ascii')
            except urllib.error.URLError:
                self._cached_inst_id = 'i-00000000'
        return self._cached_inst_id
