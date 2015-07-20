import unittest
from subprocess import call
from multiprocessing import Process
import socket
import json
import asyncio

import signal, psutil
import zmq
# from sorna.proto.manager_pb2 import ManagerRequest, ManagerResponse
# from sorna.proto.manager_pb2 import PING, PONG, CREATE, DESTROY, SUCCESS, INVALID_INPUT, FAILURE
from sorna.proto import Namespace, encode, decode
from sorna.proto.msgtypes import ManagerRequestTypes, ManagerResponseTypes, AgentRequestTypes


class SornaManagerLocalResponseTest(unittest.TestCase):
    def setUp(self):
        self.kernel_ip = '127.0.0.1'
        self.kernel_driver = 'local'
        self.manager_port = 5001
        self.manager_addr = 'tcp://{0}:{1}'.format(self.kernel_ip, self.manager_port)

        # Establish a manager server in a separate process
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.port_error = s.connect_ex((self.kernel_ip, self.manager_port))
        s.close()
        if self.port_error != 0:  # When the port is available
            cmd = ['python3', '-m', 'sorna.manager', '--kernel-driver', self.kernel_driver]
            self.server = Process(target=call, args=(cmd,))
            self.server.start()

        # Connect to the manager server
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REQ)
        self.socket.connect(self.manager_addr)

    def tearDown(self):
        if self.port_error != 0:  # Kill test server
            child_pid = psutil.Process(self.server.pid).children(recursive=True)
            for pid in child_pid:  # Kill all child processes. More graceful way?
                pid.send_signal(signal.SIGTERM)
            self.server.terminate()
            print('###' + str(self.server.pid) + ', ' + str(child_pid))

    def test_ping_response_with_same_body_as_request(self):
        # Send test HEARTBEAT request
        request = Namespace()
        request.action = ManagerRequestTypes.PING
        request.body = 'test'
        self.socket.send(encode(request))

        # Receive response
        response_data = self.socket.recv()
        response = decode(response_data)

        # Assert PONG and its body is equal to that of request
        self.assertEqual(response.reply, ManagerResponseTypes.PONG)
        self.assertEqual(request.body, response.body)

    def test_create_and_destroy_agent(self):
        # Send test CREATE request
        request = Namespace()
        request.action = ManagerRequestTypes.CREATE
        request.body = 'test'
        self.socket.send(encode(request))

        # Receive response
        response_data = self.socket.recv()
        response = decode(response_data)

        # Assert the response is SUCCESS
        self.assertEqual(response.reply, ManagerResponseTypes.SUCCESS)

        # Send DESTROY request
        request.action = ManagerRequestTypes.DESTROY
        request.kernel_id = response.kernel_id
        self.socket.send(encode(request))

        # Receive response
        response_data = self.socket.recv()
        response = decode(response_data)

        # Assert the response is SUCCESS
        self.assertEqual(response.reply, ManagerResponseTypes.SUCCESS)
        print(response)

if __name__ == '__main__':
    unittest.main()