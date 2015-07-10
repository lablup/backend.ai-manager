import unittest
from subprocess import call
from multiprocessing import Process
import socket
import json
import asyncio

import zmq
from sorna.proto.manager_pb2 import ManagerRequest, ManagerResponse
from sorna.proto.manager_pb2 import PING, PONG, CREATE, DESTROY, SUCCESS, INVALID_INPUT, FAILURE

class SornaManagerLocalResponseTest(unittest.TestCase):
    def setUp(self):
        self.kernel_ip = '127.0.0.1'
        self.kernel_id = 1
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
            self.server.terminate()
            call(['kill', str(self.server.pid+1)])

    def test_ping_response_with_same_body_as_request(self):
        # Send test HEARTBEAT request
        request = ManagerRequest()
        request.action = PING
        request.body = 'test'
        self.socket.send(request.SerializeToString())

        # Receive response
        response = ManagerResponse()
        response_data = self.socket.recv()
        response.ParseFromString(response_data)

        # Assert PONG and its body is equal to that of request
        self.assertEqual(response.reply, PONG)
        self.assertEqual(request.body, response.body)

    def test_create_and_destroy_agent(self):
        # Send test CREATE request
        request = ManagerRequest()
        request.action = CREATE
        request.body = 'test'
        self.socket.send(request.SerializeToString())

        # Receive response
        response = ManagerResponse()
        response_data = self.socket.recv()
        response.ParseFromString(response_data)
        sock = json.loads(response.body)

        # Assert the response is SUCCESS
        self.assertEqual(response.reply, SUCCESS)
        # self.assertEqual(request.body, response.body)

        # Send DESTROY request
        request.action = DESTROY
        request.kernel_id = response.kernel_id
        self.socket.send(request.SerializeToString())

        # Receive response
        response = ManagerResponse()
        response_data = self.socket.recv()
        response.ParseFromString(response_data)

        # Assert the response is SUCCESS
        self.assertEqual(response.reply, SUCCESS)
        self.assertIn(response.body, 'No such kernel')

if __name__ == '__main__':
    unittest.main()