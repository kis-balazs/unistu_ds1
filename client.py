#!/bin/python3
import logging
import select
import socket
import threading
import uuid

from scripts.message import Message
from scripts.vectorclock import VectorClock

SERVER_PORT = 5001
ACK_MSG = "ack"


class Client(threading.Thread):
    def __init__(self, address, nickname):
        threading.Thread.__init__(self)
        self._primary = address
        self._sock = None
        self._nickname = nickname
        self.onreceive = None
        self.vc = VectorClock()
        self.uuid = None
        self._logger = logging.getLogger("client_thread")
        self._stopRequest = False

    def run(self):
        self._sock = self._createSocket((self._primary, SERVER_PORT))
        try:
            while not self._stopRequest:
                ready = select.select([self._sock], [], [], 0.5)
                if ready[0]:
                    data = self._sock.recv(1024)
                    if data:
                        self._handleMessage(data)
        finally:
            self._sock.close()

    def _createSocket(self, address):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(address)
        return sock

    def _handleMessage(self, data):
        msg = Message.decode(data)
        if msg.type == "join_cluster":
            self.vc = VectorClock(copyDict=msg.vc)
            self.uuid = uuid.UUID(msg.body.uuid)
        elif msg.type == "send_text":
            # consolidate server's view with local view on vector clocks
            new_vc = {}
            for k, v in msg.vc.items():
                if k == str(self.uuid):  # if it's the current client in the vector clock, compare event number
                    assert self.vc.vcDictionary[str(self.uuid)] == msg.vc[str(self.uuid)]
                new_vc[k] = v
            self.vc = VectorClock(copyDict=new_vc)

            self._logger.debug("receiving text: '{}'".format(msg.body))
            if self.onreceive is not None:
                self.onreceive(msg.body)
        elif msg.type == "server_close":
            self._logger.info("Server closed")
            if self.onreceive is not None:
                self.onreceive('$> server disconnected...')

        assert self.vc.vcDictionary == msg.vc
        from pprint import pprint
        print('####')
        pprint(self.vc.vcDictionary)
        print('####')

    def sendMessage(self, message):
        self._logger.debug("sending text: '{}'".format(message))
        self.vc.increaseClock(self.uuid)
        self._sock.sendall(Message.encode(self.vc, 'send_text', True, message))

    def shutdown(self):
        self._stopRequest = True
