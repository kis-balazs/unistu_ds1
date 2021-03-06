#!/bin/python3
import logging
from re import S
import select
import socket
import threading
import uuid

from scripts.message import Message
from scripts.vectorclock import VectorClock

SERVER_PORT = 5001
ACK_MSG = "ack"


class Client(threading.Thread):
    def __init__(self, address, nickname, vc=None):
        threading.Thread.__init__(self)
        self._primary = address
        self._sock = None
        self._nickname = nickname
        self.on_receive = None
        self.on_close = None
        self.on_history = None
        self.vc = vc if vc is not None else VectorClock()
        self.uuid = None
        self._logger = logging.getLogger("client_thread")
        self._stopRequest = False
        
        self.healthcheckTimer = None
        self.healthcheckStop = False
        self.pingTimeoutTimer = None

    def run(self):
        self._sock = self._createSocket(self._primary)
        try:
            while not self._stopRequest:
                ready = select.select([self._sock], [], [], 0.5)
                if ready[0]:
                    data = self._sock.recv(8 * 1024)
                    if data:
                        self._handleMessage(data)
        finally:
            self._sock.close()

    def _createSocket(self, address):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(address)
        return sock

    def _startHealthchecks(self, clear_flag=True):
        if clear_flag:
            self.healthcheckStop = False

        if self.healthcheckStop or self.healthcheckTimer:
            return

        self.healthcheckTimer = threading.Timer(2, self._sendPing)
        self.healthcheckTimer.start()

    def _sendPing(self):
        # Do ping
        self.vc.increaseClock(self.uuid)
        msg = Message.encode(self.vc, 'ping', True, None)
        self._sock.sendall(msg)

        self.pingTimeoutTimer = threading.Timer(1, self._pingTimeout)
        self.pingTimeoutTimer.start()

        self.healthcheckTimer = None
        self._startHealthchecks(clear_flag=False)

    def _pingTimeout(self):
        # Server is down
        if self.on_receive is not None:
            self.on_close()

    def _stopHealthchecks(self):
        if self.healthcheckTimer:
            self.healthcheckStop = True
            self.healthcheckTimer.cancel()
        self.healthcheckTimer = None

    def _handleMessage(self, data):
        msg = Message.decode(data)
        if msg.type == "join_cluster":
            self.vc = VectorClock(copyDict=msg.vc)
            self.uuid = uuid.UUID(msg.body.uuid)
            self._startHealthchecks()
        elif msg.type in ["send_text", "history"]:
            # consolidate server's view with local view on vector clocks
            new_vc = {}
            for k, v in msg.vc.items():
                if k == str(self.uuid):  # if it's the current client in the vector clock, compare event number
                    assert self.vc.vcDictionary[str(self.uuid)] == msg.vc[str(self.uuid)]
                new_vc[k] = v
            self.vc = VectorClock(copyDict=new_vc)
        
            assert self.vc.vcDictionary == msg.vc
            if msg.type == 'send_text':
                self._logger.debug("receiving text: '{}'".format(msg.body))
                if self.on_receive is not None:
                    self.on_receive(msg.body)
            elif msg.type == 'history':
                self._logger.debug("receiving history of conversation from server!")
                if self.on_history is not None:
                    self.on_history(msg.body)
        elif msg.type == "server_close":
            self._logger.info("Server closed")
            if self.pingTimeoutTimer:
                self.pingTimeoutTimer.cancel()
                self.pingTimeoutTimer = None
            if self.on_receive is not None:
                self.on_close(msg.body)
        elif msg.type == "pong":
            # Cancel timeout
            if self.pingTimeoutTimer:
                self.pingTimeoutTimer.cancel()
                self.pingTimeoutTimer = None

    def sendMessage(self, message):
        self._logger.debug("sending text: '{}'".format(message))
        self.vc.increaseClock(self.uuid)
        self._sock.sendall(Message.encode(self.vc, 'send_text', True, message))

    def shutdown(self):
        self.on_history = None
        self.on_close = None
        self.on_receive = None
        self._stopRequest = True
        self._stopHealthchecks()
        if self.pingTimeoutTimer:
            self.pingTimeoutTimer.cancel() 
