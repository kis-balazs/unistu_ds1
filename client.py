#!/bin/python3
import logging
import threading
import socket

import discovery

SERVER_PORT = 5000
ACK_MSG = "ack"

class Client(threading.Thread):
    def __init__(self, address):
        threading.Thread.__init__(self)
        self._primary = address
        self._sock = None

    def run(self):
        self._sock = self._createSocket((self._primary, SERVER_PORT))
        try:
            message = input("Enter message: ")
            self._sendMessage(message)
        finally:
            self._sock.close()

    def _createSocket(self, address):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        sock.connect(address)
        return sock

    def _sendMessage(self, message):
        for i in range(3):
            self._sock.sendall(message.encode("UTF-8"))
            if self._waitAck(): break
        else:
            logging.error('Failed to send')

    def _waitAck(self):
        try:
            data = self._sock.recv(1024).decode("UTF-8")
            return data == ACK_MSG
        except socket.timeout:
            return False


# Run main
logging.basicConfig(format='[%(asctime)s] [%(levelname)-05s] %(message)s', level=logging.DEBUG)

primary = discovery.find_primary()
if primary is not None:
    client = Client(primary)
    client.start()
    client.join()
else:
    logging.error("No primary found")