#!/bin/python3
import logging
import select
import signal
import socket
import threading
import queue

import discovery
from scripts.middleware import Middleware

SERVER_PORT = 5001


class Server(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self._discoveryThread = None
        self._logger = logging.getLogger("server")

    def run(self):
        self._startDiscoveryThread()
        self._startClientListenerThread()

        self._clientListenerThread.join()
        self._discoveryThread.join()

    def shutdown(self):
        self._logger.info("shutting down...")
        self._discoveryThread.terminate()
        self._clientListenerThread.shutdown()
        Middleware.get().shutdown()

    def _startDiscoveryThread(self):
        self._discoveryThread = discovery.DiscoveryServerThread()
        self._discoveryThread.start()

    def _startClientListenerThread(self):
        self._clientListenerThread = ClientListener()
        self._clientListenerThread.start()


class ClientListener(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self._stopRequest = False
        self._sock = None
        self._logger = logging.getLogger("client_listener")

    def run(self):
        self._createSocket()

        try:
            while not self._stopRequest:
                ready = select.select([self._sock], [], [], 0.5)
                if ready[0]:
                    self._acceptConnection()
        finally:
            self._sock.close()

    def shutdown(self):
        self._logger.info("stop accepting new connections")
        self._stopRequest = True

    def _createSocket(self):
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.bind(('', SERVER_PORT))
        self._sock.listen(5)

        self._logger.debug("socket is listening on port " + str(SERVER_PORT))

    def _acceptConnection(self):
        client_sock, client_address = self._sock.accept()
        self._logger.debug(f"connection accepted from {client_address[0]}:{str(client_address[1])}")

        client_thread = ClientConnection(client_sock, client_address)
        client_thread.start()


class ClientConnection(threading.Thread):
    def __init__(self, sock, address):
        threading.Thread.__init__(self)
        self._stopRequest = False
        self._sock = sock
        self._address = address
        self._client = None
        self._logger = logging.getLogger("client_conn<{}>".format(self._address[0]))
        self._outQueue = queue.Queue(maxsize=1024)

    def run(self):
        self._sock.setblocking(0)
        try:
            self._clientJoin()
            self._eventLoop()
        except Exception as e:
            self._logger.error("Some error: " + str(e))
        finally:
            self._sock.close()
            self._logger.debug("Connection closed")

    def shutdown(self):
        self._stopRequest = True

    def _clientJoin(self):
        self._client = Middleware.get().joinClient(self)

    def _eventLoop(self):
        outputs = []
        while not self._stopRequest:
            readable, writable, exceptional = select.select([self._sock], outputs, [], 0.5)
            if self._sock in readable:
                data = self._sock.recv(1024)
                if data:
                    self._client.receive(data)

            if not self._outQueue.empty():
                outputs = [self._sock]
            else:
                outputs = []

            if self._sock in writable:
                try:
                    data = self._outQueue.get_nowait()
                except queue.Empty:
                    outputs = []
                else:
                    self._sock.sendall(data)

            # Check if socket is still open
            if self._isSocketClosed():
                self._logger.debug("connection reset by peer")
                Middleware.get().clientDisconnected(self._client)
                break

    def _isSocketClosed(self):
        try:
            data = self._sock.recv(16, socket.MSG_PEEK)
            if len(data) == 0:
                return True
        except BlockingIOError:
            return False
        except ConnectionResetError:
            return True
        return False

    def send(self, data):
        self._outQueue.put(data)


# Run main
logging.basicConfig(format='[%(asctime)s] %(levelname)s (%(name)s) %(message)s', level=logging.DEBUG)

server = Server()

signal.signal(signal.SIGINT, lambda s, f: server.shutdown())
signal.signal(signal.SIGTERM, lambda s, f: server.shutdown())

server.start()
server.join()
