#!/bin/python3
from concurrent.futures import thread
import logging
import queue
import select
import signal
import socket
import threading

import discovery
from scripts.middleware import Middleware

SERVER_PORT = 5001
REPLCIA_PORT = 5002


class Server(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self._discoveryThread = None
        self._logger = logging.getLogger("server")

    def run(self):
        self._startDiscoveryThread()
        self._startClientListenerThread()
        self._startReplicaListenerThread()

        self._replicaListenerThread.join()
        self._clientListenerThread.join()
        self._discoveryThread.join()

    def shutdown(self):
        self._logger.info("shutting down...")
        self._discoveryThread.terminate()
        self._clientListenerThread.shutdown()
        self._replicaListenerThread.shutdown()
        Middleware.get().shutdown()

    def _startDiscoveryThread(self):
        self._discoveryThread = discovery.DiscoveryServerThread()
        self._discoveryThread.start()

    def _startClientListenerThread(self):
        self._clientListenerThread = ConnectionListener("client_listener", SERVER_PORT, ClientConnection)
        self._clientListenerThread.start()

    def _startReplicaListenerThread(self):
        self._replicaListenerThread = ConnectionListener("replica_listener", REPLCIA_PORT, ReplicaConnection)
        self._replicaListenerThread.start()


class ConnectionListener(threading.Thread):
    def __init__(self, name, port, ConnectionClass):
        threading.Thread.__init__(self)
        self._stopRequest = False
        self._sock = None
        self._logger = logging.getLogger(name)
        self._port = port
        self._ConnectionClass = ConnectionClass

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
        self._sock.bind(('', self._port))
        self._sock.listen(5)

        self._logger.debug("socket is listening on port " + str(self._port))

    def _acceptConnection(self):
        client_sock, client_address = self._sock.accept()
        self._logger.debug(f"connection accepted from {client_address[0]}:{str(client_address[1])}")

        client_thread = self._ConnectionClass(client_sock, client_address)
        client_thread.start()


class Connection(threading.Thread):
    def __init__(self, sock, address, name):
        threading.Thread.__init__(self)
        self._stopRequest = False
        self._sock = sock
        self._sock.setblocking(0)
        self._address = address
        self._logger = logging.getLogger(name)
        self._outQueue = queue.Queue(maxsize=1024)

    def run(self):
        try:
            self.onOpen()
            self._eventLoop()
        except Exception as e:
            self._logger.error("Some error: " + str(e))
        finally:
            self._sock.close()
            self._logger.debug("Connection closed")

    def shutdown(self):
        self._stopRequest = True

    def onOpen(self):
        pass

    def onData(self):
        pass

    def onClose(self):
        pass

    def _eventLoop(self):
        outputs = []
        while not self._stopRequest or not self._outQueue.empty():
            readable, writable, exceptional = select.select([self._sock], outputs, [], 0.5)
            if self._sock in readable and not self._stopRequest:
                data = self._sock.recv(1024)
                if data:
                    self.onData(data)

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
                self.onClose()
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


class ClientConnection(Connection):
    def __init__(self, sock, address):
        Connection.__init__(self, sock, address, "client_conn<{}>".format(address[0]))
        self._client = None

    def onOpen(self):
        self._client = Middleware.get().joinClient(self)

    def onData(self, data):
        self._client.receive(data)

    def onClose(self):
        Middleware.get().clientDisconnected(self._client)


class ReplicaConnection(Connection):
    def __init__(self, sock, address):
        Connection.__init__(self, sock, address, "replica_conn<{}>".format(address[0]))
        self._replica = None

    def onOpen(self):
        self._logger.debug("replica onOpen")

    def onData(self):
        self._logger.debug("replica onData")

    def onClose(self):
        self._logger.debug("replica onClose")


if __name__ == '__main__':
    logging.basicConfig(format='[%(asctime)s] %(levelname)s (%(name)s) %(message)s', level=logging.DEBUG)

    server = Server()

    signal.signal(signal.SIGINT, lambda s, f: server.shutdown())
    signal.signal(signal.SIGTERM, lambda s, f: server.shutdown())

    server.start()
    server.join()
