#!/bin/python3
import threading
import logging
import socket
import signal
import multiprocessing

import discovery
from scripts.message import Message
from scripts.middleware import Middleware

SERVER_PORT = 5001

class Server(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self._discoveryThread = None

    def run(self):
        self._startDiscoveryThread()
        self._startClientListenerThread()

        self._clientListenerThread.join()
        self._discoveryThread.join()

    def terminate(self):
        print("terminate server")
        self._discoveryThread.terminate()
        self._clientListenerThread.terminate()

    def _startDiscoveryThread(self):
        self._discoveryThread = discovery.DiscoveryServerThread()
        self._discoveryThread.start()

    def _startClientListenerThread(self):
        self._clientListenerThread = ClientListener()
        self._clientListenerThread.start()

class ClientListener(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self._stopEvent = multiprocessing.Event()
        self._sock = None
        self._clientThreads = []

    def run(self):
        self._createSocket()

        try:
            while not self._stopEvent.is_set():
                self._acceptConnection()
        finally:
            self._stopClientThreads()
            self._sock.close()

    def terminate(self):
        print("terminate listener")
        self._stopEvent.set()
        
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(('', SERVER_PORT))
        sock.close()
            
    def _createSocket(self):
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.bind(('', SERVER_PORT))
        self._sock.listen(5)

        logging.debug("server: socket is listening on port " + str(SERVER_PORT))

    def _acceptConnection(self):
        client_sock, client_address = self._sock.accept()
        logging.debug(f"server: connection accepted from {client_address[0]}:{str(client_address[1])}")

        client_thread = ClientConnection(client_sock, client_address)
        client_thread.start()
        
        self._clientThreads.append(client_thread)

    def _stopClientThreads(self):
        for t in self._clientThreads: t.terminate()
        for t in self._clientThreads: t.join()

class ClientConnection(threading.Thread):
    def __init__(self, sock, address):
        threading.Thread.__init__(self)
        self._sock = sock
        self._address = address
        self._client = None

    def run(self):
        try:
            self._clientJoin()
            self._eventLoop()
        except Exception as e:
            logging.warning("Some error: " + str(e))
            self._sock.close()

    def terminate(self):
        print("terminate connection")
        self._sock.shutdown(socket.SHUT_RDWR)
        self._sock.close()

    def _clientJoin(self):
        self._client = Middleware.get().joinClient(self)

    def _eventLoop(self):
        while True:
            data = self._sock.recv(1024)
            if len(data) > 0 and self._client is not None:
                self._client.receive(data)

    def send(self, data):
        self._sock.sendall(data)

# Run main
logging.basicConfig(format='[%(asctime)s] [%(levelname)-05s] %(message)s', level=logging.DEBUG)

server = Server()

signal.signal(signal.SIGINT, lambda s, f: server.terminate())
signal.signal(signal.SIGTERM, lambda s, f: server.terminate())

server.start()
server.join()
