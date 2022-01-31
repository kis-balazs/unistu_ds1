import socket
import logging
from threading import Thread
import multiprocessing

DISCOVERY_PORT = 37020

WHOIS_REQ = 'whois_primary_req'
WHOIS_RES = 'whois_primary_res'

discovery_logger = logging.getLogger("discovery")

def find_primary():
    discovery_logger.info('Starting discovery process')
    broadcast_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM) #UDP
    broadcast_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    broadcast_socket.settimeout(5)

    try:
        # Send request
        discovery_logger.debug('Sending request')
        broadcast_socket.sendto(WHOIS_REQ.encode('UTF-8'), ('255.255.255.255', DISCOVERY_PORT))

        # Receive response
        data, address = broadcast_socket.recvfrom(1024)
        if data.decode('UTF-8') == WHOIS_RES:
            discovery_logger.info('Received primary address: ' + str(address))
            return address[0]
    except socket.timeout:
        discovery_logger.debug('Timed out')
    finally:
        broadcast_socket.close()

    return None

class DiscoveryServerThread(Thread):
    def __init__(self):
        Thread.__init__(self)
        self._stopEvent = multiprocessing.Event()
        self._logger = logging.getLogger("discovery_server")

    def run(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM) #UDP
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.settimeout(5)
        sock.bind(('', DISCOVERY_PORT))

        self._logger.info('Start listening for discovery requests...')
        try:
            while not self._stopEvent.is_set():
                try:
                    data, address = sock.recvfrom(1024)
                    if data.decode('UTF-8') == WHOIS_REQ:
                        self._logger.debug('Received request from ' + str(address))
                        sock.sendto(WHOIS_RES.encode('UTF-8'), address)
                except socket.timeout:
                    continue
        finally:
            sock.close()
        
    def terminate(self):
        self._stopEvent.set()

        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.sendto(''.encode('UTF-8'), ('', DISCOVERY_PORT))
        sock.close()
