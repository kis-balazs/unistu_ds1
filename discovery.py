import logging
import multiprocessing
import socket
from threading import Thread

from scripts.middleware import PeerInfo

DISCOVERY_PORT = 37020

WHOIS_REQ = 'whois_primary_req'
WHOIS_RES = 'whois_primary_res'
PRIMARY_UP = 'primary_up'

discovery_logger = logging.getLogger("discovery")

def find_primary():
    discovery_logger.info('Starting discovery process')
    broadcast_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)  # UDP
    broadcast_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    broadcast_socket.settimeout(5)

    try:
        # Send request
        discovery_logger.debug('Sending request')
        broadcast_socket.sendto(WHOIS_REQ.encode('UTF-8'), ('255.255.255.255', DISCOVERY_PORT))

        # Receive response
        data, address = broadcast_socket.recvfrom(1024)
        msg = data.decode('UTF-8').split()
        if len(msg) != 5:
            return None
        if msg[0] == WHOIS_RES:
            print('ip', msg[1])
            primary = PeerInfo(
                ip=msg[1], 
                server_port=int(msg[2]), 
                replica_port=int(msg[3]),
                election_port=int(msg[4])
            )
            discovery_logger.info('Received primary address: {}'.format(str(primary)))
            return primary
    except socket.timeout:
        discovery_logger.debug('Timed out')
    finally:
        broadcast_socket.close()

    return None

def send_primary_up(own_uuid):
    discovery_logger.info('Sending primary up message')
    broadcast_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)  # UDP
    broadcast_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)

    try:
        # Send request
        discovery_logger.debug('Sending request')
        broadcast_socket.sendto("{} {}".format(PRIMARY_UP, str(own_uuid)).encode('UTF-8'), ('255.255.255.255', DISCOVERY_PORT))
    finally:
        broadcast_socket.close()

class DiscoveryServerThread(Thread):
    def __init__(self, peer_info, is_primary, on_primary_up):
        Thread.__init__(self)
        self._stopEvent = multiprocessing.Event()
        self._logger = logging.getLogger("discovery_server")
        self._peer_info = peer_info
        self._is_primary = is_primary
        self._on_primary_up = on_primary_up

    def set_is_primary(self, is_primary):
        self._is_primary = is_primary

    def run(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)  # UDP
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.settimeout(5)
        sock.bind(('', DISCOVERY_PORT))

        self._logger.debug("Running as {}".format('primary' if self._is_primary else 'replica'))
        self._logger.info('Start listening for discovery requests...')
        try:
            while not self._stopEvent.is_set():
                try:
                    data, address = sock.recvfrom(1024)
                    msg = data.decode('UTF-8').split()
                    if len(msg) == 0:
                        continue
                    if msg[0] == WHOIS_REQ and self._is_primary:
                        self._logger.debug('Received request from ' + str(address))
                        sock.sendto(
                            "{} {} {} {} {}".format(
                                WHOIS_RES, 
                                self._peer_info.ip, 
                                self._peer_info.server_port, 
                                self._peer_info.replica_port,
                                self._peer_info.election_port
                            ).encode('UTF-8'),
                            address
                        )
                    elif msg[0] == PRIMARY_UP and len(msg) > 1:
                        self._on_primary_up(msg[1])
                except socket.timeout:
                    continue
        finally:
            sock.close()

    def terminate(self):
        self._stopEvent.set()

        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.sendto(''.encode('UTF-8'), ('', DISCOVERY_PORT))
        sock.close()
