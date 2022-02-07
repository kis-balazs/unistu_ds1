import logging
import multiprocessing
import socket
from threading import Thread

DISCOVERY_PORT = 37020

WHOIS_REQ = 'whois_primary_req'
WHOIS_RES = 'whois_primary_res'

discovery_logger = logging.getLogger("discovery")

class PrimaryInfo:
    def __init__(self, ip, server_port, replica_port):
        self.ip = ip
        self.server_port = server_port
        self.replica_port = replica_port

    def __str__(self):
        return "ip={} server_port={} replica_port={}".format(self.ip, self.server_port, self.replica_port)

    def serverAddress(self):
        return self.ip, self.server_port

    def replicaAddress(self):
        return self.ip, self.replica_port

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
        if len(msg) != 4:
            return None
        if msg[0] == WHOIS_RES:
            print('ip', msg[1])
            primary = PrimaryInfo(ip=msg[1], server_port=int(msg[2]), replica_port=int(msg[3]))
            discovery_logger.info('Received primary address: {}'.format(str(primary)))
            return primary
    except socket.timeout:
        discovery_logger.debug('Timed out')
    finally:
        broadcast_socket.close()

    return None


class DiscoveryServerThread(Thread):
    def __init__(self, server_port, replica_port):
        Thread.__init__(self)
        self._stopEvent = multiprocessing.Event()
        self._logger = logging.getLogger("discovery_server")
        self._server_port = server_port
        self._replica_port = replica_port

    def run(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)  # UDP
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.settimeout(5)
        sock.bind(('', DISCOVERY_PORT))

        own_host = socket.gethostname()
        own_ip = socket.gethostbyname(own_host)

        self._logger.info('Start listening for discovery requests...')
        try:
            while not self._stopEvent.is_set():
                try:
                    data, address = sock.recvfrom(1024)
                    if data.decode('UTF-8') == WHOIS_REQ:
                        self._logger.debug('Received request from ' + str(address))
                        sock.sendto("{} {} {} {}".format(WHOIS_RES, own_ip, self._server_port, self._replica_port).encode('UTF-8'), address)
                except socket.timeout:
                    continue
        finally:
            sock.close()

    def terminate(self):
        self._stopEvent.set()

        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.sendto(''.encode('UTF-8'), ('', DISCOVERY_PORT))
        sock.close()
