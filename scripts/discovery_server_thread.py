import socket
import logging
from threading import Thread

DISCOVERY_PORT = 37020

WHOIS_REQ = 'whois_primary_req'
WHOIS_RES = 'whois_primary_res'

def find_primary():
    logging.info('discovery: Starting discovery process')
    broadcast_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM) #UDP
    broadcast_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    broadcast_socket.settimeout(5)

    try:
        # Send request
        logging.debug('discovery: Sending request')
        broadcast_socket.sendto(WHOIS_REQ.encode('UTF-8'), ('255.255.255.255', DISCOVERY_PORT))

        # Receive response
        data, address = broadcast_socket.recvfrom(1024)
        if data.decode('UTF-8') == WHOIS_RES:
            logging.info('discovery: Received primary address: ' + str(address))
            return address[0]
    except socket.timeout:
        logging.debug('discovery: Timed out')
    finally:
        broadcast_socket.close()
    return None


class DiscoveryServerThread(Thread):
    def __init__(self):
        Thread.__init__(self)
        self.running = True

    def run(self):
        listen_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM) #UDP
        listen_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        listen_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listen_socket.settimeout(5)
        listen_socket.bind(('', DISCOVERY_PORT))

        logging.info('discovery: Start listening for discovery requests...')
        try:
            while self.running:
                try:
                    data, address = listen_socket.recvfrom(1024)
                    if data.decode('UTF-8') == WHOIS_REQ:
                        logging.debug('discovery: Received request from ' + str(address))
                        listen_socket.sendto(WHOIS_RES.encode('UTF-8'), address)
                except socket.timeout:
                    continue
        finally:
            listen_socket.close()
        
    def terminate(self):
        self.running = False
		

if __name__ == '__main__':
	logging.basicConfig(format='[%(asctime)s] [%(levelname)-05s] %(message)s', level=logging.DEBUG)
	ds = DiscoveryServerThread()
	ds.start()
	ds.join()
