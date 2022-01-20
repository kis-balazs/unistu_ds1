import threading
import socket
import ipaddress
import threading
import time


SUBNETMASK = "255.255.255.0"
BROADCAST_PORT = 61425


class Middleware():
    IP_ADRESS_OF_THIS_PC = socket.gethostbyname(socket.gethostname())
    net = ipaddress.IPv4Network(IP_ADRESS_OF_THIS_PC + '/' + SUBNETMASK, False)
    BROADCAST_IP = net.broadcast_address.exploded


    holdBackQueue = []
    deliveryQueue = []
     
    def __init__(self, type):
        self._listen_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        if type:
            self.listenUDP()
        else:
            self.broadcastToAll("abc")
            time.sleep(2)
            self.broadcastToAll("xyz")

    @staticmethod
    def broadcast( ip, port, broadcast_message):
        # Create a UDP socket
        broadcast_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        # Send message on broadcast address
        broadcast_socket.sendto(str.encode(broadcast_message), (ip, port))
        broadcast_socket.close()

    def broadcastToAll(self, message=None):
        
        # Send broadcast message
        if not message:
            message = self.IP_ADRESS_OF_THIS_PC + ' sent a broadcast'
        print('$> broadcasting message...')
        Middleware.broadcast(Middleware.BROADCAST_IP, BROADCAST_PORT, message)
        
    def sendMessageTo(self, uuid, message):
        pass

    def listenUDP(self):
        self._listen_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        self._listen_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._listen_socket.bind((self.IP_ADRESS_OF_THIS_PC, BROADCAST_PORT))
        print('$> socket listening...')
        while True:
            data, addr = self._listen_socket.recvfrom(1024)
            
            if data:
                print("Received broadcast message:", data.decode())
