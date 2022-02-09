import json
import socket
import time
import uuid
import logging
import select
import queue
from threading import Thread


class LCR:
    def __init__(self, list_of_uuids: list, my_uuid: uuid.UUID, uuid_to_address: dict, on_leader_election):
        self._logger = logging.getLogger('lcr')
        self.__uuid_to_address = uuid_to_address
        self.__on_leader_election = on_leader_election

        self._my_uuid = str(my_uuid)
        self._ring = LCR.__form_ring(list_of_uuids)
        self._participant = None

        self._logger.info(str(self._ring))
        self._stopRequest = False

        self._ring_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        address = self.__uuid_to_address[self._my_uuid]
        self._ring_socket.bind(address)
        self._ring_socket.setblocking(0)
        self._outQueue = queue.Queue(maxsize=1024)

        self._logger.debug('Participant is up and ready to run LCR at {}:{}'.format(address[0], address[1]))

        # get neighbor address in the formed ring
        assert self._ring
        self._neighbor_address = self.get_neighbour(self._my_uuid)
        self._logger.debug("Neighbor address: {}".format(str(self._neighbor_address)))

    @staticmethod
    def __form_ring(list_of_uuids):
        return sorted([member for member in list_of_uuids])

    def update_ring(self, list_of_uuids, uuid_to_address):
        self.__uuid_to_address = uuid_to_address
        self._ring = LCR.__form_ring(list_of_uuids)
        # get neighbor address in the formed ring
        assert self._ring
        self._neighbor_address = self.get_neighbour(self._my_uuid)
        self._logger.debug("Neighbor address: {}".format(str(self._neighbor_address)))

    def get_neighbour(self, current_node_uuid, direction='left'):
        current_node_index = self._ring.index(current_node_uuid) if current_node_uuid in self._ring else -1
        if current_node_index != -1:
            if direction == 'left':
                if current_node_index + 1 == len(self._ring):
                    ret_entry = self._ring[0]
                else:
                    ret_entry = self._ring[current_node_index + 1]
            elif direction == 'right':
                if current_node_index == 0:
                    ret_entry = self._ring[len(self._ring) - 1]
                else:
                    ret_entry = self._ring[current_node_index - 1]
            else:
                raise Exception('direction for <get_neighbor> can be only LEFT | RIGHT')
        else:
            return None
        return self.__uuid_to_address[ret_entry]

    def run(self):
        outputs = []

        while not self._stopRequest or not self._outQueue.empty():
            readable, writable, exceptional = select.select([self._ring_socket], outputs, [], 0.5)
            if self._ring_socket in readable and not self._stopRequest:
                data, address = self._ring_socket.recvfrom(1024)
                if data:
                    self.onData(data)

            if not self._outQueue.empty():
                outputs = [self._ring_socket]
            else:
                outputs = []

            if self._ring_socket in writable:
                try:
                    data = self._outQueue.get_nowait()
                except queue.Empty:
                    outputs = []
                else:
                    self._ring_socket.sendto(data[0], data[1])

            # Check if socket is still open
            if self._isSocketClosed():
                self._logger.debug("connection reset by peer")
                break

        self._ring_socket.close()

    def onData(self, data):
        election_message = json.loads(data.decode('UTF-8'))

        if election_message['isLeader']:
            # forward received election message to left neighbour
            if election_message['mid'] != self._my_uuid:
                self._participant = False
                self.sendto(json.dumps(election_message).encode('UTF-8'), self._neighbor_address)
            self.__on_leader_election(election_message['mid'])
        else:
            if election_message['mid'] < self._my_uuid and not self._participant:
                new_election_message = {
                    "mid": self._my_uuid,
                    "isLeader": False
                }
                self._participant = True
                # send received election message to left neighbour
                self.sendto(json.dumps(new_election_message).encode('UTF-8'), self._neighbor_address)
            elif election_message['mid'] > self._my_uuid:
                # send received election message to left neighbour
                self._participant = True
                self.sendto(json.dumps(election_message).encode('UTF-8'), self._neighbor_address)
            elif election_message['mid'] == self._my_uuid:
                election_message["isLeader"] = True
                # send new election message to left neighbour
                self._participant = False
                self.sendto(json.dumps(election_message).encode('UTF-8'), self._neighbor_address)

    def sendto(self, data, address):
        self._outQueue.put((data, address))

    def start_election(self):
        election_message = {
            'mid': self._my_uuid,
            'isLeader': False
        }
        self._participant = True

        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.sendto(json.dumps(election_message).encode('UTF-8'), self._neighbor_address)

    def shutdown(self):
        self._stopRequest = True

    def _isSocketClosed(self):
        try:
            data = self._ring_socket.recvfrom(16, socket.MSG_PEEK)
            if len(data) == 0:
                return True
        except BlockingIOError:
            return False
        except ConnectionResetError:
            return True
        return False


if __name__ == '__main__':
    x = [4, 3, 2, 1]
    print(x)
    uuid_to_addr = {1: ('127.0.0.1', 6455), 2: ('127.0.0.1', 6454), 3: ('127.0.0.1', 6453), 4: ('127.0.0.1', 6451)}
    p1 = LCR(x, 1, uuid_to_addr)
    p2 = LCR(x, 2, uuid_to_addr)
    p3 = LCR(x, 3, uuid_to_addr)
    p4 = LCR(x, 4, uuid_to_addr)

    tp1 = Thread(target=p1.run)
    tp2 = Thread(target=p2.run)
    tp3 = Thread(target=p3.run)
    tp4 = Thread(target=p4.run)

    tp1.start()
    tp2.start()
    tp3.start()
    tp4.start()

    print('p2 starts election')
    p2.start_election()

    time.sleep(5)
    print('p3 starts election')
    p3.start_election()

    tp1.join()
    tp2.join()
    tp3.join()
    tp4.join()
