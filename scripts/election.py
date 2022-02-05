import json
import socket
import time
import uuid
import logging
from threading import Thread


class LCR:
    def __init__(self, list_of_uuids: list, my_uuid: uuid.UUID, uuid_to_address: dict):
        self._logger = logging.getLogger('lcr')
        self.__uuid_to_address = uuid_to_address

        self._my_uuid = my_uuid
        self._ring = LCR.__form_ring(list_of_uuids)
        self._participant = None

        self._ring_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        address = self.__uuid_to_address[self._my_uuid]
        self._ring_socket.bind(address)

        self._logger.debug('Participant is up and ready to run LCR at {}:{}'.format(address[0], address[1]))

        self._neighbor_address = self.get_neighbour(self._my_uuid)

    @staticmethod
    def __form_ring(list_of_uuids):
        return sorted([member for member in list_of_uuids])

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

    @staticmethod
    def on_leader_election(leader_uuid):
        print(leader_uuid)

    def run(self):
        while True:
            data, address = self._ring_socket.recvfrom(1024)
            election_message = json.loads(data.decode('UTF-8'))

            if election_message['isLeader']:
                LCR.on_leader_election(election_message['mid'])
                # forward received election message to left neighbour
                if election_message['mid'] != self._my_uuid:
                    self._participant = False
                    self._ring_socket.sendto(json.dumps(election_message).encode('UTF-8'), self._neighbor_address)
            else:
                if election_message['mid'] < self._my_uuid and not self._participant:
                    new_election_message = {
                        "mid": self._my_uuid,
                        "isLeader": False
                    }
                    self._participant = True
                    # send received election message to left neighbour
                    self._ring_socket.sendto(json.dumps(new_election_message).encode('UTF-8'), self._neighbor_address)
                elif election_message['mid'] > self._my_uuid:
                    # send received election message to left neighbour
                    self._participant = True
                    self._ring_socket.sendto(json.dumps(election_message).encode('UTF-8'), self._neighbor_address)
                elif election_message['mid'] == self._my_uuid:
                    election_message["isLeader"] = True
                    # send new election message to left neighbour
                    self._participant = False
                    self._ring_socket.sendto(json.dumps(election_message).encode('UTF-8'), self._neighbor_address)

    def start_election(self):
        election_message = {
            'mid': self._my_uuid,
            'isLeader': False
        }
        self._participant = True
        self._ring_socket.sendto(json.dumps(election_message).encode('UTF-8'), self._neighbor_address)


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
