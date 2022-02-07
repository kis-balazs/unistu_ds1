from dataclasses import dataclass
import logging
import uuid
import threading
import time

from scripts.election import LCR
from scripts.message import Message
from scripts.vectorclock import VectorClock


class History:
    def __init__(self):
        self._history = []
        self._backlog = []

    def on_new_message(self, msg, sender):
        server_view_vc = Middleware.get().vc.vcDictionary
        client_view_vc = msg.vc
        _backlog_object = None
        _append_to_history_index = None

        for k, v in server_view_vc.items():
            # already received past message
            if client_view_vc[k] <= server_view_vc[k]:
                pass
            # the exactly next message, just add to history
            elif client_view_vc[k] == server_view_vc[k] + 1:
                if str(sender.uuid) == k:
                    _append_to_history_index = k
                else:
                    _backlog_object = (msg, sender)
            else:
                _backlog_object = (msg, sender)

        if _backlog_object:
            self._backlog.append(_backlog_object)
        elif _append_to_history_index is not None:
            self._history.append(msg.body)
            server_view_vc[_append_to_history_index] += 1
            self.try_pop_backlog(msg, _append_to_history_index)
        else:
            raise Exception('message has to be appended either in backlog or in history!')

    def get_history(self):
        return self._history

    def try_pop_backlog(self, msg, sender_uuid):
        if not self._backlog:
            return

        elem_to_pop = None

        for bmsg, bsender in self._backlog:
            for k, v in bmsg.vc:
                if bmsg.vc[k] <= msg.vc[k]:
                    pass
                elif bmsg.vc[k] == msg.vc[k] + 1:
                    if k == sender_uuid:
                        elem_to_pop = (bmsg, bsender)
                else:
                    elem_to_pop = None

        if elem_to_pop:
            self._backlog.remove(elem_to_pop)
            # append to history
            self._history.append(elem_to_pop[0])


class Middleware:
    __INSTANCE__ = None

    def __init__(self):
        self.vc = VectorClock()
        self.history = History()
        self.uuid = None
        self.clients = {}
        self.replicas = {}
        self.pendingReplicas = {}
        self.replicaPeerAddresses = {}
        self.replicaConn = None
        self.logger = logging.getLogger("middleware")

    @staticmethod
    def get():
        if Middleware.__INSTANCE__ is None:
            Middleware.__INSTANCE__ = Middleware()
        return Middleware.__INSTANCE__

    def joinClient(self, conn):
        client = Client(conn)
        self.clients[str(client.uuid)] = client
        if client.uuid not in self.vc.vcDictionary:
            self.vc.addParticipantToClock(client.uuid)

        body = {'uuid': str(client.uuid)}
        data = Message.encode(self.vc, 'join_cluster', True, body)

        self.logger.debug("Sending join_message")
        client.send(data)

        # if client joined after conversation started
        if self.history.get_history():
            header = '### Message History ###'
            data = Message.encode(
                self.vc, 'history', True, [header] + self.history.get_history() + ['#' * len(header)]
            )
            client.send(data)
        return client

    def replicaOpen(self, conn, election_port):
        self.replicaConn = conn
        self.replicaElectionPort = election_port

        msg = Message.encode(self.vc, 'join_replica_req', True, election_port)
        self.replicaConn.send(msg)

    def replicaReceive(self, data):
        msg = Message.decode(data)
        self.logger.debug("received message '{}'".format(msg.type))
        if msg.type == 'join_replica':
            self.newReplicaPeer(msg.body)

    def replicaSend(self, data):
        self.replicaConn.send(data)

    def replicaClose(self):
        # differentiate between close from server and graceful shutdown
        # TODO re-election
        pass

    def createReplica(self, conn):
        self.logger.debug("Creating pending replica")
        replica = Replica(conn)
        self.pendingReplicas[str(replica.uuid)] = replica

        return replica

    def joinReplica(self, replica, election_port):
        self.logger.debug("Accepting replica {}, sending 'join_replica'".format(str(replica.uuid)))
        self.replicas[str(replica.uuid)] = replica
        self.pendingReplicas.pop(str(replica.uuid))
        self.replicaPeerAddresses[str(replica.uuid)] = (replica.getAddress(), election_port)
        msg = Message.encode(self.vc, 'join_replica', True, {
            'uuid': str(replica.uuid),
            'replicas': [
                {
                    'uuid': uuid,
                    'address': address[0],
                    'election_port': address[1]
                } 
                for uuid, address in self.replicaPeerAddresses.items()
            ],
        })
        self.election.update_ring(self.replicaPeerAddresses.keys(), self.replicaPeerAddresses)
        for r in self.replicas.values():
            r.send(msg)

    def newReplicaPeer(self, body):
        self.logger.debug("Replicas: {}".format(str(body.replicas)))
        self.replicaPeerAddresses = {}

        self.replicaPeerAddresses = {}
        for r in body.replicas:
            self.replicaPeerAddresses[r['uuid']] = (r['address'], r['election_port'])

        if self.uuid is None:
            self.logger.info("Accepted into cluster")
            self.uuid = uuid.UUID(body.uuid)
            self.startElectionThread(self.replicaElectionPort)
            time.sleep(0.5)
            self.election.start_election()
        else:
            self.election.update_ring(self.replicaPeerAddresses.keys(), self.replicaPeerAddresses)

    def onPrimaryStart(self, own_address):
        self.uuid = uuid.uuid4()
        self.replicaPeerAddresses[str(self.uuid)] = own_address
        self.startElectionThread(own_address[1])

    def startElectionThread(self, port):
        self.logger.info("Starting election thread on port {}".format(str(port)))
        self.election = LCR(self.replicaPeerAddresses.keys(), self.uuid, self.replicaPeerAddresses, self.onNewLeader)
        self.electionThread = threading.Thread(target=self.election.run)
        self.electionThread.start()

    def onNewLeader(self, leader_uuid):
        self.logger.info("New leader: {}".format(leader_uuid))

    def shutdown(self):
        self.election.shutdown()
        for client in self.clients.values():
            client.closeConnection()
        for replica in self.replicas.values():
            replica.closeConnection()

    def clientDisconnected(self, client):
        self.clients.pop(str(client.uuid))
        del self.vc[str(client.uuid)]

    def replicaDisconnected(self, replica):
        # TODO Notify other replicas
        self.replicas.pop(str(replica.uuid))

    def newMessage(self, send_client, message):
        self.history.on_new_message(message, send_client)

        for client in self.clients.values():
            data = Message.encode(self.vc, 'send_text', True, message.body)
            client.send(data)


class Client:
    def __init__(self, conn):
        self.uuid = uuid.uuid4()
        self._conn = conn
        self._logger = logging.getLogger("client<{}>".format(str(self.uuid)))

    def send(self, data):
        self._conn.send(data)

    def receive(self, data):
        msg = Message.decode(data)
        if msg.type == 'send_text':
            self._logger.info('received text: {}'.format(msg.body))
            Middleware.get().newMessage(self, msg)

    def closeConnection(self):
        Middleware.get().vc.increaseClock(self.uuid)
        data = Message.encode(
            Middleware.get().vc,
            'server_close',
            True,
            None
        )
        self._conn.send(data)
        self._conn.shutdown()

class Replica:
    def __init__(self, conn):
        self.uuid = uuid.uuid4()
        self._conn = conn
        self._logger = logging.getLogger("replica<{}>".format(str(self.uuid)))

    def send(self, data):
        self._conn.send(data)

    def receive(self, data):
        msg = Message.decode(data)
        if msg.type == 'join_replica_req':
            Middleware.get().joinReplica(self, msg.body)

    def closeConnection(self):
        self._conn.shutdown()

    def getAddress(self):
        return self._conn.getAddress()[0]
