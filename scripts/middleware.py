from dataclasses import dataclass
import logging
import re
import uuid
import threading
import time

from scripts.election import LCR
from scripts.message import Message
from scripts.vectorclock import VectorClock

class PeerInfo:
    def __init__(self, ip, server_port, replica_port, election_port):
        self.ip = ip
        self.server_port = server_port
        self.replica_port = replica_port
        self.election_port = election_port

    def __str__(self):
        return "ip={} server_port={} replica_port={} election_port".format(self.ip, self.server_port, self.replica_port, self.election_port)

    def serverAddress(self):
        return self.ip, self.server_port

    def replicaAddress(self):
        return self.ip, self.replica_port

    def electionAddress(self):
        return self.ip, self.election_port

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
        self.peers = {}
        self.oldPeers = {}
        self.replicaPeerInfo = None
        self.replicaConn = None
        self.isPrimary = False
        self.logger = logging.getLogger("middleware")
        self.serverHandle = None
        self.electedLeader = None
        self.primaryUuid = None
        self.election = None
        self.electionThread = None

    @staticmethod
    def get():
        if Middleware.__INSTANCE__ is None:
            Middleware.__INSTANCE__ = Middleware()
        return Middleware.__INSTANCE__

    def setServerHandle(self, server_handle):
        self.serverHandle = server_handle

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

    def replicaOpen(self, conn, peer_info):
        self.replicaConn = conn
        self.replicaPeerInfo = peer_info

        if self.uuid is not None:
            # Rejoin
            msg = Message.encode(self.vc, 'rejoin_replica_req', True, str(self.uuid))
        else:
            # Join
            msg = Message.encode(self.vc, 'join_replica_req', True, peer_info)
            
        self.replicaConn.send(msg)

    def replicaReceive(self, data):
        msg = Message.decode(data)
        self.logger.debug("received message '{}'".format(msg.type))
        if msg.type == 'join_replica':
            self.onJoinAccepted(msg.body)
        if msg.type == 'replica_update':
            self.onPeerUpdate(msg.body)

    def replicaSend(self, data):
        self.replicaConn.send(data)

    def replicaClose(self, stopped_by_peer):
        # differentiate between close from server and graceful shutdown
        self.replicaConn = None
        self.peers.pop(str(self.primaryUuid))
        self.update_election_ring()
        self.election.start_election()

    def createReplica(self, conn):
        self.logger.debug("Creating pending replica")
        replica = Replica(conn)
        self.pendingReplicas[str(replica.uuid)] = replica

        return replica

    def joinReplica(self, replica, peer_info):
        self.logger.debug("Accepting replica {}, sending 'join_replica'".format(str(replica.uuid)))
        self.replicas[str(replica.uuid)] = replica
        if str(replica.uuid) in self.pendingReplicas.keys():
            self.pendingReplicas.pop(str(replica.uuid))
        self.peers[str(replica.uuid)] = peer_info
        
        self.sendUpdatedPeerList()
        self.update_election_ring()

        msg = Message.encode(self.vc, 'join_replica', True, str(replica.uuid))
        replica.send(msg)
    
    def sendUpdatedPeerList(self):
        msg = Message.encode(self.vc, 'replica_update', True, {
            'primary': str(self.uuid),
            'replicas': [
                {
                    'uuid': uuid,
                    'info': {
                        'ip': peer.ip,
                        'server_port': peer.server_port,
                        'replica_port': peer.replica_port,
                        'election_port': peer.election_port,
                    },
                } 
                for uuid, peer in self.peers.items()
            ],
        })
        for r in self.replicas.values():
            r.send(msg)

    def rejoinReplica(self, replica, prev_uuid):
        self.pendingReplicas.pop(str(replica.uuid))
        replica.uuid = uuid.UUID(prev_uuid)
        if prev_uuid in self.oldPeers.keys():
            self.logger.debug("Rejoining replica {}".format(prev_uuid))
            self.joinReplica(replica, self.oldPeers[prev_uuid])
        else:
            self.logger.debug("Unrecognized replica {}".format(prev_uuid))

    def update_election_ring(self):
        self.election.update_ring(self.peers.keys(), {
            uuid: peer.electionAddress() for uuid, peer in self.peers.items()
        })

    def onJoinAccepted(self, own_uuid):
        self.logger.info("Accepted into cluster")
        self.uuid = uuid.UUID(own_uuid)
        
        self.startElectionThread(self.replicaPeerInfo.election_port)
        time.sleep(0.5)
        self.election.start_election()

    def onPeerUpdate(self, body):
        self.logger.debug("\nReplicas:\n{}".format('\n'.join([
            '\t{} (server_port={})'.format(r['uuid'], str(r['info']['server_port']))
            for r in body.replicas
        ])))

        self.primaryUuid = uuid.UUID(body['primary'])
        self.peers = {}
        for r in body.replicas:
            self.peers[r['uuid']] = PeerInfo(
                r['info']['ip'],
                r['info']['server_port'],
                r['info']['replica_port'],
                r['info']['election_port'],
            )

        if self.election:
            self.update_election_ring()

    def onPrimaryStart(self, own_peer_info):
        self.isPrimary = True
        self.uuid = uuid.uuid4()
        self.peers[str(self.uuid)] = own_peer_info
        self.startElectionThread(own_peer_info.election_port)

    def startElectionThread(self, port):
        if self.electionThread:
            return
        
        self.logger.info("Starting election thread on port {}".format(str(port)))
        self.election = LCR(self.peers.keys(), self.uuid, {
            uuid: peer.electionAddress() for uuid, peer in self.peers.items()
        }, self.onNewLeader)
        self.electionThread = threading.Thread(target=self.election.run)
        self.electionThread.start()

    def onNewLeader(self, leader_uuid):
        self.logger.info("New leader: {}".format(leader_uuid))
        self.electedLeader = leader_uuid
        print(str(self.uuid), leader_uuid, str(self.isPrimary))
        if str(self.uuid) == leader_uuid and not self.isPrimary:
            # Was replica before. Promote self
            self.isPrimary = True
            self.primaryUuid = self.uuid
            self.replicas = {}
            self.oldPeers = self.peers
            self.peers = {}
            self.peers[str(self.uuid)] = self.oldPeers[str(self.uuid)]
            self.serverHandle.promote()

    def onPrimaryUpMsg(self, primary_uuid):
        self.logger.debug("Received primary_up: leader={} elected={}".format(primary_uuid, self.electedLeader))
        if primary_uuid == str(self.uuid):
            # Own message. Ignore
            return
        
        if primary_uuid != self.electedLeader:
            # Primary was not elected. Ignore
            return

        self.isPrimary = False
        self.primaryUuid = uuid.UUID(primary_uuid)
        self.serverHandle.demote(self.peers[primary_uuid])

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
        self.replicas.pop(str(replica.uuid))
        self.peers.pop(str(replica.uuid))
        self.sendUpdatedPeerList()
        self.update_election_ring()

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
        elif msg.type == 'rejoin_replica_req':
            Middleware.get().rejoinReplica(self, msg.body)

    def closeConnection(self):
        self._conn.shutdown()

    def getAddress(self):
        return self._conn.getAddress()[0]
