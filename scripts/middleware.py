import uuid
import logging

from scripts.message import Message
from scripts.vectorclock import VectorClock

class Middleware:
    __INSTANCE__ = None

    def __init__(self):
        self.vc = VectorClock()
        self.clients = {}
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

        body = { 'uuid': str(client.uuid) }
        data = Message.encode(self.vc, 'join_cluster', True, body)

        self.logger.debug("Sending join_message")
        client.send(data)
        return client

    def shutdown(self):
        for client in self.clients.values():
            client.closeConnection()

    def clientDisconnected(self, client):
        self.clients.pop(str(client.uuid))

    def newMessage(self, send_client, message):
        self.vc.increaseClock(send_client.uuid)
        for client in self.clients.values():
            data = Message.encode(self.vc, 'send_text', True, message)
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
            Middleware.get().newMessage(self, msg.body)

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
