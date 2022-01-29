import uuid

from scripts.message import Message
from scripts.vectorclock import VectorClock

class Middleware:
    __INSTANCE__ = None

    def __init__(self):
        self.vc = VectorClock()
        self.clients = {}

    @staticmethod
    def get():
        if Middleware.__INSTANCE__ is None:
            Middleware.__INSTANCE__ = Middleware()
        return Middleware.__INSTANCE__
    
    def joinClient(self, conn):
        client = Client(conn)
        self.clients[client.uuid] = client

        if client.uuid not in self.vc.vcDictionary:
            self.vc.addParticipantToClock(client.uuid)

        body = { 'uuid': str(client.uuid) }
        data = Message.encode(self.vc, 'join_cluster', True, body)

        print("sending join_cluster")
        client.send(data)
        return client

class Client:
    def __init__(self, conn):
        self.uuid = uuid.uuid4()
        self._conn = conn

    def send(self, data):
        self._conn.send(data)

    def receive(self, data):
        msg = Message.decode(data)
        if msg.type == 'send_text':
            print('Client {} sent: {}'.format(str(self.uuid), msg.body))