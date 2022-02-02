import logging
import uuid

from scripts.message import Message
from scripts.vectorclock import VectorClock


class History:
    def __init__(self):
        self._history = []

    def on_new_message(self, msg):
        server_view_vc = Middleware.get().vc.vcDictionary
        client_view_vc = msg.vc

        for k, v in server_view_vc.items():
            # already received past message
            if client_view_vc[k] <= server_view_vc[k]:
                pass
            # the exactly next message, just add to history
            elif client_view_vc[k] == server_view_vc[k] + 1:
                self._history.append(msg.body)
                # here gets the server view updated!
                server_view_vc[k] += 1
            else:
                print('! Houston we have a problem')  # todo solve this

    def get_history(self):
        return self._history


class Middleware:
    __INSTANCE__ = None

    def __init__(self):
        self.vc = VectorClock()
        self.history = History()
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

    def shutdown(self):
        for client in self.clients.values():
            client.closeConnection()

    def clientDisconnected(self, client):
        self.clients.pop(str(client.uuid))
        del self.vc[str(client.uuid)]

    def newMessage(self, send_client, message):
        self.history.on_new_message(message)
        # already increased in collecting in history
        # self.vc.increaseClock(send_client.uuid)
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
