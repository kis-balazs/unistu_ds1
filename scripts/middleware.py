import logging
import uuid

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
