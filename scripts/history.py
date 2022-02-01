from scripts.middleware import Middleware


class History:
    def __init__(self):
        self._history = []

    def on_new_message(self):
        server_view_vc = Middleware.get().vc
