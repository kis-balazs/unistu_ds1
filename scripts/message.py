import pickle

from scripts.vectorclock import VectorClock


class Message:
    # in-class class to access the decode message's fields using dot notation
    class DotDict(dict):
        __getattr__ = dict.get

    @staticmethod
    def encode(vc: VectorClock, type: str, success: bool, msg) -> bytes:
        # serialize the message together with the vector clock
        json_data = {
            'vc': vc.vcDictionary,
            'type': type,
            'body': msg,
            'success': success
        }
        return pickle.dumps(json_data)

    @staticmethod
    def decode(pickle_data):
        data = Message.DotDict(pickle.loads(pickle_data))
        Message.dotdictify(data)
        return data

    @staticmethod
    def dotdictify(d):
        for k, v in d.items():
            if isinstance(v, dict):
                d[k] = Message.DotDict(v)
                Message.dotdictify(v)


if __name__ == '__main__':
    vc = VectorClock(1)

    y = Message.encode(vc, 'text_message', True, {'uuid': '12345'})

    x = Message.decode(y)
    print(x.type)
    print(x.body.uuid)
    print(x.vc)
