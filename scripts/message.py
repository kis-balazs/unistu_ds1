import pickle
import logging
from scripts.vectorclock import VectorClock


class Message:
    # in-class class to access the decode message's fields using dot notation
    class _dotdict(dict):
        __getattr__ = dict.get

    @staticmethod
    def encode(vc: VectorClock, type: str, success: bool, msg: str) -> None:
        # serialize the message together with the vector clock
        json_data = {
            'vc': vc.vcDictionary,
            'type': type,
            'body': msg,
            'success': success
        }
        return pickle.dumps(json_data)

    @staticmethod
    def decode(pickledata):
        data = Message._dotdict(pickle.loads(pickledata))
        Message.dotdictify(data)
        return data

    @staticmethod
    def dotdictify(d):
        for k, v in d.items():
            if type(v) == dict:
                d[k] = Message._dotdict(v)
                Message.dotdictify(v)


if __name__ == '__main__':
    vc = VectorClock(1)

    y = Message.encode(vc, 'text_message', True, {'hi': 'hello'})

    x = Message.decode(y)
    print(x.type)
    print(x.body.hi)
    print(x.vc)
