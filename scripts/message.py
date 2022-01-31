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
    def decode(pickledata):
        data = Message.DotDict(pickle.loads(pickledata))
        data.body = Message.dotdictify(data.body)
        return data

    @staticmethod
    def dotdictify(d):
        if isinstance(d, dict):
            return Message.DotDict(d)
        else:
            return d


if __name__ == '__main__':
    vc = VectorClock('UI-123-23423')

    y = Message.encode(vc, 'text_message', True, {'hi': 'aa'})

    x = Message.decode(y)
    print(x.type)
    print(x.body.hi)
    print(x.vc)
