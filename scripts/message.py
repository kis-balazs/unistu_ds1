import pickle
from vectorclock import VectorClock

class Message:
    # in-class class to access the decode message's fields using dot notation
    class __dotdict(dict):
        __getattr__ = dict.get

    MSG_TYPES = ['text_message', 'election_message']

    @staticmethod
    def encode(vc: VectorClock, type: str, msg: str) -> None:
        assert type in Message.MSG_TYPES, 'type parameter not recognized'
        # serialize the message together with the vector clock
        json_data = {
            'vc': vc,
            'type': type,
            'body': msg
        }
        return pickle.dumps(json_data)

    @staticmethod
    def decode(pickledata):
        data = pickle.loads(pickledata)
        assert data['type'] in Message.MSG_TYPES, 'type parameter not recognized'
        return Message.__dotdict(data)


if __name__ == '__main__':
    vc = VectorClock(1)

    y = Message.encode(vc, 'text_message', 'hi')
    
    x = Message.decode(y)
    print(x.type)
    print(x.body)
    print(x.vc.vcDictionary)
