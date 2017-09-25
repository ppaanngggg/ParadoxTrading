import pickle
import typing


class Serializable:
    def __init__(self):
        self.pickle_keys: typing.Set[str] = set()

    def addPickleKey(self, *args):
        """
        add multi-keys to pickle set

        :param args:
        :return:
        """
        for a in args:
            assert a in vars(self)
            assert a not in self.pickle_keys
            self.pickle_keys.add(a)

    def save_state_dict(self) -> typing.Dict[str, typing.Any]:
        tmp = {}
        for k in self.pickle_keys:
            obj = vars(self)[k]
            if isinstance(obj, Serializable):
                tmp[k] = obj.save_state_dict()
            else:
                tmp[k] = obj
        return tmp

    def load_state_dict(
            self, _state_dict: typing.Dict[str, typing.Any]
    ):
        for k, v in _state_dict.items():
            assert k in vars(self)
            obj = vars(self)[k]
            if isinstance(obj, Serializable):
                obj.load_state_dict(_state_dict[k])
            else:
                self.__dict__[k] = v

    def save(self, _filename: str):
        if not _filename.endswith('.pkl'):
            _filename += '.pkl'
        pickle.dump(
            self.save_state_dict(),
            open(_filename, 'wb')
        )

    def load(self, _filename: str):
        state_dict = pickle.load(
            open(_filename, 'rb')
        )
        self.load_state_dict(state_dict)
