import typing
import pickle

class Serializable:

    def __init__(self):
        self.pickles:typing.Set[str] = set()


    def addPickleSet(self, *args):
        for a in args:
            assert a in vars(self).keys()
            self.pickles.add(a)

    def save(self, _path: str, _filename: str):
        tmp = {}
        for k in self.pickles:
            tmp[k] = vars(self)[k]
        path = _path
        if not path.endswith('/'):
            path += '/'
        path = '{}{}.pkl'.format(path, _filename)
        pickle.dump(tmp, open(path, 'wb'))

    def load(self, _path: str, _filename: str):
        path = _path
        if not path.endswith('/'):
            path += '/'
        path = '{}{}.pkl'.format(path, _filename)
        tmp: typing.Dict[str, typing.Any] = pickle.load(open(path, 'rb'))
        for k, v in tmp.items():
            self.__dict__[k] = v