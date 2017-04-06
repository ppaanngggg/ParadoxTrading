import typing

import h5py
import psycopg2
import psycopg2.extensions

from ParadoxTrading.Utils import DataStruct


class RegisterAbstract:
    def __init__(self):
        # strategies linked to this market register
        self.strategy_set: typing.Set[typing.AnyStr] = set()

    def addStrategy(self, _strategy):
        """
        link strategy to self

        :param _strategy: strategy object (just use its name)
        :return:
        """
        self.strategy_set.add(_strategy.name)

    def toJson(self) -> str:
        raise NotImplementedError('toJson')

    def toKwargs(self) -> dict:
        raise NotImplementedError('toKwargs')

    @staticmethod
    def fromJson(_json_str: str) -> 'RegisterAbstract':
        raise NotImplementedError('fromJson')

    def __repr__(self):
        return 'Key:' + '\n' + \
               '\t' + self.toJson() + '\n' + \
               'Strategy:' + '\n' + \
               '\t' + str(self.strategy_set)


class FetchAbstract:
    def fetchSymbol(
            self, _tradingday: str, **kwargs
    ) -> typing.Union[None, str]:
        raise NotImplementedError('fetchSymbol')

    def fetchData(
            self, _tradingday: str, _symbol: str, **kwargs
    ) -> typing.Union[None, DataStruct]:
        raise NotImplementedError('fetchData')

    def fetchDayData(
            self, _begin_day: str, _end_day: str, _symbol: str, **kwargs
    ) -> DataStruct:
        raise NotImplementedError('fetchDayData')

    def _get_psql_con_cur(self) -> typing.Tuple[
        psycopg2.extensions.connection, psycopg2.extensions.cursor
    ]:
        if not self.psql_con:
            self.psql_con: psycopg2.extensions.connection = psycopg2.connect(
                dbname=self.psql_dbname,
                host=self.psql_host,
                user=self.psql_user,
                password=self.psql_password,
            )
        if not self.psql_cur:
            self.psql_cur: psycopg2.extensions.cursor = self.psql_con.cursor()

        return self.psql_con, self.psql_cur

    def cache2DataStruct(
            self, _symbol: str, _tradingday: str, _index: str
    ) -> typing.Union[None, DataStruct]:
        f = h5py.File(self.cache_path, 'a')
        try:
            grp = f[_symbol.lower() + '/' + _tradingday]
        except KeyError:
            return None

        datastruct = DataStruct(list(grp.keys()), _index.lower())
        for k in grp.keys():
            dataset = grp[k]
            datastruct.data[k] = dataset[:].tolist()
            if 'timestamp' in dataset.attrs['type']:
                datastruct.float2datetime(k)

        f.close()
        return datastruct

    def DataStruct2cache(
            self, _symbol: str, _tradingday: str,
            _columns: typing.List[str], _types: typing.List[str],
            _datastruct: DataStruct
    ):
        f = h5py.File(self.cache_path, 'a')

        for c, t in zip(_columns, _types):
            if 'int' in t:
                dtype = 'int32'
            elif 'char' in t:
                dtype = h5py.special_dtype(vlen=str)
            else:
                dtype = 'float64'

            if 'timestamp' in t:
                _datastruct.datetime2float(c)

            dataset = f.create_dataset(
                _symbol.lower() + '/' + _tradingday + '/' + c,
                (len(_datastruct),),
                dtype=dtype,
            )
            dataset[:] = _datastruct.data[c]
            dataset.attrs['type'] = t

            if 'timestamp' in t:
                _datastruct.float2datetime(c)

        f.close()
