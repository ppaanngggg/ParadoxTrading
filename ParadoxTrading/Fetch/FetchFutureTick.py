import json
import typing

import h5py
import psycopg2
import pymongo
from pymongo import MongoClient

from ParadoxTrading.Fetch import FetchAbstract, RegisterAbstract
from ParadoxTrading.Utils import DataStruct


class RegisterFuture(RegisterAbstract):
    def __init__(
            self,
            _product: str = None, _instrument: str = None,
            _product_index: bool = False, _sub_dominant: bool = False
    ):
        """
        Market Register is used to store market sub information,
        pre-processed data and strategies used it

        :param _product: reg which product, if not None, ignore instrument
        :param _instrument: reg which instrument
        :param _product_index:
        :param _sub_dominant: only work when use product,
            false means using dominant inst,
            true means sub dominant one
        """
        super().__init__()
        assert _product is not None or _instrument is not None

        # market register info
        self.product = _product
        self.instrument = _instrument
        self.product_index = _product_index
        self.sub_dominant = _sub_dominant

    def toJson(self) -> str:
        """
        encode register info into json str

        :return: json str
        """
        return json.dumps((
            ('product', self.product),
            ('instrument', self.instrument),
            ('product_index', self.product_index),
            ('sub_dominant', self.sub_dominant),
        ))

    def toKwargs(self) -> dict:
        return {
            '_product': self.product,
            '_instrument': self.instrument,
            '_product_index': self.product_index,
            '_sub_dominant': self.sub_dominant,
        }

    @staticmethod
    def fromJson(_json_str: str) -> 'RegisterFuture':
        """
        create object from a json str

        :param _json_str: json str stores register info
        :return: market register object
        """
        data: typing.Dict[str, typing.Any] = dict(json.loads(_json_str))
        return RegisterFuture(
            data['product'],
            data['instrument'],
            data['product_index'],
            data['sub_dominant'],
        )

    def __repr__(self):
        return 'Key:' + '\n' + \
               '\t' + self.toJson() + '\n' + \
               'Params:' + '\n' + \
               '\tproduct: ' + str(self.product) + '\n' + \
               '\tinstrument: ' + str(self.instrument) + '\n' + \
               '\tproduct_index: ' + str(self.product_index) + '\n' + \
               '\tsub_dominant: ' + str(self.sub_dominant) + '\n' + \
               'Strategy: ' + '\n' + \
               '\t' + '; '.join(self.strategy_set)


class RegisterFutureTick(RegisterFuture):
    pass


class FetchFutureTick(FetchAbstract):
    def __init__(self):
        self.mongo_host = 'localhost'
        self.mongo_prod_db = 'FutureProd'
        self.mongo_inst_db = 'FutureInst'

        self.psql_host = 'localhost'
        self.psql_dbname = 'FutureTick'
        self.psql_user = 'pang'
        self.psql_password = ''

        self.cache_path = 'FutureTick.hdf5'

    def productList(self) -> list:
        client = MongoClient(host=self.mongo_host)
        db = client[self.mongo_prod_db]
        ret = db.collection_names()
        client.close()

        return ret

    def productIsTrade(
            self, _product: str, _tradingday: str
    ) -> bool:
        """
        check whether product is traded on tradingday

        Args:
            _product (str): which product.
            _tradingday (str): which day.

        Returns:
            bool: True for traded, False for not
        """
        client = MongoClient(host=self.mongo_host)
        db = client[self.mongo_prod_db]
        coll = db[_product.lower()]
        count = coll.count({'TradingDay': _tradingday})
        client.close()

        return count > 0

    def productLastTradingDay(
            self, _product: str, _tradingday: str
    ) -> typing.Union[None, str]:
        """
        get the first day less then _tradingday of _product

        Args:
            _product (str): which product.
            _tradingday (str): which day.

        Returns:
            str: if None, it means nothing found
        """
        client = MongoClient(host=self.mongo_host)
        db = client[self.mongo_prod_db]
        coll = db[_product.lower()]
        d = coll.find_one(
            {'TradingDay': {'$lt': _tradingday}},
            sort=[('TradingDay', pymongo.DESCENDING)]
        )
        client.close()

        return d['TradingDay'] if d is not None else None

    def productNextTradingDay(
            self, _product: str, _tradingday: str
    ) -> typing.Union[None, str]:
        """
        get the first day greater then _tradingday of _product

        Args:
            _product (str): which product.
            _tradingday (str): which day.

        Returns:
            str: if None, it means nothing found
        """
        client = MongoClient(host=self.mongo_host)
        db = client[self.mongo_prod_db]
        coll = db[_product.lower()]
        d = coll.find_one(
            {'TradingDay': {'$gt': _tradingday}},
            sort=[('TradingDay', pymongo.ASCENDING)]
        )
        client.close()

        return d['TradingDay'] if d is not None else None

    def instrumentIsTrade(
            self, _instrument: str, _tradingday: str
    ) -> bool:
        """
        check whether instrument is traded on tradingday

        Args:
            _instrument (str): which instrument.
            _tradingday (str): which day.

        Returns:
            bool: True for traded, False for not
        """
        client = MongoClient(host=self.mongo_host)
        db = client[self.mongo_inst_db]
        coll = db[_instrument.lower()]
        count = coll.count({'TradingDay': _tradingday})
        client.close()

        return count > 0

    def instrumentLastTradingDay(
            self, _instrument: str, _tradingday: str
    ) -> typing.Union[None, str]:
        """
        get the first day less then _tradingday of _instrument

        Args:
            _instrument (str): which instrument.
            _tradingday (str): which day.

        Returns:
            str: if None, it means nothing found
        """
        client = MongoClient(host=self.mongo_host)
        db = client[self.mongo_inst_db]
        coll = db[_instrument.lower()]
        d = coll.find_one(
            {'TradingDay': {'$lt': _tradingday}},
            sort=[('TradingDay', pymongo.DESCENDING)]
        )
        client.close()

        return d['TradingDay'] if d is not None else None

    def instrumentNextTradingDay(
            self, _instrument: str, _tradingday: str
    ) -> typing.Union[None, str]:
        """
        get the first day greater then _tradingday of _instrument

        Args:
            _instrument (str): which instrument.
            _tradingday (str): which day.

        Returns:
            str: if None, it means nothing found
        """
        client = MongoClient(host=self.mongo_host)
        db = client[self.mongo_inst_db]
        coll = db[_instrument.lower()]
        d = coll.find_one(
            {'TradingDay': {'$gt': _tradingday}},
            sort=[('TradingDay', pymongo.ASCENDING)]
        )
        client.close()

        return d['TradingDay'] if d is not None else None

    def fetchTradeInstrument(
            self, _product: str, _tradingday: str
    ) -> typing.List[str]:
        """
        fetch all traded insts of one product on tradingday

        Args:
            _product (str): which product.
            _tradingday (str): which day.

        Returns:
            list: list of str. if len() == 0, then no traded inst
        """
        client = MongoClient(host=self.mongo_host)
        db = client[self.mongo_prod_db]
        coll = db[_product.lower()]
        data = coll.find_one({'TradingDay': _tradingday})
        ret = []
        if data is not None:
            ret = data['InstrumentList']
        client.close()

        return ret

    def fetchDominant(
            self, _product: str, _tradingday: str
    ) -> typing.Union[None, str]:
        """
        fetch dominant instrument of one product on tradingday

        Args:
            _product (str): which product.
            _tradingday (str): which day.

        Returns:
            str: dominant instrument. if None, then no traded inst
        """
        client = MongoClient(host=self.mongo_host)
        db = client[self.mongo_prod_db]
        coll = db[_product.lower()]
        data = coll.find_one({'TradingDay': _tradingday})
        ret = None
        if data is not None:
            ret = data['Dominant']
        client.close()

        return ret

    def fetchSubDominant(
            self, _product: str, _tradingday: str
    ) -> typing.Union[None, str]:
        """
        fetch sub dominant instrument of one product on tradingday

        Args:
            _product (str): which product.
            _tradingday (str): which day.

        Returns:
            str: sub dominant instrument. if None, then no traded inst
        """
        client = MongoClient(host=self.mongo_host)
        db = client[self.mongo_prod_db]
        coll = db[_product.lower()]
        data = coll.find_one({'TradingDay': _tradingday})
        ret = None
        if data is not None:
            ret = data['SubDominant']
        client.close()

        return ret

    def cache2DataStruct(
            self, _inst: str, _tradingday: str, _index: str
    ) -> typing.Union[None, DataStruct]:
        f = h5py.File(self.cache_path, 'a')
        try:
            grp = f[_inst.lower() + '/' + _tradingday]
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
            self, _inst: str, _tradingday: str,
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
                _inst.lower() + '/' + _tradingday + '/' + c,
                (len(_datastruct),),
                dtype=dtype,
            )
            dataset[:] = _datastruct.data[c]
            dataset.attrs['type'] = t

            if 'timestamp' in t:
                _datastruct.float2datetime(c)

        f.close()

    def fetchSymbol(
            self, _tradingday: str,
            _product: str = None, _instrument: str = None,
            _product_index: bool = False, _sub_dominant: bool = False,
    ) -> typing.Union[None, str]:
        assert _product is not None or _instrument is not None

        if isinstance(_product, str):
            _product = _product.lower()
        if isinstance(_instrument, str):
            _instrument = _instrument.lower()

        # set inst to real instrument name
        inst = _instrument
        if _product is not None:
            if _product_index:
                if self.productIsTrade(_product, _tradingday):
                    return _product
            elif not _sub_dominant:
                inst = self.fetchDominant(_product, _tradingday)
            else:
                inst = self.fetchSubDominant(_product, _tradingday)

        # check instrument valid
        if inst is None or not self.instrumentIsTrade(
                inst, _tradingday):
            return None
        return inst

    def fetchData(
            self, _tradingday: str,
            _product: str = None, _instrument: str = None,
            _product_index: bool = False, _sub_dominant: bool = False,
            _cache=True, _index='HappenTime'
    ) -> typing.Union[None, DataStruct]:
        """

        :param _tradingday:
        :param _product:
        :param _instrument:
        :param _product_index:
        :param _sub_dominant:
        :param _index:
        :param _cache:
        :return:
        """
        if isinstance(_product, str):
            _product = _product.lower()
        if isinstance(_instrument, str):
            _instrument = _instrument.lower()

        inst: str = self.fetchSymbol(
            _tradingday, _product, _instrument,
            _product_index, _sub_dominant)
        if inst is None:
            return None

        if _cache:
            # if found in cache, then return
            ret = self.cache2DataStruct(
                inst, _tradingday, _index)
            if ret is not None:
                return ret

        # fetch from database
        con = psycopg2.connect(
            dbname=self.psql_dbname,
            host=self.psql_host,
            user=self.psql_user,
            password=self.psql_password,
        )
        cur = con.cursor()

        # get all column names
        cur.execute(
            "select column_name, data_type "
            "from information_schema.columns "
            "where table_name='" + inst.lower() + "'"
        )
        columns = []
        types = []
        for d in cur.fetchall():
            columns.append(d[0])
            types.append(d[1])

        # get all ticks
        cur.execute(
            "SELECT * FROM " + inst.lower() +
            " WHERE TradingDay='" + _tradingday +
            "' ORDER BY " + _index.lower()
        )
        datas = list(cur.fetchall())

        cur.close()
        con.close()

        # turn into datastruct
        datastruct = DataStruct(columns, _index.lower(), datas)

        if len(datastruct):
            if _cache:
                self.DataStruct2cache(
                    inst, _tradingday,
                    columns, types, datastruct
                )
            return datastruct
        else:
            return None

    def fetchDayData(
            self, _begin_day: str, _end_day: str = None,
            _instrument: str = None, _index: str = 'TradingDay'
    ) -> DataStruct:
        raise NotImplementedError()
