import typing

import h5py
import psycopg2
import pymongo
from pymongo import MongoClient

from ParadoxTrading.Utils.DataStruct import DataStruct


class Fetch:
    mongo_host = 'localhost'
    mongo_prod_db = 'FutureProd'
    mongo_inst_db = 'FutureInst'

    pgsql_host = 'localhost'
    pgsql_tick_dbname = 'FutureTick'
    pgsql_min_dbname = 'FutureMin'
    pgsql_hour_dbname = 'FutureHour'
    pgsql_day_dbname = 'FutureDay'
    pgsql_user = 'pang'
    pgsql_password = ''

    cache_path = 'cache.hdf5'

    @staticmethod
    def productList() -> list:
        client = MongoClient(host=Fetch.mongo_host)
        db = client[Fetch.mongo_prod_db]
        ret = db.collection_names()
        client.close()

        return ret

    @staticmethod
    def productIsTrade(_product: str, _tradingday: str) -> bool:
        """
        check whether product is traded on tradingday

        Args:
            _product (str): which product.
            _tradingday (str): which day.

        Returns:
            bool: True for traded, False for not
        """
        client = MongoClient(host=Fetch.mongo_host)
        db = client[Fetch.mongo_prod_db]
        coll = db[_product]
        count = coll.count({'TradingDay': _tradingday})
        client.close()

        return count > 0

    @staticmethod
    def productLastTradingDay(_product: str, _tradingday: str
                              ) -> typing.Union[None, str]:
        """
        get the first day less then _tradingday of _product

        Args:
            _product (str): which product.
            _tradingday (str): which day.

        Returns:
            str: if None, it means nothing found
        """
        client = MongoClient(host=Fetch.mongo_host)
        db = client[Fetch.mongo_prod_db]
        coll = db[_product]
        d = coll.find_one(
            {'TradingDay': {'$lt': _tradingday}},
            sort=[('TradingDay', pymongo.DESCENDING)]
        )
        client.close()

        return d['TradingDay'] if d is not None else None

    @staticmethod
    def productNextTradingDay(
            _product: str, _tradingday: str
    ) -> typing.Union[None, str]:
        """
        get the first day greater then _tradingday of _product

        Args:
            _product (str): which product.
            _tradingday (str): which day.

        Returns:
            str: if None, it means nothing found
        """
        client = MongoClient(host=Fetch.mongo_host)
        db = client[Fetch.mongo_prod_db]
        coll = db[_product]
        d = coll.find_one(
            {'TradingDay': {'$gt': _tradingday}},
            sort=[('TradingDay', pymongo.ASCENDING)]
        )
        client.close()

        return d['TradingDay'] if d is not None else None

    @staticmethod
    def instrumentIsTrade(_instrument: str, _tradingday: str) -> bool:
        """
        check whether instrument is traded on tradingday

        Args:
            _instrument (str): which instrument.
            _tradingday (str): which day.

        Returns:
            bool: True for traded, False for not
        """
        client = MongoClient(host=Fetch.mongo_host)
        db = client[Fetch.mongo_inst_db]
        coll = db[_instrument]
        count = coll.count({'TradingDay': _tradingday})
        client.close()

        return count > 0

    @staticmethod
    def instrumentLastTradingDay(_instrument: str, _tradingday: str) -> \
            typing.Union[None, str]:
        """
        get the first day less then _tradingday of _instrument

        Args:
            _instrument (str): which instrument.
            _tradingday (str): which day.

        Returns:
            str: if None, it means nothing found
        """
        client = MongoClient(host=Fetch.mongo_host)
        db = client[Fetch.mongo_inst_db]
        coll = db[_instrument]
        d = coll.find_one(
            {'TradingDay': {'$lt': _tradingday}},
            sort=[('TradingDay', pymongo.DESCENDING)]
        )
        client.close()

        return d['TradingDay'] if d is not None else None

    @staticmethod
    def instrumentNextTradingDay(_instrument: str, _tradingday: str) -> \
            typing.Union[None, str]:
        """
        get the first day greater then _tradingday of _instrument

        Args:
            _instrument (str): which instrument.
            _tradingday (str): which day.

        Returns:
            str: if None, it means nothing found
        """
        client = MongoClient(host=Fetch.mongo_host)
        db = client[Fetch.mongo_inst_db]
        coll = db[_instrument]
        d = coll.find_one(
            {'TradingDay': {'$gt': _tradingday}},
            sort=[('TradingDay', pymongo.ASCENDING)]
        )
        client.close()

        return d['TradingDay'] if d is not None else None

    @staticmethod
    def fetchTradeInstrument(
            _product: str, _tradingday: str
    ) -> typing.List[str]:
        """
        fetch all traded insts of one product on tradingday

        Args:
            _product (str): which product.
            _tradingday (str): which day.

        Returns:
            list: list of str. if len() == 0, then no traded inst
        """
        client = MongoClient(host=Fetch.mongo_host)
        db = client[Fetch.mongo_prod_db]
        coll = db[_product]
        data = coll.find_one({'TradingDay': _tradingday})
        ret = []
        if data is not None:
            ret = data['InstrumentList']
        client.close()

        return ret

    @staticmethod
    def fetchDominant(
            _product: str, _tradingday: str
    ) -> typing.Union[None, str]:
        """
        fetch dominant instrument of one product on tradingday

        Args:
            _product (str): which product.
            _tradingday (str): which day.

        Returns:
            str: dominant instrument. if None, then no traded inst
        """
        client = MongoClient(host=Fetch.mongo_host)
        db = client[Fetch.mongo_prod_db]
        coll = db[_product]
        data = coll.find_one({'TradingDay': _tradingday})
        ret = None
        if data is not None:
            ret = data['Dominant']
        client.close()

        return ret

    @staticmethod
    def fetchSubDominant(
            _product: str, _tradingday: str
    ) -> typing.Union[None, str]:
        """
        fetch sub dominant instrument of one product on tradingday

        Args:
            _product (str): which product.
            _tradingday (str): which day.

        Returns:
            str: sub dominant instrument. if None, then no traded inst
        """
        client = MongoClient(host=Fetch.mongo_host)
        db = client[Fetch.mongo_prod_db]
        coll = db[_product]
        data = coll.find_one({'TradingDay': _tradingday})
        ret = None
        if data is not None:
            ret = data['SubDominant']
        client.close()

        return ret

    @staticmethod
    def cache2DataStruct(
            _type: str, _inst: str, _tradingday: str, _index: str
    ) -> typing.Union[None, DataStruct]:
        f = h5py.File(Fetch.cache_path, 'a')
        try:
            grp = f[_type + '/' + _inst + '/' + _tradingday]
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

    @staticmethod
    def DataStruct2cache(
            _type: str, _inst: str, _tradingday: str,
            _columns: typing.List[str], _types: typing.List[str],
            _datastruct: DataStruct
    ):
        f = h5py.File(Fetch.cache_path, 'a')

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
                _type + '/' + _inst + '/' + _tradingday + '/' + c,
                (len(_datastruct),),
                dtype=dtype,
            )
            dataset[:] = _datastruct.data[c]
            dataset.attrs['type'] = t

            if 'timestamp' in t:
                _datastruct.float2datetime(c)

        f.close()

    @staticmethod
    def fetchInstrument(
            _tradingday: str, _product: str = None, _instrument: str = None,
            _product_index: bool = False, _sub_dominant: bool = False,
    ) -> typing.Union[None, str]:
        assert _product is not None or _instrument is not None

        # set inst to real instrument name
        inst = _instrument
        if _product is not None:
            if _product_index:
                if Fetch.productIsTrade(_product, _tradingday):
                    return _product
            elif not _sub_dominant:
                inst = Fetch.fetchDominant(_product, _tradingday)
            else:
                inst = Fetch.fetchSubDominant(_product, _tradingday)

        # check instrument valid
        if inst is None or not Fetch.instrumentIsTrade(inst, _tradingday):
            return None
        return inst

    @staticmethod
    def fetchIntraDayData(
            _tradingday: str, _product: str = None, _instrument: str = None,
            _product_index: bool = False, _sub_dominant: bool = False,
            _data_type: str = 'FutureTick', _index: str = 'HappenTime',
            _cache: bool = True
    ) -> typing.Union[None, DataStruct]:
        """


        :param _tradingday:
        :param _product:
        :param _instrument:
        :param _product_index:
        :param _sub_dominant:
        :param _data_type:
        :param _index:
        :param _cache:
        :return:
        """

        inst: str = Fetch.fetchInstrument(
            _tradingday, _product, _instrument, _product_index, _sub_dominant)
        if inst is None:
            return None

        if _cache:
            # if found in cache, then return
            ret = Fetch.cache2DataStruct(_data_type, inst, _tradingday, _index)
            if ret is not None:
                return ret

        # fetch from database
        con = psycopg2.connect(
            dbname=_data_type,
            host=Fetch.pgsql_host,
            user=Fetch.pgsql_user,
            password=Fetch.pgsql_password,
        )
        cur = con.cursor()

        # get all column names
        cur.execute(
            "select column_name, data_type from information_schema.columns " +
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
        con.close()

        # turn into datastruct
        datastruct = DataStruct(columns, _index.lower(), datas)

        if len(datastruct):
            if _cache:
                Fetch.DataStruct2cache(
                    _data_type, inst, _tradingday, columns, types, datastruct)
            return datastruct
        else:
            return None

    @staticmethod
    def fetchInterDayData(
            _instrument: str, _begin_day: str, _end_day: str = None,
            _index: str = 'TradingDay'
    ) -> DataStruct:

        begin_day = _begin_day
        end_day = _end_day
        if _end_day is None:
            end_day = begin_day

        con = psycopg2.connect(
            dbname=Fetch.pgsql_day_dbname,
            host=Fetch.pgsql_host,
            user=Fetch.pgsql_user,
            password=Fetch.pgsql_password,
        )
        cur = con.cursor()

        # get all column names
        cur.execute(
            "select column_name, data_type from information_schema.columns " +
            "where table_name='" + _instrument.lower() + "'"
        )
        columns = [d[0] for d in cur.fetchall()]

        query = "select * from {} where {} >= '{}' and {} <= '{}'".format(
            _instrument.lower(),
            _index.lower(), begin_day,
            _index.lower(), end_day,
        )
        cur.execute(query)
        datas = list(cur.fetchall())
        con.close()

        return DataStruct(columns, _index.lower(), datas)
