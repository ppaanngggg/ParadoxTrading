import json
import typing

import psycopg2
import psycopg2.extensions
import pymongo
import pymongo.collection
import pymongo.database
from diskcache import Cache
from pymongo import MongoClient

from ParadoxTrading.Fetch import FetchAbstract, RegisterAbstract
from ParadoxTrading.Utils import DataStruct


class RegisterFutureTick(RegisterAbstract):
    def __init__(
            self, _product: str = None,
            _instrument: str = None,
            _sub_dominant: bool = False
    ):
        """
        Market Register is used to store market sub information,
        pre-processed data and strategies used it

        :param _product: reg which product, if not None, ignore instrument
        :param _instrument: reg which instrument
        :param _sub_dominant: only work when use product,
            false means using dominant inst,
            true means sub dominant one
        """
        super().__init__()
        assert _product is not None or _instrument is not None

        # market register info
        self.product = _product
        self.instrument = _instrument
        self.sub_dominant = _sub_dominant

    def toJson(self) -> str:
        """
        encode register info into json str

        :return: json str
        """
        return json.dumps((
            ('product', self.product),
            ('instrument', self.instrument),
            ('sub_dominant', self.sub_dominant),
        ))

    def toKwargs(self) -> dict:
        """
        turn self to dict, used by fetchSymbol()

        :return:
        """
        return {
            '_product': self.product,
            '_instrument': self.instrument,
            '_sub_dominant': self.sub_dominant,
        }

    @staticmethod
    def fromJson(_json_str: str) -> 'RegisterFutureTick':
        """
        create object from a json str

        :param _json_str: json str stores register info
        :return: market register object
        """
        data: typing.Dict[str, typing.Any] = dict(json.loads(_json_str))
        return RegisterFutureTick(
            data['product'],
            data['instrument'],
            data['sub_dominant'],
        )


class FetchFutureTick(FetchAbstract):
    def __init__(self):
        super().__init__()
        self.register_type: RegisterAbstract = RegisterFutureTick

        self.mongo_host: str = 'localhost'
        self.mongo_prod_db: str = 'FutureProd'
        self.mongo_inst_db: str = 'FutureInst'
        self.mongo_tradingday_db: str = 'FutureTradingDay'

        self.psql_host: str = 'localhost'
        self.psql_dbname: str = 'FutureTick'
        self.psql_user: str = ''
        self.psql_password: str = ''

        self.cache: Cache = Cache('cache')
        self.market_key = 'market_{}_{}'
        self.tradingday_key = 'tradingday_{}'
        self.prod_key = 'prod_{}_{}'
        self.inst_key = 'inst_{}_{}'

        self._mongo_client: MongoClient = None
        self._mongo_prod: pymongo.database.Database = None
        self._mongo_inst: pymongo.database.Database = None
        self._mongo_tradingday: pymongo.database.Database = None
        self._psql_con: psycopg2.extensions.connection = None
        self._psql_cur: psycopg2.extensions.cursor = None

        self.columns = [
            'tradingday', 'lastprice', 'highestprice', 'lowestprice',
            'volume', 'turnover', 'openinterest',
            'upperlimitprice', 'lowerlimitprice',
            'askprice', 'askvolume', 'bidprice', 'bidvolume',
            'averageprice', 'happentime'
        ]
        self.types = [
            'character', 'double precision', 'double precision',
            'double precision', 'integer', 'double precision',
            'double precision', 'double precision', 'double precision',
            'double precision', 'integer', 'double precision', 'integer',
            'double precision', 'timestamp without time zone'
        ]

    def _get_mongo_prod(self) -> pymongo.database.Database:
        if not self._mongo_prod:
            if not self._mongo_client:
                self._mongo_client: MongoClient = MongoClient(
                    host=self.mongo_host
                )
            self._mongo_prod: pymongo.database.Database = self._mongo_client[
                self.mongo_prod_db]
        return self._mongo_prod

    def _get_mongo_inst(self) -> pymongo.database.Database:
        if not self._mongo_inst:
            if not self._mongo_client:
                self._mongo_client: MongoClient = MongoClient(
                    host=self.mongo_host
                )
            self._mongo_inst: pymongo.database.Database = self._mongo_client[
                self.mongo_inst_db]
        return self._mongo_inst

    def _get_mongo_tradingday(self) -> pymongo.database.Database:
        if not self._mongo_tradingday:
            if not self._mongo_client:
                self._mongo_client: MongoClient = MongoClient(
                    host=self.mongo_host
                )
            self._mongo_tradingday: pymongo.database.Database = self._mongo_client[
                self.mongo_tradingday_db
            ]
        return self._mongo_tradingday

    def _get_psql_con_cur(self) -> typing.Tuple[
        psycopg2.extensions.connection, psycopg2.extensions.cursor
    ]:
        if not self._psql_con:
            self._psql_con: psycopg2.extensions.connection = psycopg2.connect(
                dbname=self.psql_dbname,
                host=self.psql_host,
                user=self.psql_user,
                password=self.psql_password,
            )
        if not self._psql_cur:
            self._psql_cur: psycopg2.extensions.cursor = self._psql_con.cursor()

        return self._psql_con, self._psql_cur

    def allProductList(self) -> list:
        """
        get all product list stored in mongo

        :return: list of product names
        """
        db = self._get_mongo_prod()
        return db.collection_names()

    def fetchTradeProduct(self, _tradingday: str) -> list:
        data = self.fetchTradingDayInfo(_tradingday)
        ret = []
        if data is not None:
            ret = data['ProductList']
        return ret

    def isTradingDay(self, _tradingday: str) -> bool:
        return self.fetchTradingDayInfo(
            _tradingday
        ) is not None

    def productIsTrade(
            self, _product: str, _tradingday: str
    ) -> bool:
        """
        check whether product is traded on tradingday
        """
        return self.fetchProductInfo(
            _product, _tradingday
        ) is not None

    def productLastTradingDay(
            self, _product: str, _tradingday: str
    ) -> typing.Union[None, str]:
        """
        get the first day less then _tradingday of _product
        """
        db = self._get_mongo_prod()
        coll = db[_product.lower()]
        d = coll.find_one(
            {'TradingDay': {'$lt': _tradingday}},
            sort=[('TradingDay', pymongo.DESCENDING)]
        )

        return d['TradingDay'] if d is not None else None

    def productNextTradingDay(
            self, _product: str, _tradingday: str
    ) -> typing.Union[None, str]:
        """
        get the first day greater then _tradingday of _product
        """
        db = self._get_mongo_prod()
        coll = db[_product.lower()]
        d = coll.find_one(
            {'TradingDay': {'$gt': _tradingday}},
            sort=[('TradingDay', pymongo.ASCENDING)]
        )

        return d['TradingDay'] if d is not None else None

    def instrumentIsTrade(
            self, _instrument: str, _tradingday: str
    ) -> bool:
        """
        check whether instrument is traded on tradingday
        """
        return self.fetchInstrumentInfo(
            _instrument, _tradingday
        ) is not None

    def instrumentLastTradingDay(
            self, _instrument: str, _tradingday: str
    ) -> typing.Union[None, str]:
        """
        get the first day less then _tradingday of _instrument
        """
        db = self._get_mongo_inst()
        coll = db[_instrument.lower()]
        d = coll.find_one(
            {'TradingDay': {'$lt': _tradingday}},
            sort=[('TradingDay', pymongo.DESCENDING)]
        )

        return d['TradingDay'] if d is not None else None

    def instrumentNextTradingDay(
            self, _instrument: str, _tradingday: str
    ) -> typing.Union[None, str]:
        """
        get the first day greater then _tradingday of _instrument
        """
        db = self._get_mongo_inst()
        coll = db[_instrument.lower()]
        d = coll.find_one(
            {'TradingDay': {'$gt': _tradingday}},
            sort=[('TradingDay', pymongo.ASCENDING)]
        )

        return d['TradingDay'] if d is not None else None

    def fetchTradeInstrument(
            self, _product: str, _tradingday: str
    ) -> typing.List[str]:
        """
        fetch all traded insts of one product on tradingday
        """
        data = self.fetchProductInfo(_product, _tradingday)
        ret = []
        if data is not None:
            ret = data['InstrumentList']
        return ret

    def fetchDominant(
            self, _product: str, _tradingday: str
    ) -> typing.Union[None, str]:
        """
        fetch dominant instrument of one product on tradingday
        """
        data = self.fetchProductInfo(_product, _tradingday)
        ret = None
        if data is not None:
            ret = data['Dominant']

        return ret

    def fetchSubDominant(
            self, _product: str, _tradingday: str
    ) -> typing.Union[None, str]:
        """
        fetch sub dominant instrument of one product on tradingday
        """
        data = self.fetchProductInfo(_product, _tradingday)
        ret = None
        if data is not None:
            ret = data['SubDominant']

        return ret

    def fetchTradingDayInfo(
            self, _tradingday: str
    ) -> typing.Union[None, typing.Dict]:
        key = self.tradingday_key.format(_tradingday)
        try:
            return self.cache[key]
        except KeyError:
            db = self._get_mongo_tradingday()
            coll = db.tradingday
            data = coll.find_one({'TradingDay': _tradingday})
            self.cache[key] = data
            return data

    def fetchProductInfo(
            self, _product: str, _tradingday: str
    ) -> typing.Union[None, typing.Dict]:
        product = _product.lower()
        key = self.prod_key.format(product, _tradingday)
        try:
            return self.cache[key]
        except KeyError:
            db = self._get_mongo_prod()
            coll = db[product]
            data = coll.find_one({'TradingDay': _tradingday})
            self.cache[key] = data
            return data

    def fetchInstrumentInfo(
            self, _instrument: str, _tradingday: str
    ) -> typing.Union[None, typing.Dict]:
        instrument = _instrument.lower()
        key = self.inst_key.format(instrument, _tradingday)
        try:
            return self.cache[key]
        except KeyError:
            db = self._get_mongo_inst()
            coll = db[instrument]
            data = coll.find_one({'TradingDay': _tradingday})
            self.cache[key] = data
            return data

    def fetchSymbol(
            self, _tradingday: str,
            _product: str = None, _instrument: str = None,
            _sub_dominant: bool = False,
    ) -> typing.Union[None, str]:
        """
        get symbol from database

        :param _tradingday: the tradingday
        :param _product: the product to fetch
        :param _instrument: the instrument to fetch, if _product is None, then use this
        :param _sub_dominant: whether to use sub dominant
        :return:
        """
        assert _product is not None or _instrument is not None

        if isinstance(_product, str):
            _product = _product.lower()
        if isinstance(_instrument, str):
            _instrument = _instrument.lower()

        # set inst to real instrument name
        inst = _instrument
        if _product is not None:
            if not _sub_dominant:
                inst = self.fetchDominant(_product, _tradingday)
            else:
                inst = self.fetchSubDominant(_product, _tradingday)

        # check instrument valid
        if inst is None or not self.instrumentIsTrade(
                inst, _tradingday):
            return None
        return inst

    def fetchData(
            self, _tradingday: str, _symbol: str,
            _cache=True, _index='HappenTime'
    ) -> typing.Union[None, DataStruct]:
        """

        :param _tradingday:
        :param _symbol:
        :param _cache: whether to cache by hdf5
        :param _index: use which column to index
        :return:
        """
        assert isinstance(_symbol, str)
        symbol = _symbol.lower()

        key = self.market_key.format(symbol, _tradingday)
        if _cache:
            try:
                return self.cache[key]
            except KeyError:
                pass

        # fetch from database
        con, cur = self._get_psql_con_cur()

        # get all ticks
        cur.execute(
            "SELECT * FROM {} WHERE TradingDay='{}' ORDER BY {}".format(
                symbol, _tradingday, _index.lower())
        )
        data = list(cur.fetchall())
        if len(data):
            data = DataStruct(self.columns, _index.lower(), data)
        else:
            data = None

        if _cache:
            self.cache[key] = data
        return data

    def fetchDayData(
            self, _begin_day: str, _end_day: str, _symbol: str, **kwargs
    ) -> DataStruct:
        pass


class RegisterFutureTickIndex(RegisterAbstract):
    def __init__(self, _product: str):
        """
        because it is index, there is only product as parameter

        :param _product:
        """
        super().__init__()

        self.product = _product

    def toJson(self) -> str:
        return json.dumps((
            ('product', self.product),
        ))

    def toKwargs(self) -> dict:
        return {
            '_product': self.product
        }

    @staticmethod
    def fromJson(_json_str: str) -> 'RegisterAbstract':
        data: typing.Dict[str, typing.Any] = dict(json.loads(_json_str))
        return RegisterFutureTickIndex(
            data['product']
        )


class FetchFutureTickIndex(FetchFutureTick):
    def __init__(self):
        super().__init__()

        self.register_type = RegisterFutureTickIndex

        self.columns = [
            'tradingday', 'lastprice', 'volume', 'openinterest', 'happentime'
        ]
        self.types = [
            'character', 'double precision', 'integer', 'double precision',
            'timestamp without time zone'
        ]

    def fetchSymbol(
            self, _tradingday: str,
            _product: str = None, _instrument: str = None,
            _sub_dominant: bool = False,
    ):
        assert _product is not None

        if self.productIsTrade(_product, _tradingday):
            return _product
        return None


class RegisterFutureMin(RegisterFutureTick):
    pass


class FetchFutureMin(FetchFutureTick):
    def __init__(self):
        super().__init__()

        self.register_type = RegisterFutureMin
        # reset path
        self.psql_dbname = 'FutureMin'

        self.columns = [
            'tradingday', 'openprice', 'highprice', 'lowprice', 'closeprice',
            'volume', 'turnover', 'openinterest',
            'presettlementprice', 'precloseprice', 'preopeninterest',
            'bartime', 'barendtime',
        ]
        self.types = [
            'character', 'double precision', 'double precision',
            'double precision', 'double precision', 'integer',
            'double precision', 'double precision',
            'double precision', 'double precision', 'double precision',
            'timestamp without time zone', 'timestamp without time zone',
        ]

    def fetchData(
            self, _tradingday: str, _symbol: str = None,
            _cache=True, _index='BarEndTime'
    ) -> typing.Union[None, DataStruct]:
        return super().fetchData(
            _tradingday, _symbol,
            _cache, _index
        )


class RegisterFutureMinIndex(RegisterFutureTickIndex):
    pass


class FetchFutureMinIndex(FetchFutureTickIndex):
    def __init__(self):
        super().__init__()

        self.register_type = RegisterFutureMinIndex

        self.psql_dbname = 'FutureMin'

        self.columns = [
            'tradingday', 'openprice', 'highprice', 'lowprice', 'closeprice',
            'volume', 'openinterest', 'bartime', 'barendtime',
        ]
        self.types = [
            'character', 'double precision', 'double precision',
            'double precision', 'double precision', 'integer',
            'double precision',
            'timestamp without time zone', 'timestamp without time zone',
        ]

    def fetchData(
            self, _tradingday: str, _symbol: str,
            _cache=True, _index='BarEndTime'
    ):
        return super().fetchData(
            _tradingday, _symbol,
            _cache, _index
        )


class RegisterFutureHour(RegisterFutureTick):
    pass


class FetchFutureHour(FetchFutureMin):
    def __init__(self):
        super().__init__()

        self.register_type = RegisterFutureHour
        # reset path
        self.psql_dbname = 'FutureHour'


class RegisterFutureHourIndex(RegisterFutureTickIndex):
    pass


class FetchFutureHourIndex(FetchFutureMinIndex):
    def __init__(self):
        super().__init__()

        self.register_type = RegisterFutureHourIndex

        self.psql_dbname = 'FutureHour'


class RegisterFutureDay(RegisterFutureTick):
    pass


class FetchFutureDay(FetchFutureTick):
    def __init__(self):
        super().__init__()
        self.psql_dbname = 'FutureDay'

        self.register_type = RegisterFutureDay

        self.columns = [
            'tradingday', 'openprice', 'highprice', 'lowprice', 'closeprice',
            'volume', 'turnover', 'openinterest',
            'presettlementprice', 'precloseprice', 'preopeninterest'
        ]
        del self.types

    def fetchData(
            self, _tradingday: str, _symbol: str,
            _cache=True, _index='TradingDay'
    ) -> typing.Union[None, DataStruct]:
        return super().fetchData(
            _tradingday, _symbol, _cache, _index
        )

    def fetchDayData(
            self, _begin_day: str, _end_day: str = None,
            _symbol: str = None, _index: str = 'TradingDay'
    ) -> DataStruct:
        begin_day = _begin_day
        end_day = _end_day
        if _end_day is None:
            end_day = begin_day

        con, cur = self._get_psql_con_cur()

        query = "select * from {} where {} >= '{}' and {} < '{}'".format(
            _symbol.lower(),
            _index.lower(), begin_day,
            _index.lower(), end_day,
        )
        cur.execute(query)
        datas = list(cur.fetchall())

        return DataStruct(self.columns, _index.lower(), datas)


class RegisterFutureDayIndex(RegisterFutureTickIndex):
    pass


class FetchFutureDayIndex(FetchFutureDay):
    def __init__(self):
        super().__init__()

        self.register_type = RegisterFutureDayIndex
        self.columns = [
            'tradingday', 'openprice', 'highprice', 'lowprice',
            'closeprice', 'volume', 'openinterest',
        ]

    def fetchSymbol(
            self, _tradingday: str,
            _product: str = None, _instrument: str = None,
            _sub_dominant: bool = False,
    ) -> typing.Union[None, str]:
        assert _product is not None

        if self.productIsTrade(_product, _tradingday):
            return _product
        return None
