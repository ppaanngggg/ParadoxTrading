import typing

from ParadoxTrading.Utils import DataStruct


class RegisterAbstract:
    def __init__(self):
        # strategies linked to this market register
        self.strategy_set: typing.Set[str] = set()

    def addStrategy(self, _strategy):
        """
        link strategy to self

        :param _strategy: strategy object (just use its name)
        :return:
        """
        self.strategy_set.add(_strategy.name)

    def toJson(self) -> str:
        """
        turn market register to json, as the key of market register

        :return:
        """
        raise NotImplementedError('toJson')

    def toKwargs(self) -> dict:
        """
        turn register to args as dict

        :return:
        """
        raise NotImplementedError('toKwargs')

    @staticmethod
    def fromJson(_json_str: str) -> 'RegisterAbstract':
        """
        create a register from json

        :param _json_str:
        :return:
        """
        raise NotImplementedError('fromJson')

    def __repr__(self):
        return 'Key:\n\t{}\nStrategy:\n\t{}'.format(
            self.toJson(), self.strategy_set
        )


class FetchAbstract:
    def __init__(self):
        self.register_type: RegisterAbstract = None

    def fetchSymbol(
            self, _tradingday: str, **kwargs
    ) -> typing.Union[None, str]:
        """
        fetch symbol from database
        for example, get the dominant instrument of product,
        !!! kwargs equals with register.toKwargs

        :param _tradingday:
        :param kwargs:
        :return:
        """
        raise NotImplementedError('fetchSymbol')

    def fetchData(
            self, _tradingday: str, _symbol: str, **kwargs
    ) -> typing.Union[None, DataStruct]:
        """
        get one day data from database

        :param _tradingday:
        :param _symbol:
        :param kwargs:
        :return:
        """
        raise NotImplementedError('fetchData')

    def fetchDayData(
            self, _begin_day: str, _end_day: str, _symbol: str, **kwargs
    ) -> DataStruct:
        """
        get several days' data from database

        :param _begin_day: the begin day, included
        :param _end_day: the end day, included
        :param _symbol:
        :param kwargs:
        :return:
        """
        raise NotImplementedError('fetchDayData')
