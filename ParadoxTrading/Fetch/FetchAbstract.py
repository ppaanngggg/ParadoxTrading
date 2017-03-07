import typing

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


class FetchAbstract:
    def fetchSymbol(
            self, _tradingday: str, **kwargs
    ) -> typing.Union[None, str]:
        raise NotImplementedError('fetchSymbol')

    def fetchData(
            self, _tradingday: str, **kwargs
    ) -> typing.Union[None, DataStruct]:
        raise NotImplementedError('fetchData')

    def fetchDayData(
            self, _begin_day: str, _end_day: str, **kwargs
    ) -> DataStruct:
        raise NotImplementedError('fetchDayData')
