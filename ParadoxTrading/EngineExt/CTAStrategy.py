import typing

from ParadoxTrading.Engine import StrategyAbstract


class CTAStrategy(StrategyAbstract):
    def addEvent(
            self, _symbol: str, _signal_type: int,
            _product: str = None, _strength: typing.Any = None
    ):
        assert _product is not None and _strength is not None

        super().addEvent(
            _symbol, _signal_type,
            _strength={_product: _strength}
        )
