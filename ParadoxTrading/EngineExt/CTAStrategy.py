import typing

from ParadoxTrading.Engine import StrategyAbstract


class ProductStength:
    def __init__(self, _product, _strength):
        self.product = _product
        self.strength = _strength

    def __repr__(self) -> str:
        return '{}:{}'.format(self.product, self.strength)


class CTAStrategy(StrategyAbstract):
    def addEvent(
            self, _symbol: str, _signal_type: int,
            _product: str = None, _strength: typing.Any = None
    ):
        assert _product is not None and _strength is not None

        super().addEvent(
            _symbol, _signal_type,
            _strength=ProductStength(
                _product, _strength
            )
        )
