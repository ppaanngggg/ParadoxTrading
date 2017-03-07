import numpy as np

from ParadoxTrading.Utils import DataStruct


def sharpRatio(
        _fund: DataStruct,
        _factor: int = 252,
        _unrisk_return_ratio: float = 0.0
) -> float:
    fund = _fund.getColumn('Fund')
    return_ratio = np.array(fund[1:]) / np.array(
        fund[:-1]) - 1.0 - _unrisk_return_ratio
    return float(
        np.mean(return_ratio) / np.std(return_ratio)
        * np.sqrt(_factor)
    )
