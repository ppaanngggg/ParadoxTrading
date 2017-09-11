import math
import random
import typing

import numpy as np

from ParadoxTrading.Utils.DataStruct import DataStruct


class CommoditySim:
    def __init__(
            self, _init_price=1000, _length=1000,
            _mu=0.0, _theta=0.05, _sigma=0.01,
            _alpha=2.7, _beta=3.1,
            _noise=0.005,
    ):
        # basic info
        self.index = 0
        self.length = _length
        self.value = 0.0
        self.rate = 0.0
        self.price = _init_price

        # trend process
        self.mu = _mu
        self.theta = _theta
        self.sigma = _sigma
        # return rate mask
        self.alpha = _alpha
        self.beta = _beta
        assert self.alpha > 1 and self.beta > 1
        self.mode = (self.alpha - 1) / (self.alpha + self.beta - 2)
        # white noise level
        self.noise = _noise

        # buf random values
        self.laplace_buf = np.random.laplace(
            size=self.length
        ).astype(np.float32) * self.sigma
        self.beta_buf = np.random.beta(
            self.alpha, self.beta, size=self.length
        ).astype(np.float32) - self.mode
        self.normal_buf = np.random.normal(
            size=self.length
        ).astype(np.float32) * self.noise

    @staticmethod
    def genParams() -> typing.Dict[str, float]:
        return {
            '_theta': 0.05 * math.exp(random.uniform(-0.5, 0.5)),
            '_sigma': random.uniform(0.005, 0.015),
            '_alpha': random.uniform(2.6, 2.8),
            '_beta': random.uniform(3.2, 3.4),
            '_noise': 0.005 * math.exp(random.uniform(-0.5, 0)),
        }

    def step(self):
        assert self.index < len(self.laplace_buf)

        # ou process, instead normal by laplace
        self.value += self.theta * (self.mu - self.value) + \
            self.laplace_buf[self.index]
        # masked by beta and add a normal white noise
        self.rate = self.value * self.beta_buf[self.index] + \
            self.normal_buf[self.index]
        # update price
        self.price *= 1 + self.rate

        tmp_index = self.index
        self.index += 1

        return tmp_index, self.value, self.rate, self.price

    def getAll(self) -> DataStruct:
        keys = ['index', 'value', 'rate', 'price']
        data = DataStruct(keys, 'index')
        for _ in range(self.length):
            tmp = self.step()
            data.addRow(tmp, keys)
        return data
