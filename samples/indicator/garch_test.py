import time

import numpy as np
from TorchTSA.model import IGARCHModel, ARMAGARCHModel
from arch import arch_model

from ParadoxTrading.Chart import Wizard
from ParadoxTrading.Fetch.ChineseFutures import FetchDominantIndex
from ParadoxTrading.Indicator import LogReturn
from ParadoxTrading.Indicator.TSA import GARCH

fetcher = FetchDominantIndex()

market = fetcher.fetchDayData('20100701', '20171201', 'cu')

returns = LogReturn().addMany(market).getAllData()
return_arr = np.array(returns['logreturn'])

am = arch_model(return_arr, mean='Zero')
start_time = time.time()
res = am.fit(disp='off', show_warning=False)
print('fitting time:', time.time() - start_time)
print(res.params)

igarch_model = IGARCHModel(_use_mu=False)
start_time = time.time()
igarch_model.fit(return_arr)
print('fitting time:', time.time() - start_time)
print(
    igarch_model.getAlphas(),
    igarch_model.getBetas(),
    igarch_model.getConst(),
)

arma_garch_model = ARMAGARCHModel()
arma_garch_model.fit(return_arr)
print(
    arma_garch_model.getPhis(), arma_garch_model.getThetas(),
    arma_garch_model.getAlphas(), arma_garch_model.getBetas(),
    arma_garch_model.getConst(), arma_garch_model.getMu()
)

start_time = time.time()
garch = GARCH().addMany(market).getAllData()
print('fitting time:', time.time() - start_time)

wizard = Wizard()

price_view = wizard.addView('price')
price_view.addLine(
    'market', market.index(), market['closeprice']
)

sub_view = wizard.addView('std')
sub_view.addLine(
    'predict', garch.index(), garch['predict']
)
sub_view.addLine(
    'total garch', returns.index(), res.conditional_volatility
)
sub_view.addLine(
    'total igarch', returns.index()[1:], igarch_model.getVolatility()
)

wizard.show()
