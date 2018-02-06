import numpy as np
from TorchTSA.model import ARMAGARCHModel

from ParadoxTrading.Chart import Wizard
from ParadoxTrading.Fetch.ChineseFutures import FetchDominantIndex
from ParadoxTrading.Indicator import LogReturn
from ParadoxTrading.Indicator.TSA import ARMAGARCH

fetcher = FetchDominantIndex()

market = fetcher.fetchDayData('20100701', '20180101', 'rb')

returns = LogReturn().addMany(market).getAllData()
return_arr = np.array(returns['logreturn'])

arma_garch_model = ARMAGARCHModel()
arma_garch_model.fit(return_arr)
print(
    arma_garch_model.getPhis(), arma_garch_model.getThetas(),
    arma_garch_model.getAlphas(), arma_garch_model.getBetas(),
    arma_garch_model.getConst(), arma_garch_model.getMu()
)

arma_garch = ARMAGARCH().addMany(market).getAllData()
mean_arr = np.array(arma_garch['mean'])
std_arr = np.array(arma_garch['std'])
print(arma_garch)

wizard = Wizard()

mean_view = wizard.addView('mean')
mean_view.addLine('zero', arma_garch.index(), np.zeros_like(mean_arr))
mean_view.addLine('real', returns.index(), returns['logreturn'])
mean_view.addLine('mean', arma_garch.index(), mean_arr)
mean_view.addLine('up', arma_garch.index(), mean_arr + std_arr)
mean_view.addLine('down', arma_garch.index(), mean_arr - std_arr)

std_view = wizard.addView('std')
std_view.addLine('std', arma_garch.index(), std_arr)

wizard.show()
