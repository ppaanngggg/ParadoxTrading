from ParadoxTrading.Fetch.ChineseFutures import FetchDominantIndex
from ParadoxTrading.Indicator import EFF
from ParadoxTrading.Chart import Wizard

fetcher = FetchDominantIndex()
rb = fetcher.fetchDayData('20100101', '20170101', 'rb')

eff = EFF(20).addMany(rb).getAllData()

wizard = Wizard()

main_view = wizard.addView('main', _adaptive=True, _view_stretch=3)
main_view.addLine('price', rb.index(), rb['closeprice'])
sub_view = wizard.addView('sub')
sub_view.addLine('eff', eff.index(), eff['eff'])

wizard.show()
