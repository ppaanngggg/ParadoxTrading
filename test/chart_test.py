from ParadoxTrading.Chart import Wizard
from ParadoxTrading.Fetch import FetchSHFEDay

fetcher = FetchSHFEDay()
data = fetcher.fetchDayData('20170101', '20170401', 'rb1705')
print(data)

buy_time_list = [data.index()[i] for i in range(0, len(data), 5)]
buy_price_list = [data['closeprice'][i] for i in range(0, len(data), 5)]

wizard = Wizard('test')

view_price = wizard.addView('price', 3)
wizard.addLine(view_price, data.index(), data['closeprice'], 'closeprice')
wizard.addScatter(view_price, buy_time_list, buy_price_list, 'buy', 'red')
view_volume = wizard.addView('volume')
wizard.addBar(view_volume, data.index(), data['volume'], 'volume', 'orange')

wizard.show()
