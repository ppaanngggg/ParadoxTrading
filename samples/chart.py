from ParadoxTrading.Performance import dailyReturn, sharpRatio, FetchRecord
from ParadoxTrading.Fetch import FetchGuoJinDay



signal_list = FetchRecord.fetchFillRecords('range_break', 'range_break')
dailyReturn