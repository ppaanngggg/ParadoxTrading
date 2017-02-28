"""

**ParadoxTrading基础工具**

- DataStruct是ParadoxTrading中支持的核心数据类型。
- Fetch从数据库中获取历史信息。
- Split讲历史数据按时间切分K线。

"""

from .DataStruct import DataStruct
from .Fetch import Fetch
from .Split import SplitIntoHour, SplitIntoMinute, SplitIntoSecond, \
    SplitAbstract
