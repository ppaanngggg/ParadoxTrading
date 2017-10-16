from distutils.core import setup

setup(
    name='ParadoxTrading',
    version='0.0.2',
    author='hantian.pang',
    packages=[
        'ParadoxTrading',
        'ParadoxTrading/Chart',
        'ParadoxTrading/Engine',
        'ParadoxTrading/EngineExt',
        'ParadoxTrading/EngineExt/Futures',
        'ParadoxTrading/EngineExt/Futures/Trend',
        'ParadoxTrading/EngineExt/Futures/Arbitrage',
        'ParadoxTrading/Fetch',
        'ParadoxTrading/Indicator',
        'ParadoxTrading/Indicator/Bar',
        'ParadoxTrading/Indicator/General',
        'ParadoxTrading/Indicator/Stop',
        'ParadoxTrading/Performance',
        'ParadoxTrading/Utils',
        'ParadoxTrading/Utils/CTP',
    ],
    install_requires=[
        'numpy', 'tabulate',
        'h5py',
        'psycopg2',
        'pymongo',
        'PyQt5', 'PyQtChart',
        'arrow',
        'pandas',
        'diskcache',
        'schedule',
        'requests', 'beautifulsoup4', 'urllib3', 'lxml'
        # 'PyCTP'
    ],
    dependency_links=[
        # 'https://github.com/ppaanngggg/PyCTP/master#egg=PyCTP-0.1'
    ]
)
