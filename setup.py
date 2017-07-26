from distutils.core import setup

setup(
    name='ParadoxTrading',
    version='0.0.1',
    author='hantian.pang',
    packages=[
        'ParadoxTrading',
        'ParadoxTrading/Fetch',
        'ParadoxTrading/Chart',
        'ParadoxTrading/Engine',
        'ParadoxTrading/EngineExt',
        'ParadoxTrading/Utils',
        'ParadoxTrading/Indicator',
        'ParadoxTrading/Indicator/Bar',
        'ParadoxTrading/Indicator/General',
        'ParadoxTrading/Indicator/Stop',
        'ParadoxTrading/Performance',
    ],
    install_requires=[
        'numpy',
        'tabulate',
        'h5py',
        'psycopg2',
        'pymongo',
        'PyQt5',
        'PyQtChart',
        'arrow',
        'pandas', 'diskcache',
        # 'PyCTP'
    ],
    dependency_links=[
        # 'https://github.com/ppaanngggg/PyCTP/master#egg=PyCTP-0.1'
    ]
)
