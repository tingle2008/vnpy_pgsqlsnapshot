# flake8: noqa

import psycopg2
from typing import Dict, List, Any
from vnpy.event import EventEngine
from vnpy.trader.engine import MainEngine,BaseEngine
from vnpy.trader.event import (
    EVENT_ORDER,
    EVENT_TRADE,
    EVENT_POSITION,
    EVENT_ACCOUNT
)

from vnpy.event import Event, EventEngine
from vnpy.trader.setting import SETTINGS

CREATE_ACCOUNT_TABLE_SCRIPT='''
create table if not exists public.vnpy_account(
    gateway_name varchar(50),
    accountid varchar(50),
    balance float,
    frozen  float,
    last_update_time timestamp(0),
    CONSTRAINT gw_account_unique UNIQUE (gateway_name, accountid)
)
'''

CREATE_POSITION_TABLE_SCRIPT='''
create table if not exists public.vnpy_position(
    gateway_name varchar(50),
    symbol   varchar(100),
    exchange varchar(100),
    direction varchar(5),
    volume    float,
    frozen    float,
    price     float,
    pnl       float,
    yd_volume float,
    last_update_time timestamp(0),
    CONSTRAINT position_gw_symbol_unique UNIQUE (gateway_name,symbol)
)
'''

CREATE_TRADEDATA_TABLE_SCRIPT='''
create table if not exists public.vnpy_tradedata(
    gateway_name varchar(50),
    symbol varchar(100),
    exchange varchar(50),
    orderid varchar(100),
    tradeid varchar(100),
    direction varchar(5),
    price     float,
    volume    float,
    "datetime" timestamp(0),
    CONSTRAINT orderid_unique UNIQUE (orderid)
)
'''

SNAP_TRADEDATA_TABLE_SCRIPT='''
INSERT INTO public.vnpy_tradedata
(gateway_name, symbol, exchange, orderid, tradeid, direction, price, volume, "datetime")
VALUES
(%(gateway_name)s, %(symbol)s, %(exchange)s, %(orderid)s, %(tradeid)s, %(direction)s, %(price)s, %(volume)s, current_timestamp)
'''

SNAP_ACCOUNT_TABLE_SCRIPT='''
INSERT INTO public.vnpy_account
(gateway_name, accountid, balance, frozen, last_update_time)
VALUES
(%(gateway_name)s, %(accountid)s, %(balance)s, %(frozen)s, now())
ON CONFLICT (gateway_name, accountid) DO UPDATE
SET balance = excluded.balance,
frozen      = excluded.frozen,
last_update_time = current_timestamp
'''

SNAP_POSITION_TABLE_SCRIPT='''
INSERT INTO public.vnpy_position
(gateway_name, symbol, exchange, direction, volume, frozen, price, pnl, yd_volume, last_update_time)
VALUES
(%(gateway_name)s, %(symbol)s, %(exchange)s, %(direction)s, %(volume)s, %(frozen)s, %(price)s, %(pnl)s, %(yd_volume)s, current_timestamp)
ON CONFLICT (gateway_name, symbol) DO UPDATE
SET exchange = excluded.exchange,
direction    = excluded.direction,
volume       = excluded.volume,
frozen       = excluded.frozen,
price        = excluded.price,
pnl          = excluded.pnl,
yd_volume    = excluded.yd_volume,
last_update_time = current_timestamp;
'''

class PgsqlSnapshotEngine( BaseEngine ):
    def __init__(self, main_engine: MainEngine, event_engine: EventEngine) -> None:
        super(PgsqlSnapshotEngine, self).__init__(main_engine, event_engine, "PgsqlSnapshot")
        self.user: str = SETTINGS["database.user"]
        self.password: str = SETTINGS["database.password"]
        self.host: str = SETTINGS["database.host"]
        self.port: int = SETTINGS["database.port"]
        self.db: str = SETTINGS["database.database"]

        self.connection: psycopg2.connection = psycopg2.connect(f"postgres://{self.user}:{self.password}@{self.host}:{self.port}/{self.db}")
        self.cursor: psycopg2.cursor = self.connection.cursor()
        self.execute(CREATE_ACCOUNT_TABLE_SCRIPT)
        self.execute(CREATE_POSITION_TABLE_SCRIPT)
        self.execute(CREATE_TRADEDATA_TABLE_SCRIPT)

        self.register_event()

    def add_function(self) -> None:
        self.main_engine.runit = self.runit

    # nothing just for add_function as a test.
    def run(self):
        print("running it")

    def register_event(self) -> None:
        self.event_engine.register(EVENT_TRADE, self.snap_trade_event)

    def snap_trade_event(self, event: Event) -> None:
        trade_data: List[dict] = []
        trade: TradeData = event.data
        d = {}
        d["gateway_name"] = trade.gateway_name
        d["symbol"] = trade.symbol
        d["exchange"] = trade.exchange.value
        d["orderid"]  = f"{trade.orderid}"
        d["tradeid"]  = f"{trade.tradeid}"
        d["direction"]  = trade.direction.value
        d["price"]  = trade.price
        d["volume"]  = trade.volume
        trade_data.append(d)
        self.execute(SNAP_TRADEDATA_TABLE_SCRIPT,trade_data)

    def save_account_data(self,accounts) -> None:
        account_data: List[dict] = []
        for account in accounts:
            d = {}
            d["gateway_name"] = account.gateway_name
            d["accountid"]   = account.accountid
            d["balance"]      = account.balance
            d["frozen"]       = account.frozen
            account_data.append(d)
        self.execute(SNAP_ACCOUNT_TABLE_SCRIPT,account_data)

    def save_position_data(self,positions) -> None:
        position_data: List[dict] = []
        for position in positions:
            d = {}
            d["gateway_name"] = position.gateway_name
            d["symbol"] = position.symbol
            d["exchange"] = position.exchange.value
            d["direction"] = position.direction.value
            d["volume"] = position.volume 
            d["frozen"] = position.frozen 
            d["price"] = position.price 
            d["pnl"] = position.pnl 
            d["yd_volume"] = position.yd_volume 
            position_data.append(d)
        self.execute(SNAP_POSITION_TABLE_SCRIPT,position_data)

    def execute(self, query: str, data: Any = None) -> None:
        """执行SQL查询"""
        if query in { SNAP_ACCOUNT_TABLE_SCRIPT, SNAP_POSITION_TABLE_SCRIPT, SNAP_TRADEDATA_TABLE_SCRIPT }:
            self.cursor.executemany(query, data)
        else:
            self.cursor.execute(query, data)

        self.connection.commit()
       
