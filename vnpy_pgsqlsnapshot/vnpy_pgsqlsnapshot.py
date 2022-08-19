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
    updated_at timestamp(0),
    CONSTRAINT gw_account_unique UNIQUE (gateway_name, accountid)
)
'''

CREATE_ACCOUNT_SNAPSHOT_SCRIPT='''
create table if not exists public.vnpy_account_snapshot(
    id serial,
    gateway_name varchar(50),
    accountid varchar(50),
    balance float,
    frozen  float,
    created_at timestamp(0)
)
'''

CREATE_ACCOUNT_SNAPSHOT_PROCESS_SCRIPT='''
CREATE OR REPLACE FUNCTION public.process_vnpy_account_snap() RETURNS TRIGGER AS $_$
BEGIN

IF (TG_OP = 'INSERT') THEN
   INSERT INTO public.vnpy_account_snapshot (gateway_name,
                                      accountid,
                                      balance,
                                      frozen,
                                      created_at)
          VALUES (NEW.gateway_name,
                  NEW.accountid,
                  NEW.balance,
                  NEW.frozen,
                  NEW.updated_at);

ELSIF (TG_OP = 'UPDATE') THEN

   IF (NEW.balance != OLD.balance) or (NEW.frozen != OLD.frozen) THEN
        RAISE NOTICE 'Balance was changed';
        INSERT INTO public.vnpy_account_snapshot (gateway_name,
                                           accountid,
                                           balance,
                                           frozen,
                                           created_at)
               VALUES (NEW.gateway_name,
                       NEW.accountid,
                       NEW.balance,
                       NEW.frozen,
                       NEW.updated_at);
   ELSE
        RAISE NOTICE 'Nothing changed';
   END IF;
END IF;

RETURN NULL;

END;
$_$ LANGUAGE plpgsql
'''

CREATE_ACCOUNT_SNAP_TRIGGER='''
CREATE OR REPLACE TRIGGER vnpy_account_snap
AFTER INSERT OR UPDATE ON public.vnpy_account
FOR EACH ROW EXECUTE FUNCTION public.process_vnpy_account_snap();
'''

CREATE_ACCOUNT_SNAPSHOT_CREATED_AT_INDEX='''
create index if not exists vnpy_account_snapshot_created_at_idx 
        on public.vnpy_account_snapshot 
        using brin(created_at)
'''

SNAP_ACCOUNT_TABLE_SCRIPT='''
INSERT INTO public.vnpy_account
(gateway_name, accountid, balance, frozen, updated_at)
VALUES
(%(gateway_name)s, %(accountid)s, %(balance)s, %(frozen)s, CURRENT_TIMESTAMP)
ON CONFLICT (gateway_name, accountid) DO UPDATE
SET balance = excluded.balance,
frozen      = excluded.frozen,
updated_at = current_timestamp
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
    updated_at timestamp(0),
    CONSTRAINT position_gw_symbol_unique UNIQUE (gateway_name,symbol)
)
'''

CREATE_POSITION_SNAPSHOT_SCRIPT='''
create table if not exists public.vnpy_position_snapshot(
    id serial,
    gateway_name varchar(50),
    symbol   varchar(100),
    exchange varchar(100),
    direction varchar(5),
    volume    float,
    frozen    float,
    price     float,
    pnl       float,
    yd_volume float,
    created_at timestamp(0)
)
'''

CREATE_POSITION_SNAPSHOT_CREATED_AT_INDEX='''
create index if not exists vnpy_position_snapshot_created_at_idx 
        on public.vnpy_position_snapshot 
        using brin(created_at)
'''

SNAP_POSITION_TABLE_SCRIPT='''
INSERT INTO public.vnpy_position
(gateway_name, symbol, exchange, direction, volume, frozen, price, pnl, yd_volume, updated_at)
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
updated_at   = current_timestamp;
'''

CREATE_POSITION_SNAPSHOT_PROCESS_SCRIPT='''
CREATE OR REPLACE FUNCTION public.process_vnpy_position_snap() RETURNS TRIGGER AS $_$
BEGIN

IF (TG_OP = 'INSERT') THEN
   INSERT INTO public.vnpy_position_snapshot (gateway_name,
                                              symbol,
                                              exchange,
                                              direction,
                                              volume,
                                              frozen,
                                              price,
                                              pnl,
                                              yd_volume,
                                              created_at)
          VALUES (NEW.gateway_name,
                  NEW.symbol,
                  NEW.exchange,
                  NEW.direction,
                  NEW.volume,
                  NEW.frozen,
                  NEW.price,
                  NEW.pnl,
                  NEW.yd_volume,
                  NEW.updated_at);

ELSIF (TG_OP = 'UPDATE') THEN

   IF (NEW.volume != OLD.volume) or (NEW.price != OLD.price) or (NEW.pnl != OLD.pnl) THEN
        RAISE NOTICE 'New record changed';

        INSERT INTO public.vnpy_position_snapshot (gateway_name,
                                              symbol,
                                              exchange,
                                              direction,
                                              volume,
                                              frozen,
                                              price,
                                              pnl,
                                              yd_volume,
                                              created_at)
          VALUES (NEW.gateway_name,
                  NEW.symbol,
                  NEW.exchange,
                  NEW.direction,
                  NEW.volume,
                  NEW.frozen,
                  NEW.price,
                  NEW.pnl,
                  NEW.yd_volume,
                  NEW.updated_at);
   ELSE
        RAISE NOTICE 'Nothing changed';
   END IF;
END IF;

RETURN NULL;

END;
$_$ LANGUAGE plpgsql
'''

CREATE_POSITION_SNAP_TRIGGER='''
CREATE OR REPLACE TRIGGER vnpy_position_snap
AFTER INSERT OR UPDATE ON public.vnpy_position
FOR EACH ROW EXECUTE FUNCTION public.process_vnpy_position_snap();
'''

CREATE_TRADEDATA_TABLE_SCRIPT='''
create table if not exists public.vnpy_tradedata(
    gateway_name varchar(50),
    symbol varchar(100),
    exchange varchar(50),
    orderid varchar(100),
    tradeid varchar(100),
    direction  varchar(5),
    "offset"   text,
    price      float,
    volume     float,
    "datetime" timestamp(0),
    CONSTRAINT orderid_unique UNIQUE (orderid)
)
'''

SNAP_TRADEDATA_TABLE_SCRIPT='''
INSERT INTO public.vnpy_tradedata
(gateway_name, symbol, exchange, orderid, tradeid, direction, "offset", price, volume, "datetime")
VALUES
(%(gateway_name)s, %(symbol)s, %(exchange)s, %(orderid)s, %(tradeid)s, %(direction)s, %(offset)s, %(price)s, %(volume)s, current_timestamp)
'''

class PgsqlSnapshotEngine( BaseEngine ):
    def __init__(self, main_engine: MainEngine, event_engine: EventEngine) -> None:
        super(PgsqlSnapshotEngine, self).__init__(main_engine, event_engine, "PgsqlSnapshot")
        self.user: str = SETTINGS["database.user"]
        self.password: str = SETTINGS["database.password"]
        self.host: str = SETTINGS["database.host"]
        self.port: int = SETTINGS["database.port"]
        self.db: str = SETTINGS["database.database"]
        con_str = f"postgres://{self.user}:{self.password}@{self.host}:{self.port}/{self.db}"

        self.connection: psycopg2.connection = psycopg2.connect(con_str)
        self.cursor: psycopg2.cursor = self.connection.cursor()

        self.execute(CREATE_ACCOUNT_TABLE_SCRIPT)
        self.execute(CREATE_ACCOUNT_SNAPSHOT_SCRIPT)
        self.execute(CREATE_ACCOUNT_SNAPSHOT_CREATED_AT_INDEX)
        self.execute(CREATE_ACCOUNT_SNAPSHOT_PROCESS_SCRIPT)
        self.execute(CREATE_ACCOUNT_SNAP_TRIGGER)

        self.execute(CREATE_POSITION_TABLE_SCRIPT)
        self.execute(CREATE_POSITION_SNAPSHOT_SCRIPT)
        self.execute(CREATE_POSITION_SNAPSHOT_CREATED_AT_INDEX)
        self.execute(CREATE_POSITION_SNAPSHOT_PROCESS_SCRIPT)
        self.execute(CREATE_POSITION_SNAP_TRIGGER)

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
        d["offset"]  = trade.offset.value
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
        if query in { SNAP_ACCOUNT_TABLE_SCRIPT, 
                      SNAP_POSITION_TABLE_SCRIPT, 
                      SNAP_TRADEDATA_TABLE_SCRIPT }:
            self.cursor.executemany(query, data)
        else:
            self.cursor.execute(query, data)

        self.connection.commit()
       
