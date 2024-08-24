import asyncio
from datetime import datetime

import pandas as pd
import pytz
import requests
from databases import Database
from sqlalchemy import insert

from sqlalchemy import Column, BigInteger, String, Text, DateTime
from sqlalchemy.ext.declarative import declarative_base

from sqlalchemy import (
    MetaData,
    Text,
    JSON,
    text,
    ForeignKey,
    VARCHAR,
    TIMESTAMP,
    INTEGER, String, DATETIME,
)
from sqlalchemy.dialects.mysql import BIGINT
from sqlalchemy.schema import Table, Column
from enum import Enum

profile_meta = MetaData()

Base = declarative_base()

t_signal_table = Table(
    "signal_table",
    profile_meta,
    Column("id", BIGINT(), primary_key=True),
    Column("signal_id", BIGINT()),
    Column("modelId", VARCHAR(255, "utf8mb4_unicode_ci")),
    Column("generateTime", VARCHAR(255, "utf8mb4_unicode_ci")),
    Column("scoreType", VARCHAR(255, "utf8mb4_unicode_ci")),
    Column("text", Text()),
    Column("detail", JSON),
    Column("checked", VARCHAR(512, "utf8mb4_unicode_ci")),
    Column("scoreDetail", JSON),
    Column("longChecked", VARCHAR(255, "utf8mb4_unicode_ci")),
    Column("checkedTrans", VARCHAR(255, "utf8mb4_unicode_ci")),
    Column("symbol", VARCHAR(255, "utf8mb4_unicode_ci")),
    Column("model_name", VARCHAR(255, "utf8mb4_unicode_ci")),
    Column("model_type_name", VARCHAR(255, "utf8mb4_unicode_ci")),
    Column("process_rate", VARCHAR(255, "utf8mb4_unicode_ci")),
    Column("create_time", VARCHAR(255, "utf8mb4_unicode_ci")),
    Column("update_time", VARCHAR(255, "utf8mb4_unicode_ci")),
    Column("create_at", DateTime),
)


async def get_signal():
    DATABASE_URL = "cb:cryptoBricks123@cb-rds.cw5tnk9dgstt.us-west-2.rds.amazonaws.com:3306/da_test"
    # DATABASE_URL = "root:root@192.168.50.2:3306/test"
    db = Database(
        f"mysql+aiomysql://{DATABASE_URL}?charset=utf8mb4&min_size=5&max_size=20"
    )

    utc_time = datetime.utcnow().replace(tzinfo=pytz.utc)
    formatted_time = utc_time.strftime('%Y-%m-%d %H:%M:%S')
    timestamp = int(utc_time.timestamp() * 1000)

    await db.connect()

    new_list = []

    url = "https://signarl.com/api/model/signal/list"
    for i in range(1, 5):
        params = {
            'pageNum': f"{i}",
            'pageSize': 1000,
        }

        response = requests.get(url, params=params)
        data = response.json()  # 假设服务器返回的是JSON格式的数据
        data_list = data.get("data").get("list")

        for item in data_list:
            print(item)
            information = {
                "signal_id": item.get("id"),
                "modelId": item.get("modelId"),
                "generateTime": item.get("generateTime"),
                "scoreType": item.get("scoreType"),
                "text": item.get("text"),
                "detail": item.get("detail"),
                "checked": item.get("checked"),
                "scoreDetail": item.get("scoreDetail"),
                "longChecked": item.get("longChecked"),
                "checkedTrans": item.get("checkedTrans"),
                "symbol": item.get("symbol"),
                "model_name": item.get("model_name"),
                "model_type_name": item.get("model_type_name"),
                "process_rate": item.get("process_rate"),
                "create_time": timestamp,
                "update_time": timestamp,
                "create_at": formatted_time,
            }
            result = t_signal_table.insert().values(information)
            await db.execute(result)


async def main():
    await get_signal()


if __name__ == '__main__':
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(get_signal())
