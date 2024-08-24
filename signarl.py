import asyncio
import traceback
from datetime import datetime

import aiohttp
import pytz
from sqlalchemy import select
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker

from base import BaseJob

import loguru
import requests

from demo import t_signal_table
from message import send_a_message

signarl_list = list()

engine = create_async_engine(
    "mysql+aiomysql://root:root@192.168.50.2:3306/test?charset=utf8mb4",
    pool_pre_ping=True,
    pool_recycle=180,
    pool_size=10,
)
async_session = sessionmaker(
    engine, expire_on_commit=False, class_=AsyncSession
)


class GetSignarl(BaseJob):
    def init(self, dao=None):
        loguru.logger.info("Initializing signarl")

    def get_scheduler(self):

        return {
            "trigger": "cron",
            "second": "0",  # 指定秒数为0
            "minute": "*/5",  # 每15分钟触发一次
            "hour": "*",  # 任意小时
            "day": "*",  # 任意日期
            "month": "*",  # 任意月份
            "day_of_week": "*",  # 任意星期
            "timezone": "UTC",
            "misfire_grace_time": 600,
        }

    async def get_signarl_list(self):
        try:
            utc_time = datetime.utcnow().replace(tzinfo=pytz.utc)
            formatted_time = utc_time.strftime('%Y-%m-%d %H:%M:%S')
            timestamp = int(utc_time.timestamp() * 1000)

            url = "https://signarl.com/api/model/signal/list"
            async with aiohttp.ClientSession() as session:
                async with session.get(url, ssl=False) as response:
                    response_json = await response.json()
                    result_data = response_json.get("data")
                    result_list = result_data.get("list")

                    if result_list:
                        async with async_session() as session:
                            async with session.begin():
                                query = select(t_signal_table.c.signal_id)
                                result = await session.execute(query)
                                await session.commit()

                                signal_ids = result.scalars().all()
                                await session.close()

                        id_list = [item['id'] for item in result_list]
                        filtered_result_list = [id for id in id_list if id not in signal_ids]

                        if filtered_result_list:
                            extracted_records = [record for record in result_list if record['id'] in filtered_result_list]
                            for item in extracted_records:
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
                                async with async_session() as db_session:
                                    async with db_session.begin():
                                        query = t_signal_table.insert().values(information)
                                        await db_session.execute(query)
                                        await db_session.commit()
                                        await db_session.close()

            return result_list
        except Exception as e:
            loguru.logger.exception(traceback.format_exc())

    async def get_signarl(self):
        try:

            result_list = await self.get_signarl_list()

            for index, signarl in enumerate(result_list):
                generateTime = signarl.get("generateTime")

                if generateTime in signarl_list:
                    continue
                text = signarl.get("text")
                symbol = signarl.get("symbol")
                url = f"https://api.binance.com/api/v3/ticker/price?symbol={symbol}"
                proxies = {
                    'http': 'http://158.178.225.38:38080',
                    'https': 'http://158.178.225.38:38080',
                }
                price_result = requests.get(url=url, proxies=proxies).json()
                price = price_result["price"]
                text = text + "\n" + f"{symbol}价格：" + price

                model_type_name = signarl.get("model_type_name")

                if (model_type_name == "强空" or model_type_name == "强多" or
                        model_type_name == "超空" or model_type_name == "超多" or
                    model_type_name == "中空" or model_type_name == "中多"
                ):
                    signarl_list.append(generateTime)
                    send_a_message(text)

        except Exception as e:
            loguru.logger.exception(traceback.format_exc())

    async def do_job(self):
        loguru.logger.info("---->{signarl} start")
        await self.get_signarl()


signarl = GetSignarl()


async def main():
    signarl = GetSignarl()
    result = await signarl.get_signarl()


if __name__ == "__main__":
    asyncio.run(main())
