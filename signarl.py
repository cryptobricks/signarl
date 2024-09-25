import asyncio
import hashlib
import json
import traceback
from datetime import datetime

import aiohttp
import aioredis
import pytz
from sqlalchemy import select
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker

from base import BaseJob

import loguru
import requests

from demo import t_signal_table
from error_message import send_error_a_message
from message import send_a_message

signarl_list = list()

engine = create_async_engine(
    "mysql+aiomysql://cb:cryptoBricks123@cb-rds.cw5tnk9dgstt.us-west-2.rds.amazonaws.com:3306/da_test?charset=utf8mb4",
    pool_pre_ping=True,
    pool_recycle=180,
    pool_size=10,
)
async_session = sessionmaker(
    engine, expire_on_commit=False, class_=AsyncSession
)

# 测试环境Redis
# REDIS_URL = "redis://10.244.4.140:6379"
# 生产环境redis
REDIS_URL = "redis://10.244.4.58:6379/15"
# 本地环境Redis
# REDIS_URL = "redis://127.0.0.1:6379/15"
pool = aioredis.ConnectionPool.from_url(REDIS_URL, max_connections=100000)
redis_client = aioredis.Redis(connection_pool=pool)


async def signal_entry_table(extracted_records):
    try:
        utc_time = datetime.utcnow().replace(tzinfo=pytz.utc)
        formatted_time = utc_time.strftime('%Y-%m-%d %H:%M:%S')
        timestamp = int(utc_time.timestamp() * 1000)

        if extracted_records:
            for item in extracted_records:
                signal_id = item["id"]
                await redis_client.set(signal_id, str(item))

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

    except Exception as e:
        loguru.logger.info(e)
        loguru.logger.error(traceback.format_exc())
        raise e


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
            url = "https://signarl.com/api/model/signal/list"
            async with aiohttp.ClientSession() as session:
                async with session.get(url, ssl=False) as response:
                    response_json = await response.json()
                    result_data = response_json.get("data")
                    result_list = result_data.get("list")

            return result_list
        except Exception as e:
            loguru.logger.exception(traceback.format_exc())
            raise e

    async def verify_old_signal(self, result_list):
        try:
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
                    return extracted_records
        except Exception as e:
            loguru.logger.exception(e)
            loguru.logger.error(traceback.format_exc())
            raise e

    async def sending_a_signal(self, extracted_records):
        try:
            utc_time = datetime.utcnow().replace(tzinfo=pytz.utc)
            formatted_time = utc_time.strftime('%Y-%m-%d %H:%M:%S')
            timestamp = int(utc_time.timestamp() * 1000)

            if extracted_records:
                for item in extracted_records:
                    detail = json.loads(item['detail'])
                    if detail:
                        open_price = detail['checked'].get('close')
                    else:
                        open_price = ""

                    body = {
                        "data": [
                            {
                                "symbol": item['symbol'],
                                "model_name": item['model_name'],
                                "model_type_name": item["model_type_name"],
                                "create_time": item['generateTime'],
                                "open_price": open_price,
                                "deep_low": item['scoreDetail'].get("min_depth"),
                                "deep_high": item['scoreDetail'].get("max_depth"),
                                "period_low": item['scoreDetail'].get("min_time"),
                                "period_high": item['scoreDetail'].get("max_time")
                            }
                        ]
                    }

                    body_json = json.dumps(body)
                    body_requests = body_json + str(timestamp) + 'n9jl2OU8SvHzRKrbGa10xtCM3Qc6V7gA'

                    input_bytes = body_requests.encode('utf-8')
                    md5 = hashlib.md5()
                    md5.update(input_bytes)
                    md5_digest = md5.hexdigest()

                    header = {
                        "push-timestamp": str(timestamp),
                        "push-sign": md5_digest
                    }

                    url = 'http://test2.admin.martingrid.com/sapiv2/pushAiSignal'
                    async with aiohttp.ClientSession() as session:
                        async with session.post(url, headers=header, data=body_json, ssl=False) as response:
                            response_json = await response.text()
                            response_json = json.loads(response_json)
                            if response_json == 200:
                                loguru.logger.info(response_json)
                            else:
                                loguru.logger.error(response_json)
        except Exception as e:
            send_error_a_message(traceback.format_exc())
            send_error_a_message(e)
            loguru.logger.info(e)
            loguru.logger.error(traceback.format_exc())

    async def sending_a_signal_to_tribe(self, extracted_records):
        try:
            utc_time = datetime.utcnow().replace(tzinfo=pytz.utc)
            formatted_time = utc_time.strftime('%Y-%m-%d %H:%M:%S')
            timestamp = int(utc_time.timestamp() * 1000)

            if extracted_records:
                for item in extracted_records:
                    detail = json.loads(item['detail'])
                    if detail:
                        open_price = detail['checked'].get('close')
                    else:
                        open_price = ""

                    body = {
                        "data": [
                            {
                                "symbol": item['symbol'],
                                "model_name": item['model_name'],
                                "model_type_name": item["model_type_name"],
                                "create_time": item['generateTime'],
                                "open_price": open_price,
                                "deep_low": item['scoreDetail'].get("min_depth"),
                                "deep_high": item['scoreDetail'].get("max_depth"),
                                "period_low": item['scoreDetail'].get("min_time"),
                                "period_high": item['scoreDetail'].get("max_time")
                            }
                        ]
                    }

                    body_json = json.dumps(body)

                    headers = {
                        'Content-Type': 'application/json'
                    }

                    url = 'https://uadcwf9ooa.execute-api.ap-southeast-2.amazonaws.com/Dev/btc-post-resource'
                    async with aiohttp.ClientSession() as session:
                        async with session.post(url, headers=headers, data=body_json, ssl=False) as response:
                            response_json = await response.text()
                            response_json = json.loads(response_json)
                            if response_json == 200:
                                loguru.logger.info(response_json)
                            else:
                                loguru.logger.error(response_json)
        except Exception as e:
            send_error_a_message(traceback.format_exc())
            send_error_a_message(e)
            loguru.logger.info(e)
            loguru.logger.error(traceback.format_exc())

    async def get_signarl(self):
        try:

            result_list = await self.get_signarl_list()

            old_signal = await self.verify_old_signal(result_list)
            await signal_entry_table(old_signal)

            if old_signal:
                try:
                    await self.sending_a_signal(old_signal)
                except Exception as e:
                    print(f"Error sending a signal: {e}")

                try:
                    await self.sending_a_signal_to_tribe(old_signal)
                except Exception as e:
                    print(f"Error sending a signal to tribe: {e}")

            for index, signarl in enumerate(result_list):
                signal_id = signarl.get('id')

                value = await redis_client.get(signal_id)
                await redis_client.close()

                if value:
                    break
                else:
                    generateTime = signarl.get("generateTime")

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
            send_error_a_message(e)
            send_error_a_message(traceback.format_exc())

    async def do_job(self):
        loguru.logger.info("---->{signarl} start")
        await self.get_signarl()


signarl = GetSignarl()


async def main():
    signarl = GetSignarl()
    result = await signarl.get_signarl_list()


if __name__ == "__main__":
    asyncio.run(main())
