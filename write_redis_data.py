import asyncio
import json
import traceback

from sqlalchemy import select
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker

import aiohttp
import aioredis
import loguru

from demo import t_signal_table

REDIS_URL = "redis://127.0.0.1:6379/15"
pool = aioredis.ConnectionPool.from_url(REDIS_URL, max_connections=100000)
redis_client = aioredis.Redis(connection_pool=pool)

engine = create_async_engine(
    "mysql+aiomysql://cb:cryptoBricks123@cb-rds.cw5tnk9dgstt.us-west-2.rds.amazonaws.com:3306/da_test?charset=utf8mb4",
    pool_pre_ping=True,
    pool_recycle=180,
    pool_size=10,
)
async_session = sessionmaker(
    engine, expire_on_commit=False, class_=AsyncSession
)


async def write_redis_data():
    try:
        url = "https://signarl.com/api/model/signal/list"
        async with aiohttp.ClientSession() as session:
            async with session.get(url, ssl=False) as response:
                response_json = await response.json()
                result_data = response_json.get("data")
                result_list = result_data.get("list")
                for index, result in enumerate(result_list):
                    print(f"{index}: {result}")
                    signal_id = result["id"]
                    await redis_client.set(signal_id, str(result))
                    break
                print(result_list)
    except Exception as e:
        loguru.logger.exception(e)
        loguru.logger.error(traceback.format_exc())


async def verify_old_signal():
    try:
        # if result_list:
        async with async_session() as session:
            async with session.begin():
                query = t_signal_table.select()
                result = await session.execute(query)
                await session.commit()

                signals_dicts = [row._asdict() for row in result.all()]

        for signal in signals_dicts:
            signal_id = signal.get("signal_id")
            await redis_client.set(signal_id, str(signal))
        # id_list = [item['id'] for item in result_list]
        # filtered_result_list = [id for id in id_list if id not in signal_ids]
        # if filtered_result_list:
        #     extracted_records = [record for record in result_list if record['id'] in filtered_result_list]
        #     return extracted_records
    except Exception as e:
        loguru.logger.exception(e)
        loguru.logger.error(traceback.format_exc())
        raise e


async def main():
    result = await verify_old_signal()
    print(result)
    # key = "470111"
    # value = await redis_client.get(key)
    # await redis_client.close()
    #
    # if value is None:
    #     print(f"Key '{key}' does not exist in Redis.")
    # else:
    #     print(f"Key '{key}' exists in Redis, value: {value}")


if __name__ == '__main__':
    asyncio.run(main())
