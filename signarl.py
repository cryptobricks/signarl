import asyncio
import json
import traceback

from base import BaseJob

from collections import defaultdict
from datetime import date, datetime
import loguru
import requests

from message import send_a_message

signarl_list = list()


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
            url = "https://signarl.com/api/model/signal/list?pageNum=1&pageSize=5&modelId=model_44843740513537256"
            response = requests.get(url=url)
            response = response.json()
            result_data = response.get("data")
            result_list = result_data.get("list")
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

                model_type_name = signarl.get("model_type_name")

                if model_type_name == "强空" or model_type_name == "强多":
                    signarl_list.append(generateTime)
                    information = {
                        "now": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "generateTime": datetime.fromtimestamp(generateTime / 1000).strftime("%Y-%m-%d %H:%M:%S"),
                        "model_type_name": model_type_name,
                        "text": text
                    }

                    information = json.dumps(information, ensure_ascii=False, indent=4)
                    send_a_message(information)

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
