import asyncio
import inspect

from peewee import *
from peewee import MySQLDatabase
from playhouse.db_url import connect
import os
import loguru

# db = connect(os.environ.get('DATABASE') or 'sqlite:///default.db')
import os

from playhouse.pool import PooledPostgresqlExtDatabase, PooledMySQLDatabase




# cex_db = connect(settings.DATABASE_CEX_URL)

# cex_db = PooledMySQLDatabase(
#     'test',  # 数据库名
#     user='cb',  # 用户名
#     password='cryptoBricks123',  # 密码
#     host='0.0.0.0',  # 主机
#     port=3307,  # 端口
#     max_connections=32,
#     stale_timeout=300  # 5 minutes
# )

cex_db = PooledMySQLDatabase(
    'test',  # 数据库名
    user='cb',  # 用户名
    password='cryptoBricks123',  # 密码
    host='52.39.20.198',  # 主机
    port=3306,  # 端口
    max_connections=32,
    stale_timeout=300  # 5 minutes
)





class BaseJob:
    def __init__(self, *args, **kwargs):
        self.logger = loguru.logger.bind(name=self.__class__.__name__)
        self.init(*args, **kwargs)

    # def start_background_task(self):
    #     loop = asyncio.new_event_loop()
    #     loop.run_until_complete(self.run_background())

    def init(self):
        raise NotImplementedError

    def do_job(self):
        raise NotImplementedError

    def run(self):
        loop = asyncio.new_event_loop()

        # loop = asyncio.e()
        # asyncio.set_event_loop()
        # loop = asyncio.get_event_loop()
        # print(loop)
        loguru.logger.info(f"---->{self.__class__.__name__}")
        if inspect.iscoroutinefunction(self.do_job):
            # loguru.logger.info(f"------!")
            try:
                loop.run_until_complete(self.do_job())
                # asyncio.run(self.do_job())
            except Exception as e:
                self.report(e)
        else:
            try:
                self.do_job()
                # .run(self.do_job())
            except Exception as e:
                self.report(e)

    def report(self, e):
        loguru.logger.info(e)
        pass
