# from datahub.hub.defillama.tvl_job import tvl_job
import tracemalloc

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor
from apscheduler.jobstores.memory import MemoryJobStore
import pytz
import loguru

from signarl import signarl

jobstores = {
    # 'default': MongoDBJobStore(collection='apschedule_job', database=CONF['MONGODB_CONFIG']['DB'], client=client)
    "default": MemoryJobStore()
}

executors = {"default": ThreadPoolExecutor(20), "processpool": ProcessPoolExecutor(3)}

job_defaults = {"coalesce": False, "max_instances": 3}

scheduler = BlockingScheduler(
    jobstores=jobstores,
    executors=executors,
    job_defaults=job_defaults,
    timezone=pytz.timezone("asia/Shanghai"),
)


tracemalloc.start()
scheduler.add_job(signarl.run, **signarl.get_scheduler())

scheduler.start()
