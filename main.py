from scrapy.cmdline import execute

from service.scheduleService import ScheduleService
from util.argparseUtil import ArgparseUtil
import redis

if __name__ == "__main__":
    arg = ArgparseUtil.invoke()
    if arg.debug:
        from util.serviceDebugUtil import ServiceDebugUtil

        ScheduleService.invokeDebug(ServiceDebugUtil.getAllService(), arg.sTime, arg.eTime, arg.uSec)
    else:
        ScheduleService.invoke()

    execute("scrapy crawlall".split())
