from apscheduler.schedulers.blocking import BlockingScheduler

sched = BlockingScheduler()

@sched.scheduled_job('cron', hour=3)
def scheduled_job():
    print('This job is run every weekday at 3am.')

sched.start()