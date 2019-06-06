from apscheduler.schedulers.blocking import BlockingScheduler
from subprocess import call

sched = BlockingScheduler()

@sched.scheduled_job('interval', seconds=10)

def timed_job():
    call(["python", "get_wifi_data.py"])
    call(["python", "push_to_remote_db.py"])

sched.start()
