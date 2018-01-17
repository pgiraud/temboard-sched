# temboard-sched

Minimal task scheduler.

## Installing

``` console
# python setup.py install
```

## Usage

``` python
import time
import logging

from temboardsched import taskmanager


@taskmanager.worker(pool_size=4)
def hello_world(sleep):
    time.sleep(sleep)
    return 'Slept for %ss' % sleep


def main():
    # setup a logger for debugging
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    logger.addHandler(logging.StreamHandler())

    # instanciate & start the task manager
    task_manager = taskmanager.TaskManager()
    task_manager.start()

    # execute hello_world function every 10 seconds
    taskmanager.schedule_task('hello_world', options={'sleep': 5},
                              redo_interval=10)


if __name__ == '__main__':
    main()
```
