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
def sleep_worker(sleep):
    time.sleep(sleep)
    return 'Slept for %ss' % sleep


@taskmanager.bootstrap()
def sleep_bootstrap():
    yield taskmanager.Task(
                worker_name='sleep_worker',
                id='sleep_1',
                options={'sleep': 5},
                redo_interval=10
          )


def main():
    # setup a logger for debugging
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    logger.addHandler(logging.StreamHandler())

    # instanciate & start the task manager
    task_manager = taskmanager.TaskManager()
    task_manager.start()


if __name__ == '__main__':
    main()
```
