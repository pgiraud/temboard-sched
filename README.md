# temboard-sched

Minimal task scheduler.

## Installing

``` console
# python setup.py install
```

## Usage

``` python
import time

from temboardsched import taskmanager


@taskmanager.worker(pool_size=4)
def sleep_worker(duration, message):
    time.sleep(duration)
    return 'I say %s' % message


@taskmanager.bootstrap()
def sleep_bootstrap(context):
    yield taskmanager.Task(
                worker_name='sleep_worker',
                id='sleep_1',
                options={
                    'duration': 5,
                    'message': context.get('message')
                },
                redo_interval=10
          )


def main():
    # Instanciate & start the task manager
    tm = taskmanager.TaskManager()
    tm.set_context('message', 'Hi!')
    tm.start()


if __name__ == '__main__':
    main()
```
