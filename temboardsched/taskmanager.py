import sys
import time
import uuid
import logging
from select import select
from datetime import datetime, timedelta
from collections import deque
from multiprocessing import Process, Queue
from multiprocessing.connection import Listener, Client, AuthenticationError
from threading import Thread

try:
    from Queue import Empty
except ModuleNotFoundError:
    from queue import Empty

TM_WORKERS = []

# Message types
MSG_TYPE_TASK_NEW = 0
MSG_TYPE_TASK_STATUS = 1
MSG_TYPE_TASK_CANCEL = 2
MSG_TYPE_TASK_ABORT = 3
MSG_TYPE_TASK_LIST = 4
MSG_TYPE_RESP = 5
MSG_TYPE_ERROR = 6

# Task status
TASK_STATUS_DEFAULT = 1
TASK_STATUS_SCHEDULED = 2
TASK_STATUS_QUEUED = 4
TASK_STATUS_DOING = 8
TASK_STATUS_DONE = 16
TASK_STATUS_FAILED = 32
TASK_STATUS_CANCELED = 64
TASK_STATUS_ABORTED = 128
TASK_STATUS_ABORT = 256


def worker(pool_size=1):
    # Decorator that defines a new worker function
    def defines_worker(function):
        global TM_WORKERS
        TM_WORKERS.append({
            'name': function.__name__,
            'pool_size': pool_size,
            'module': function.__module__,
            'function': function.__name__
        })
        return function

    return defines_worker


def schedule_task(worker_name, options=None, start=datetime.utcnow(),
                  redo_interval=None, listener_addr='/tmp/temboardsched.sock',
                  authkey=None):
    return TaskManager.send_message(
                listener_addr,
                Message(
                    MSG_TYPE_TASK_NEW,
                    Task(
                        worker_name=worker_name,
                        options=options,
                        start_datetime=start,
                        redo_interval=redo_interval,
                    )
                ),
                authkey=authkey
           )


class Task(object):

    def __init__(self, worker_name=None, options=None, id=None,
                 status=TASK_STATUS_DEFAULT, start_datetime=None,
                 redo_interval=None):
        self.worker_name = worker_name
        self.options = options
        self.status = status
        self.start_datetime = start_datetime
        self.redo_interval = redo_interval
        self.stop_datetime = None
        self.id = id
        self.output = None

    def __repr__(self):
        return str(self.__dict__)


class Message(object):

    def __init__(self, type, content):
        self.type = type,
        self.content = content

    def __repr__(self):
        return str(self.__dict__)


class TaskList(object):

    def __init__(self, path):
        self.path = path
        self.tasks = dict()

    def save(self):
        pass

    def load(self):
        pass

    def push(self, t):
        # Add a new task to the list
        t.id = self._gen_task_id()
        self.tasks[t.id] = t
        return t.id

    def get(self, task_id):
        if task_id not in self.tasks:
            raise Exception("Task id=%s not found" % task_id)
        return self.tasks[task_id]

    def update(self, task_id, **kwargs):
        if task_id not in self.tasks:
            raise Exception("Task id=%s not found" % task_id)
        t = self.tasks[task_id]
        for k, v in kwargs.items():
            try:
                getattr(t, k)
            except AttributeError:
                raise Exception("Task attribute %s does not exist" % k)
            setattr(t, k, v)
        self.tasks[task_id] = t

    def rm(self, task_id):
        if task_id not in self.tasks:
            raise Exception("Task id=%s not found" % task_id)
        del(self.tasks[task_id])

    def _gen_task_id(self):
        id = None
        while id is None or id in self.tasks:
            id = str(uuid.uuid4())[0:8]
        return id


class TaskManager(object):

    def __init__(self, address='/tmp/temboardsched.sock', task_path=None,
                 authkey=None):

        self.scheduler = Scheduler(address, task_path, authkey)
        self.worker_pool = WorkerPool(
                                self.scheduler.task_queue,
                                self.scheduler.event_queue,
                           )
        self.scheduler.logger = logging.getLogger()
        self.worker_pool.logger = logging.getLogger()

    def start(self):
        self.scheduler.start()
        self.worker_pool.start()

    @staticmethod
    def send_message(address, message, authkey=''):
        conn = Client(address, authkey=authkey)
        conn.send(message)
        res = conn.recv()
        conn.close()
        return res


class Scheduler(object):

    def __init__(self, address, task_path, authkey):
        # Process objet from multiprocessing
        self.process = None
        # Listener for TM -> Scheduler IPC
        self.listener = Listener(address, authkey=authkey)
        self.logger = None
        # Queue used to send Task orders (start, stop) to WorkerPool
        self.task_queue = Queue()
        # Queue used to notify Scheduler about Task status
        self.event_queue = Queue()
        self.task_list = TaskList(task_path)

    def handle_listener_message(self):
        # read message from the listener
        try:
            conn = self.listener.accept()
        except AuthenticationError:
            self.logger.debug("SCHED: authent. error")
            return

        try:
            message = conn.recv()
        except Exception as e:
            conn.close()
            self.logger.error("SCHED: Unable to read the message")
            self.logger.debug("SCHED: %s" % str(e))
            return

        message.type = message.type[0]
        self.logger.debug("SCHED: received Message=%s" % message)

        # handle incoming message and return a response
        res = self.handle_message(message)
        conn.send(res)
        conn.close()

    def handle_event_queue_message(self):
        # read message from the event queue
        try:
            message = self.event_queue.get(False)
        except Empty:
            # should not happen
            self.logger.debug("SCHED: event queue empty")
            return

        message.type = message.type[0]
        self.logger.debug("SCHED: received Message=%s" % message)

        # handle incoming message
        self.handle_message(message)

    def run(self):
        timeout = 1
        t = timeout
        date_end = float(time.time()) + timeout
        while True:
            # scheduler main loop

            self.logger.debug("SCHED: select on Listener with t=%s" % t)

            # wait for I/O on Listener and event Queue
            (fds, _, _) = select([self.listener._listener._socket.fileno(),
                                  self.event_queue._reader.fileno()],
                                 [], [], t)
            if len(fds) > 0:

                for fd in fds:
                    if fd == self.listener._listener._socket.fileno():
                        self.handle_listener_message()
                    elif fd == self.event_queue._reader.fileno():
                        self.handle_event_queue_message()

                # we need to change select timeout value
                self.logger.debug("SCHED: date_end=%s, float(time.time())=%s"
                                  % (date_end, float(time.time())))
                t = date_end - float(time.time())
                if t < 0:
                    t = timeout
                    date_end = float(time.time()) + timeout
            else:
                # we are here every 1 second
                self.logger.debug('SCHED: schedule')
                # schedule Tasks & maintain Tasks list
                self.schedule()

                # reinit select timeout value
                t = timeout
                date_end = float(time.time()) + timeout

    def schedule(self):
        now = datetime.utcnow()
        remove_list = []
        for task_id, t in self.task_list.tasks.items():

            start = t.start_datetime
            redo = t.redo_interval

            if start < now and t.status & TASK_STATUS_DEFAULT:
                # new task
                t.status = TASK_STATUS_SCHEDULED
                self.task_list.tasks[task_id] = t
                self.logger.debug('SCHED: Pushing to WorkerPool Task=%s'
                                  % t)
                self.task_queue.put(t, False)
                continue

            if (redo and start + timedelta(seconds=redo) < now
                    and t.status & (TASK_STATUS_DONE | TASK_STATUS_FAILED |
                                    TASK_STATUS_ABORTED)):
                # redo task
                # update task attributes
                t.status = TASK_STATUS_SCHEDULED
                t.start_datetime = now
                t.stop_datetime = None
                t.output = None
                self.task_list.tasks[task_id] = t
                # push the task to the worker pool
                self.task_queue.put(t, False)
                continue

            if (not redo and t.stop_datetime and
                    t.stop_datetime + timedelta(seconds=3600) < now and
                    t.status & (TASK_STATUS_DONE | TASK_STATUS_FAILED |
                                TASK_STATUS_ABORTED |
                                TASK_STATUS_CANCELED)):
                self.logger.debug('SCHED: Task to remove Task=%s' % (t))
                # remove old tasks
                remove_list.append(t.id)
                continue

        for task_id in remove_list:
                self.task_list.rm(task_id)

    def handle_message(self, message):
        if message.type == MSG_TYPE_TASK_NEW:
            # New task
            task_id = self.task_list.push(message.content)
            return Message(MSG_TYPE_RESP, {'id': task_id})

        elif message.type == MSG_TYPE_TASK_STATUS:
            # task status update
            status = message.content['status']
            # special case when task's status is TASK_STATUS_CANCELD, we dont'
            # want to change it's state.
            t = self.task_list.get(message.content['task_id'])
            if t.status & TASK_STATUS_CANCELED:
                status = t.status

            self.task_list.update(
                    t.id,
                    status=status,
                    output=message.content.get('output', None),
                    stop_datetime=message.content.get('stop_datetime', None),
            )
            return Message(MSG_TYPE_RESP, {'id': t.id})

        elif message.type == MSG_TYPE_TASK_LIST:
            # task list
            task_list = [t.__dict__ for task_id, t
                         in self.task_list.tasks.items()]
            return sorted(task_list, key=lambda k: k['start_datetime'])

        elif message.type == MSG_TYPE_TASK_ABORT:
            # task abortation
            t = Task(id=message.content['task_id'],
                     status=TASK_STATUS_ABORT)
            self.task_queue.put(t)

        elif message.type == MSG_TYPE_TASK_CANCEL:
            # task cancellation
            # first, we need to change its status
            self.task_list.update(
                    message.content['task_id'],
                    status=TASK_STATUS_CANCELED,
            )
            # send the cancelation order to WP
            t = Task(id=message.content['task_id'],
                     status=TASK_STATUS_CANCELED)
            self.task_queue.put(t)

        # TODO: handle other message types

    def start(self):
        self.process = Process(target=self.run, args=())
        self.process.start()

    def stop(self):
        self.process.terminate()
        self.process.join()


class WorkerPool(object):

    def __init__(self, task_queue, event_queue):
        self.thread = None
        self.logger = None
        self.task_queue = task_queue
        self.event_queue = event_queue
        self.workers = {}
        global TM_WORKERS
        for worker in TM_WORKERS:
            self.workers[worker['name']] = {
                'queue': deque(),
                'pool_size': worker['pool_size'],
                'module': worker['module'],
                'function': worker['function'],
                'pool': []
            }

    def _abort_job(self, task_id):
        for workername in self.workers:
            for job in self.workers[workername]['pool']:
                if job['id'] == task_id:
                    self.logger.debug("WORKP: process pid=%s is going to be "
                                      "killed." % job['process'])
                    job['process'].terminate()
                    return True
        return False

    def _rm_task_worker_queue(self, task_id):
        for workername in self.workers:
            for t in self.workers[workername]['queue']:
                if t.id == task_id:
                    self.workers[workername]['queue'].remove(t)
                    self.logger.debug("WORKP: Task id=%s removed from queue."
                                      % t.id)
                    return True
        return False

    def run(self):
        while True:
            try:
                t = self.task_queue.get(timeout=0.1)
                self.logger.debug("WORKP: Task=%s" % t)
                if t.status & TASK_STATUS_SCHEDULED:
                    self.logger.debug("WORKP: add task to queue")
                    self.workers[t.worker_name]['queue'].appendleft(t)
                    # Update task status
                    self.event_queue.put(
                        Message(
                            MSG_TYPE_TASK_STATUS,
                            {
                                'task_id': t.id,
                                'status': TASK_STATUS_QUEUED,
                            }
                        )
                    )
                if t.status & TASK_STATUS_ABORT:
                    self._abort_job(t.id)
                if t.status & TASK_STATUS_CANCELED:
                    # Task cancellation includes 2 things:
                    # - remove the task from workers queue if present (job not
                    # yet running)
                    # - abort running job executing the task if any

                    # Looking up into workers jobs first, there is more chance
                    # to find the task here.
                    if not self._abort_job(t.id):
                        # If not aborted, task has been queued
                        self._rm_task_worker_queue(t.id)
            except Empty:
                pass
            # check running jobs state
            self.check_jobs()
            # start new jobs
            self.start_jobs()

    def exec_worker(self, module, function, out, *args, **kws):
        # Function wrapper around worker function
        try:
            res = getattr(sys.modules[module], function)(*args, **kws)
            # Put function result into output queue as a Message
            out.put(Message(MSG_TYPE_RESP, res))
        except Exception as e:
            out.put(Message(MSG_TYPE_ERROR, e))

    def start_jobs(self):
        # Execute Tasks
        for name, worker in self.workers.items():
            while len(self.workers[name]['pool']) < worker['pool_size']:
                try:
                    t = worker['queue'].pop()
                    self.logger.debug("WORKP: start new worker %s.%s"
                                      % (worker['module'], worker['function']))
                    # Queue used to get worker function return
                    out = Queue()
                    p = Process(
                            target=self.exec_worker,
                            args=(worker['module'], worker['function'], out),
                            kwargs=t.options,
                        )
                    p.start()
                    self.workers[name]['pool'].append(
                            {'id': t.id, 'process': p, 'out': out}
                    )
                    # Update task status
                    self.event_queue.put(
                        Message(
                            MSG_TYPE_TASK_STATUS,
                            {
                                'task_id': t.id,
                                'status': TASK_STATUS_DOING,
                            }
                        )
                    )
                except IndexError:
                    break

    def check_jobs(self):
        # Check jobs process state for each worker
        for name, worker in self.workers.items():
            for job in worker['pool']:
                if not job['process'].is_alive():
                    # Dead process case
                    self.logger.debug("WORKP: Job id=%s is dead" % job['id'])
                    try:
                        # Fetch the message from job's output queue
                        message_out = job['out'].get(False)
                    except Empty:
                        message_out = None
                    self.logger.debug("WORKP: Job output Message=%s"
                                      % message_out)
                    # Close job's output queue
                    job['out'].close()
                    # join the process
                    job['process'].join()

                    # Let's build the message we'll have to send to scheduler
                    # for the update of task's status.
                    task_stop_dt = datetime.utcnow()
                    if job['process'].exitcode == 0:
                        if message_out and \
                                message_out.type[0] == MSG_TYPE_RESP:
                            task_status = TASK_STATUS_DONE
                        else:
                            # when an exception is raised from the worker
                            # function
                            task_status = TASK_STATUS_FAILED
                    elif job['process'].exitcode < 0:
                        # process killed
                        task_status = TASK_STATUS_ABORTED
                    else:
                        task_status = TASK_STATUS_FAILED
                    task_output = None
                    if message_out:
                        task_output = message_out.content

                    # Update task status
                    self.event_queue.put(
                        Message(
                            MSG_TYPE_TASK_STATUS,
                            {
                                'task_id': job['id'],
                                'status': task_status,
                                'output': task_output,
                                'stop_datetime': task_stop_dt
                            }
                        )
                    )

                    # Finally, remove the job from the pool
                    self.workers[name]['pool'].remove(job)

    def start(self):
        self.thread = Thread(target=self.run, args=())
        self.thread.start()

    def stop(self):
        self.thread.join()
