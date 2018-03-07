import logging

from multiprocessing import Queue, Process, Value, Lock
from queue import Empty

LOGGER = logging.getLogger(__name__)


class Task(object):
    """
    The base class for tasks run via :class:`Workflow` instances. Each task is run in its own process(es).
    A task represents a specific type of work. Tasks ran be executed by multiple task runners that to
    the same type of work in parallel (hence multiple processes for one task).
    Each task runner reads from one input and can write to multiple outputs. Input and outputs are configured and
    controlled by the :class:`Workflow`.

    Clients have to implement :func:`run_loop` in order to do something and to read and write to and from
    queues. Client implementations do not read directly from a task/task runners input and output but use
    :func:`get_many`, :func:`get_one`, or :func:`get_available` to read and :func:`put` to write.
    Inputs behave additively for different tasks reading from the same queue (each task
    gets a copy of all objects written to the queue). Inputs behave alternatively for the task runners of
    the same task (each runner gets some of the objects to divide the work).
    Output objects behave additively and all objects written to the same queue by all the tasks and task
    runners are send to the queue.

    The basic task/task runner lifecycle is: :func:`__init__` (parent process), :func:`configure` (parent process),
    :func:`before` (task runner), :func:`run_loop` (task runner), :func:`stop` (task runner). All state changes done in
    the task runner, will not change the state of the instance in the parent process task (the object [and its state]
    will be cloned for each task runner). On stop, task runners will close all outputs, which will
    eventually close the queue, and will cause other tasks that depend on the output of this task to
    eventually stop. Therefore, the whole workflow lifecycle is 'controlled' from the first task(s) in the workflow.
    """
    def __init__(self, name=None):
        """
        :param name: An optional name for the task. Otherwise the class name will be used.
        """
        # share variables
        self._outputs = None
        self._input = None

        # parent variables
        self._processes = []

        # child variables
        self._stopped = False
        self._stop_cause = None
        self.process_number = -1

        # task name
        self._name = name if name is not None else type(self).__name__

    def configure(self, workflow, input, output_queues, runner_count):
        """
        Used by :class:`Workflow` to configure the task with input and output channels and to create the actual
        runner processes. Is executed in the parent process. Should not be overwritten.

        :param workflow: The workflow this task is configured for.
        :param input: The given input is used by the task to read from a queue. The given input is always additive,
        configure will create alternative inputs from this additive input for each task runner.
        :param output_queues: A list of queues to write to. Configure will create an additive output from the given
        queue for each task_runner.
        :param runner_count: The amount of runners configured by clients via :func:`Workflow.add_task`.
        """
        def run(process_number):
            self.process_number = process_number
            LOGGER.debug("Starting runner %s(%s)" % (self._name, self.process_number))
            self._run()
            LOGGER.debug("Completed runner %s(%s) and process can be joined now." % (self._name, self.process_number))

        while len(self._processes) < runner_count:
            if input is not None:
                self._input = input.add_or_input()
            self._outputs = {}
            for key in output_queues:
                self._outputs[key] = output_queues[key].add_output()
            self._processes.append(Process(target=run, kwargs=dict(process_number=len(self._processes))))

    def start(self):
        """
        Used by :class:`Workflow` to start the runner processes created in :func:`configure`. Should not be
        overwritten.
        """
        for process in self._processes:
            process.start()

    def get_many(self, count=1, timeout=None, minimum=0):
        """
        Returns a iterable of objects read from the input of a running task. Executed in a task runner process.
        Will block until enough objects are available. Will raise StopeIteration if not more objects are
        available and the queue has been closed.
        :param count: The number of objects to wait for.
        :param timeout: A timeout used to wait for an object. Will potentially return iterable with less than count items.
        :param minimum: A minimum amount of objects to return even with timeout.
        :return: An iterable of objects. Can be smaller than :param:`count` if the queue was closed.
        """
        assert minimum <= count
        self._check()
        values = []
        try:
            while len(values) < count:
                try:
                    values.append(self._input.get(timeout))
                except Empty:
                    if len(values) > minimum:
                        return values
        except StopIteration as e:
            self._stop_cause = e
            self._stopped = True
        finally:
            return values

    def get_available(self, minimum=1, timeout=None):
        """
        Returns an iterable with all available objects in the input queue of a running task. Exectued in a task
        runner processes. Will block for :param:`timeout` or until the :param:`minimum` of objects are available.
        Will raise StopIteration if not more objects are available and the queue has been closed.
        :param minimum: The minimum number to wait for.
        :param timeout: The timeout that should be used to decide if more objects are available or not.
        :return: An iterable of objects. Can be smaller than :param:`minimum` if the queue was closed.
        """
        return self.get_many(count=int('inf'), timeout=timeout, minimum=minimum)

    def get_one(self):
        """
        Returns the next object from the input queue. Executed in a task runner processes. Will block until an
        object is availabe or the queue was closed. Raises StopIteration if the queue was closed.
        :return: The object retrieved from the input queue.
        """
        self._check()
        try:
            return self._input.get()
        except StopIteration as e:
            self._stop_cause = e
            self._stopped = True
            raise e

    def _check(self):
        if self._stopped:
            raise self._stop_cause

    def _get_output_queue(self, key):
        if key is None:
            if len(self._outputs) == 1:
                _, queue = next(iter(self._outputs.items()))
                return queue
            else:
                raise KeyError
        else:
            return self._outputs[key]

    def put(self, value, key=None):
        """
        Allows to write object to an output queue.
        :param value: The object to put into the output queue.
        :param key: The key that identifies the output queue. Can be omitted if the task has only one possible output.
        """
        try:
            self._get_output_queue(key).put(value)
        except KeyError:
            pass

    def _run(self):
        self.before()

        try:
            if self._input is None:
                # for generator tasks that don't have inputs, run the loop once
                self._run_loop_save()
            else:
                # for all other tasks with input, run the loop indefinitely (it will raise StopIteration when input is closed)
                while True:
                    self._run_loop_save()
        except StopIteration:
            pass
        except KeyboardInterrupt as e:
            LOGGER.debug("Received keyboard interrupt in %s(%s). Stopping now..." % (self._name, self.process_number))
            self._stop_cause = e

        self.stop()

    def before(self):
        """
        Is called before the actual task is run via :func:`run_loop`. Is executed on the task runner and once
        for each runner. Can be overwritten.
        """
        pass

    def _run_loop_save(self):
        try:
            self.run_loop()
        except StopIteration as e:
            self._stop_cause = e
            self._stopped = True
            raise e
        except Exception as e:
            LOGGER.error("Error while running task %s(%s): %s" % (self._name, self.process_number, str(e)), exc_info=e)

    def run_loop(self):
        """
        Is called after the task has started and after :func:`before`. The task will end after this method returns.
        It should be run in a loop of continious reading from input and doing stuff. Should end after no more
        input is available after input queue was closed. Tasks without inputs (e.g. to generate seed objects) should
        also implement this, even though they are probably not run in a continious loop. After this method
        all output queues are closed and :func:`close` is called. Is executed in the runner processes. Must be
        overwritten.
        """
        raise NotImplemented

    def stop(self):
        """
        Is called after the task completed. Is executed in the runner process. Can be overwritten,
        but must be called at the end. Closes all output queues.
        Closing the queues will stop all processes reading from the queues.
        """
        natural_cause = isinstance(self._stop_cause, StopIteration)

        # report on stopping and stop reason
        if natural_cause:
            message = "Stopped process %s(%s) naturally." % (self._name, self.process_number)
            LOGGER.debug(message)
        else:
            message = "Stopped process %s(%s) because of %s:" % (self._name, self.process_number, str(self._stop_cause))
            LOGGER.debug(message)

        # clear all inputs, just in cause
        if self._input is not None:
            LOGGER.debug("Emptying input of %s(%s), just in case." % (self._name, self.process_number))
            self._input.empty()
            LOGGER.debug("Input of %s(%s) is empty." % (self._name, self.process_number))

        # closing all outputs and wait for them being emptied by other task runners
        LOGGER.debug("Closing all outputs of %s(%s)." % (self._name, self.process_number))
        for queue in self._outputs:
            LOGGER.debug("Closing output %s of %s(%s)." % (queue, self._name, self.process_number))
            self._get_output_queue(queue).close()

    def join(self):
        """
        Is called by the :class:`Workflow` in the parent process and will block until all task runner have completed
        and their outputs are emptied.
        """
        # do not try to join processes, when they are still related to unconsumed outputs.
        for queue in self._outputs:
            LOGGER.debug("Waiting for output %s of %s to be closed and emptied." % (queue, self._name))
            self._get_output_queue(queue).block_until_closed_and_emptied()
            LOGGER.debug("Output %s of %s has been emptied." % (queue, self._name))
        if len(self._outputs) > 0:
            LOGGER.debug("All outputs of %s have been emptied." % self._name)

        # now join the runner processes
        for process in self._processes:
            process.join()
            LOGGER.debug("Joined after a runner of %s completed." % self._name)
        LOGGER.debug("All runner of %s have been joined after completion." % self._name)


class _MultiQueue(object):
    """
    A class for queues that supports multiple inputs and outputs for multiple processes writing to, reading from
    can closing a queue.
    """
    end = "<THE END>"

    class Input(object):
        def __init__(self, queue, queue_lock):
            self._queue = queue
            self._queue_lock = queue_lock
            self._ors = Value("i", 0)

        def get(self, timeout=None):
            with self._ors.get_lock():
                if self._ors.value == 0:
                    raise StopIteration("Queue already closed and emptied.")
                if timeout is None:
                    value = self._queue.get(block=True)
                else:
                    value = self._queue.get(block=True, timeout=timeout)
                if value == _MultiQueue.end:
                    self._ors.value -= 1
                    if self._ors.value > 0:
                        self._queue.put(_MultiQueue.end)
                    else:
                        self._queue_lock.release()
                    raise StopIteration("MultiQueue.end received.")
                else:
                    return value

        def empty(self):
            """
            Purposefully empties the input queue.
            """
            while not self._queue_lock.acquire(block=False):
                while True:
                    try:
                        self.get(timeout=0.01)
                    except StopIteration:
                        break
                    except Empty:
                        break
                    except EOFError:
                        break
            self._queue_lock.release()

        def add_or_input(self):
            """
            :return: A new alternative (hence 'or') input based on this input.
            """
            with self._ors.get_lock():
                self._ors.value += 1
            return self

    class Output(object):
        def __init__(self, queue):
            self._queue = queue

        def put(self, obj):
            self._queue.put(obj)

        def close(self):
            self._queue.close()

        def block_until_closed_and_emptied(self):
            self._queue.block_until_closed_and_emptied()

    def __init__(self, name):
        self._name = name
        self._and_queues = []
        self._output_counter = Value('i', 0)
        self.maxsize = -1

    def configure(self, maxsize):
        """
        Allows to configure the max size of this queue. Outputting processes will be blocked if queue becomes
        to large. Must be called before processes are spawned, i.e. before :func:`workflow.run`.
        :param maxsize: The max size as integer.
        """
        self.maxsize = maxsize

    def add_and_input(self):
        """
        :return: A new additive (hence 'and') input for the queue.
        """
        queue = Queue(maxsize=self.maxsize)
        queue_lock = Lock()  # This is locked until the queue has been emptied after being closed
        queue_lock.acquire()
        self._and_queues.append((queue, queue_lock))
        return _MultiQueue.Input(queue, queue_lock)

    def add_output(self):
        """
        :return: A new additive output for the queue.
        """
        with self._output_counter.get_lock():
            self._output_counter.value += 1
            return _MultiQueue.Output(self)

    def put(self, obj):
        with self._output_counter.get_lock():
            if self._output_counter.value == 0:
                raise Exception("You cannot output to a closed queue.")
            else:
                for and_queue, _ in self._and_queues:
                    and_queue.put(obj)

    def close(self):
        with self._output_counter.get_lock():
            self._output_counter.value -= 1
            if self._output_counter.value == 0:
                LOGGER.debug("Closing queue (including all additive queues) %s." % self._name)
                for and_queue, _ in self._and_queues:
                    and_queue.put(_MultiQueue.end)
                    and_queue.close()
                    and_queue.join_thread()
                LOGGER.debug("Queue %s closed and queue thread joined." % self._name)

    def block_until_closed_and_emptied(self):
        for _, and_queue_lock in self._and_queues:
            and_queue_lock.acquire()
            and_queue_lock.release()

    def full(self):
        """
        :return: True if one of the additive queues is full.
        """
        for queue, _ in self._and_queues:
            if queue.full():
                return True
        return False

    def empty(self):
        """
        :return: True if one of the additive queues is empty.
        """
        for queue, _ in self._and_queues:
            if queue.empty():
                return True
        return False


class Workflow(object):
    """
    Allows to create workflows of parallel running tasks that communicate via queues. Usually there is one
    seed task that produces objects (write) and multiple worker task that process (read and write) or
    consume objects (just read). The workflow lifecylce is determined by the seed task. Tasks will
    close queues when no more objects are to be produces or processed. A closed input queue will then
    terminate each worker tasks that reads from it. The seed task terminates after it has done its work.
    Tasks are connected via queues, which provide the input and output channels for each tasks.

    We distinguish between the parent process that is used to configure and start the workflow and task runner
    processes that are used to run tasks.

    See :class:`Task` for more details on task lifecycle and communication.
    """
    def __init__(self):
        self._tasks = []
        self._queues = dict()

    def _get_queue(self, key, maxsize=-1):
        queue = self._queues.get(key, None)
        if queue is None:
            queue = _MultiQueue(key)
            self._queues[key] = queue
        if maxsize > 0:
            queue.configure(maxsize)
        return queue

    def add_task(self, task, input=None, outputs=None, runner_count=1, max_input_size=-1):
        """
        Can be used to add a task to the workflow. Executed in the parent process. Input and output queues are
        identified via unique names (keys). Tasks will be configured with
        inputs and outputs created at the corresponding queues.

        :param task: A task instance. Be aware that this instance will be cloned multiple times for tasks with
                     multiple runners.
        :param input: The name of the input queue.
        :param outputs: THe name of the output queues as an iterable.
        :param runner_count: The amount of task runner for this task.
        :param max_input_size: The maximum size of the input queue. This will block processes that provide the
        input for this task, if the queue becomes to large, i.e. input production is faster than consumption.
        """
        self._tasks.append(task)

        if input is None:
            task_input = None
        else:
            input_queue = self._get_queue(input, maxsize=max_input_size)
            task_input = input_queue.add_and_input()

        task_output_queues = dict()
        if outputs is not None:
            for output_queque_name in outputs:
                task_output_queues[output_queque_name] = self._get_queue(output_queque_name)

        task.configure(self, task_input, task_output_queues, runner_count)

    def queues(self):
        return self._queues

    def run(self):
        """
        Can be used to run the workflow after all tasks have been added with :func:`add_task`. Will block
        until all task runner processes have completed.
        """
        interrupted = False
        for task in self._tasks:
            task.start()
        for task in reversed(self._tasks):
            while True:
                try:
                    task.join()
                    break
                except KeyboardInterrupt as e:
                    if interrupted:
                        print("\nHelp!")
                        raise e
                    else:
                        interrupted = True
                        print("\nReceived keyboard interrupt, wait for all processes to stop.")
                        LOGGER.debug("Received keyboard interrupt in parent process.")
                        pass


_synchronize_locks = {}


def synchronized(func):
    """
    A decorator that can be used to synchronize concurrent calls from different processes, e.g.
    to sequentialize database accesses. The used lock will be specific for the decorated function.
    Hence it is not global and will only synchronize calls to the same function
    (but for all instances in case of a method).
    """
    lock = _synchronize_locks.get(func, None)
    if lock is None:
        lock = Lock()
        _synchronize_locks[func] = lock

    def wrapper(*args, **kwargs):
        lock.acquire()
        func(*args, **kwargs)
        lock.release()

    return wrapper
