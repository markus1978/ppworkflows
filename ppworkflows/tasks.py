import logging
import shutil
import time
from collections import OrderedDict

import sys

from ppworkflows import Task

LOGGER = logging.getLogger(__name__)


class GeneratorTask(Task):
    """
    A seed task that will send the objects created by a given generator.
    """
    def __init__(self, generator, with_status=False, key=None):
        super().__init__()
        self._with_status = with_status
        self._generator = generator
        self._key = key
        if self._key is None and with_status:
            self._key = "data"

    def run_loop(self):
        for item in self._generator():
            self.put(item, key=self._key)
            if self._with_status:
                self.put((("items: {:7d}", 1), ), key="status")


class SimpleTask(Task):
    def __init__(self, work):
        super().__init__("SimpleTask::%s" % work.__name__)
        self._work = work

    def run_loop(self):
        self._work(self)


class StatusTask(Task):
    """
    A task that reads list of tuples and displays them as a status line. The first item of each tuple
    is used as a label and format string (e.g. "Count: {:7d}"), the second is interpreted as a
    numerical value. Values are accumulated. The task produces no output.
    """

    class StreamWithClear(object):
        """
        File wrapper that clears the status bar before writing and rewrites it afterwards.
        Should be used arrount stdout/stderr for regular output mixed with status output.
        """
        def __init__(self, file, status_task):
            self.file = file
            self._status_task = status_task

        def write(self, x):
            # Avoid print() second call (useless \n)
            if len(x.rstrip()) > 0:
                self.file.write("\r\033[K")
                self.file.write(x)
                self.file.write("\n")
                self._status_task.print_last_status()

        def flush(self):
            return getattr(self.file, "flush", lambda: None)()

    def __init__(self, status_out=sys.stdout):
        """
        :param status_out: A file like object that can handle terminal control characters (usually stdout, stderr).
                           Or None for no status.
        """
        super().__init__()
        self._overall_start_time = time.time()
        self._status_out = status_out

        self._data = OrderedDict()

        self._console_cols = shutil.get_terminal_size().columns
        self._last_status = None
        self._workflow = None
        self._queues = None

    def configure(self, workflow, input, output_queues, runner_count):
        super().configure(workflow, input, output_queues, runner_count)
        self._workflow = workflow

    def before(self):
        super().before()
        self._queues = list([(name, queue) for name, queue in self._workflow.queues().items()])
        LOGGER.info("Queues: %s" % ", ".join(["%s(%d)" % (name, queue.maxsize) for name, queue in self._queues]))

    def _queue_status(self):
        return ",".join(["X" if queue.full() else "O" if queue.empty() else "_" for _, queue in self._queues]) + " "

    def run_loop(self):
        for status in self.get_many(100, timeout=0.1, minimum=1):
            for field, value in status:
                old_value = self._data.get(field, 0)
                self._data[field] = old_value + value

        self._data['Time: {:.3f}'] = time.time() - self._overall_start_time
        self._data['Q: {}'] = self._queue_status()
        self.print_status(" ".join(self._data.keys()).format(*self._data.values()))

    def stop(self):
        super(StatusTask, self).stop()
        self.close_status()

    def print_status(self, status_str):
        if self._status_out is not None:
            self._last_status = status_str
            status_str = status_str[:self._console_cols]
            self._status_out.write("\rSTATUS   {}\033[K\b ".format(status_str))
            self._status_out.flush()

    def close_status(self):
        if self._status_out is not None:
            self._last_status = None
            self._status_out.write("\n")
            self._status_out.flush()

    def print_last_status(self):
        if self._last_status is not None:
            self.print_status(self._last_status)
