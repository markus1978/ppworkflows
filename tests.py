import logging
import random
import time
import unittest

from multiprocessing import Value

import sys

import ppworkflows as ppw


class TestValue:
    """
    A basic helper class that allows to record processed int values for later assertion.
    """
    def __init__(self):
        self._value = Value('i', 0)
        self._calls = Value('i', 0)

    def assign_value(self, new_value):
        with self._value.get_lock():
            self._value.value = max(self._value.value, new_value)
        with self._calls.get_lock():
            self._calls.value += 1

    def __getattribute__(self, name):
        if name == "value":
            return self._value.value
        elif name == "calls":
            return self._calls.value
        else:
            return super().__getattribute__(name)


class BaseTest(unittest.TestCase):

    def __init__(self, method_name='runTest'):
        super().__init__(method_name)
        self.test_value = TestValue()

    # noinspection PyMethodMayBeStatic
    def gen_producer(self, values):

        def producer():
            for i in range(0, values):
                yield i + 1

        return producer

    def gen_worker(self, many=None, output=None, status=False):

        def work(task):
            if many is None:
                items = [task.get_one()]
            else:
                items = task.get_many(many)

            for item in items:
                time.sleep(random.uniform(0, 0.01))
                self.test_value.assign_value(item)
                if output is not None:
                    task.put(item, key=output)

            if status:
                task.put([('Calls: {:2d}', 1)], key="status")

        return work

    def perform_test(self, value):
        pass

    def test_1(self):
            self.perform_test(1)

    def test_2(self):
        self.perform_test(2)

    def test_10(self):
        self.perform_test(10)

    def test_100(self):
        self.perform_test(100)


class TestSimpleChains(BaseTest):

    def perform_test(self, values):
        workflow = ppw.Workflow()
        workflow.add_task(ppw.GeneratorTask(self.gen_producer(values)), outputs=["1"])
        workflow.add_task(ppw.SimpleTask(self.gen_worker()), input="1")
        workflow.run()

        self.assertEqual(values, self.test_value.value)
        self.assertEqual(values, self.test_value.calls)


class TestParallelWorker(BaseTest):

    def perform_test(self, values):
        workflow = ppw.Workflow()
        workflow.add_task(ppw.GeneratorTask(self.gen_producer(values)), outputs=["1"])
        workflow.add_task(ppw.SimpleTask(self.gen_worker()), input="1", runner_count=4)
        workflow.run()

        self.assertEqual(values, self.test_value.value)
        self.assertEqual(values, self.test_value.calls)


class TestParallelChains(BaseTest):

    def perform_test(self, values):
        workflow = ppw.Workflow()
        workflow.add_task(ppw.GeneratorTask(self.gen_producer(values)), outputs=["1"])
        workflow.add_task(ppw.SimpleTask(self.gen_worker(output="2")), input="1", outputs=["2"], runner_count=4)
        workflow.add_task(ppw.SimpleTask(self.gen_worker()), input="2", runner_count=4)
        workflow.run()

        self.assertEqual(values, self.test_value.value)
        self.assertEqual(values * 2, self.test_value.calls)


class TestMultiInput(BaseTest):

    def perform_test(self, values):
        workflow = ppw.Workflow()
        workflow.add_task(ppw.GeneratorTask(self.gen_producer(values)), outputs=["1"])
        workflow.add_task(ppw.SimpleTask(self.gen_worker()), input="1", runner_count=4)
        workflow.add_task(ppw.SimpleTask(self.gen_worker()), input="1", runner_count=4)
        workflow.run()

        self.assertEqual(values, self.test_value.value)
        self.assertEqual(values * 2, self.test_value.calls)


class TestMany(BaseTest):

    def perform_test(self, values):
        workflow = ppw.Workflow()
        workflow.add_task(ppw.GeneratorTask(self.gen_producer(values)), outputs=["1"])
        workflow.add_task(ppw.SimpleTask(self.gen_worker(many=2, output="2")), input="1", outputs=["2"], runner_count=4)
        workflow.add_task(ppw.SimpleTask(self.gen_worker(many=10)), input="2", runner_count=4)
        workflow.run()

        self.assertEqual(values, self.test_value.value)
        self.assertEqual(values * 2, self.test_value.calls)


class TestMultiOutput(BaseTest):

    def perform_test(self, values):
        workflow = ppw.Workflow()
        workflow.add_task(ppw.GeneratorTask(self.gen_producer(values)), outputs=["1"])
        workflow.add_task(ppw.SimpleTask(self.gen_worker(output="2")), input="1", outputs=["2", "3"], runner_count=4)
        workflow.add_task(ppw.SimpleTask(self.gen_worker(output="3")), input="1", outputs=["2", "3"], runner_count=4)
        workflow.add_task(ppw.SimpleTask(self.gen_worker()), input="2", runner_count=4)
        workflow.add_task(ppw.SimpleTask(self.gen_worker()), input="3", runner_count=4)
        workflow.run()

        self.assertEqual(values, self.test_value.value)
        self.assertEqual(values * 4, self.test_value.calls)


class TestInputLimit(BaseTest):

    def perform_test(self, values):
        workflow = ppw.Workflow()
        workflow.add_task(ppw.GeneratorTask(self.gen_producer(values)), outputs=["1"])
        workflow.add_task(ppw.SimpleTask(self.gen_worker(output="2")), input="1", outputs=["2"], runner_count=4, max_input_size=1)
        workflow.add_task(ppw.SimpleTask(self.gen_worker(many=3)), input="2", runner_count=4, max_input_size=2)
        workflow.add_task(ppw.SimpleTask(self.gen_worker(many=2)), input="2", runner_count=4)
        workflow.run()

        self.assertEqual(values, self.test_value.value)
        self.assertEqual(values * 3, self.test_value.calls)


class TestStatus(BaseTest):

    def perform_test(self, values):
        workflow = ppw.Workflow()
        workflow.add_task(ppw.GeneratorTask(self.gen_producer(values)), outputs=["1"])
        workflow.add_task(ppw.SimpleTask(self.gen_worker(status=True, output="2")), input="1", outputs=["2", "status"], runner_count=4)
        workflow.add_task(ppw.SimpleTask(self.gen_worker(status=True)), input="2", outputs=["status"], runner_count=4)
        workflow.add_task(ppw.StatusTask(sys.stdout), input="status")
        workflow.run()

        self.assertEqual(values, self.test_value.value)
        self.assertEqual(values * 2, self.test_value.calls)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    unittest.main()
