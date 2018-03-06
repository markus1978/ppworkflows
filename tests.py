import logging
import random
import time

import ppworkflows as ppw


def test_basic():
    def producer():
        for i in range(0, 1000):
            yield i

    def work(task):
        items = task.get_many(100)
        task.put([('Sum: {:6d}', sum(items)), ('Calls: {:2d}', 1)])

    workflow = ppw.Workflow()
    workflow.add_task(ppw.GeneratorTask(producer), outputs=["numbers"])
    workflow.add_task(ppw.SimpleTask(work), input="numbers", outputs=["sums"], runner_count=4)
    workflow.add_task(ppw.StatusTask(), input="sums")
    workflow.run()


def test_massive_parallel():
    def producer():
        for i in range(0, 1000):
            time.sleep(random.uniform(0, 0.01))
            yield i

    def forward(task):
        item = task.get_one()
        time.sleep(random.uniform(0, 0.01))
        task.put(item)

    def work(task):
        item = task.get_one()
        task.put([('Sum: {:6d}', item), ('Calls: {:2d}', 1)])

    workflow = ppw.Workflow()
    workflow.add_task(ppw.GeneratorTask(producer), outputs=["1"])
    workflow.add_task(ppw.SimpleTask(forward), input="1", outputs=["2"], runner_count=20)
    workflow.add_task(ppw.SimpleTask(work), input="2", outputs=["status"], runner_count=4)
    workflow.add_task(ppw.StatusTask(), input="status")
    workflow.run()


def test_deadlock():
    def producer():
        for i in range(0, 10000):
            yield i

    def forward(task):
        item = task.get_one()
        if random.uniform(0, 1) < 0.5:
            if task.process_number < 10:
                time.sleep(task.process_number)
            else:
                time.sleep(random.uniform(0, 0.05))
            task.put(item)

    def work(task):
        item = task.get_one()
        if item - 9000 > 0:
            time.sleep(random.uniform(0, 0.1))
        else:
            time.sleep(random.uniform(0, 0.007))
        task.put([('Sum: {:6d}', item), ('Calls: {:2d}', 1)])

    workflow = ppw.Workflow()
    workflow.add_task(ppw.GeneratorTask(producer), outputs=["1"])
    workflow.add_task(ppw.SimpleTask(forward), input="1", outputs=["2"], runner_count=20, max_input_size=3000)
    workflow.add_task(ppw.SimpleTask(work), input="2", outputs=["status"], runner_count=4, max_input_size=1000)
    workflow.add_task(ppw.StatusTask(), input="status", max_input_size=1)
    workflow.run()


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    test_basic()
    test_massive_parallel()
    test_deadlock()

