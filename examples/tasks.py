import time
from ppworkflows import Workflow, GeneratorTask, StatusTask, SimpleTask


def producer():
    for i in range(0, 1000):
        yield i


def work(task):
    items = task.get_many(100)
    time.sleep(1)
    task.put([('Sum: {:6d}', sum(items)), ('Calls: {:2d}', 1)])


if __name__ == "__main__":

    workflow = Workflow()
    workflow.add_task(GeneratorTask(producer), outputs=["numbers"])
    workflow.add_task(SimpleTask(work), input="numbers", outputs=["sums"], runner_count=4)
    workflow.add_task(StatusTask(), input="sums")
    workflow.run()
