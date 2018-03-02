# Parallel Python Workflows

Execute data driven task-based workflows in parallel with python. 

Available at *pipy*; install with *pip*

```
pip install ppworkflows
```

Basic example:

```python
import time
from ppworkflows import Task, Workflow


class Producer(Task):

    def run_loop(self):
        for i in range(0, 1000):
            self.put(i)


class Worker(Task):

    def run_loop(self):
        items = self.get_many(100)
        time.sleep(1)
        self.put(sum(items))


class Consumer(Task):

    def run_loop(self):
        print(self.get_one())


if __name__ == "__main__":

    workflow = Workflow()
    workflow.add_task(Producer(), outputs=["numbers"])
    workflow.add_task(Worker(), input="numbers", outputs=["sums"], runner_count=4)
    workflow.add_task(Consumer(), input="sums")
    workflow.run()
```

Example that uses predefined common tasks:

```python
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
```
