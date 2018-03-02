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
