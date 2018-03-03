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
        i = 0
        while i < 30000:
            i += 1
            yield i

    def forward(task):
        item = task.get_one()
        task.put(item)

    def work(task):
        item = task.get_one()
        task.put([('Sum: {:6d}', item), ('Calls: {:2d}', 1)])

    workflow = ppw.Workflow()
    workflow.add_task(ppw.GeneratorTask(producer), outputs=["numbers"])
    workflow.add_task(ppw.SimpleTask(work), input="numbers", outputs=["sums"], runner_count=24)
    workflow.add_task(ppw.StatusTask(), input="sums")
    workflow.run()


if __name__ == "__main__":
    test_basic()
    test_massive_parallel()

