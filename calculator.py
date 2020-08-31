import logging
import prefect
from prefect import task, Flow, Parameter
from prefect.tasks.control_flow import switch, merge
from prefect.utilities.logging import get_logger
from prefect.engine.result.base import Result

# Define a task which "parses the inputs" and returns a dict of those inputs
@task(name="Parse Input")
def parse_input(expression):
    logger = prefect.context.get("logger")
    x, op, y = expression.split(' ')
    logger.info("Received {} {} {}".format(x, op, y))
    return dict(x=float(x), op=op, y=float(y))

# 'Arithmetic' is the name of the flow
with Flow('Arithmetic') as flow:
    inputs = parse_input(Parameter('expression'))

    # once we have our inputs, we create a dict of operations:
    x, y = inputs['x'], inputs['y']
    operations = {
        '+': x + y,
        '-': x - y,
        '*': x * y,
        '/': x / y
    }

    # use prefect's `switch` task (conditional) to branch the flow, selecting the right operation
    switch(condition=inputs['op'], cases=operations)

    # use prefect's `merge` task to bring bring the branches from `switch` back together, producing a result
    result = merge(*operations.values())

    # do something with the result...
    Result(result)

task_logger = get_logger("Task")

# Register the `Arithmetic` flow in the `Test1` project
flow.register(project_name='Test1')
# flow.run()

# print(flow.serialize())
