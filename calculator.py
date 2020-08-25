from prefect import task, Flow, Parameter
from prefect.tasks.control_flow import switch, merge

@task
def parse_input(expression):
    x, op, y = expression.split(' ')
    return dict(x=float(x), op=op, y=float(y))

with Flow('Arithmetic') as flow:
    inputs = parse_input(Parameter('expression'))

    # once we have our inputs, everything else is the same:
    x, y = inputs['x'], inputs['y']
    operations = {
        '+': x + y,
        '-': x - y,
        '*': x * y,
        '/': x / y
    }
    switch(condition=inputs['op'], cases=operations)
    result = merge(*operations.values())
    print(result)

flow.register(project_name='Test1')

