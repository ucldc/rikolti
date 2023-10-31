import json
from datetime import datetime
from airflow.decorators import dag, task

@task()
def create_path():
    return "boo"

@task()
def write_data(file_path):
    with open(f"/tmp/{file_path}.txt", "w") as f:
        f.write("first things first!")
    return True

@task()
def modify_data(file_path):
    with open(f"/tmp/{file_path}.txt", "r") as f:
        contents = f.read()
    contents = contents[:-1] + ", second things second!"
    print(contents)
    with open(f"/tmp/{file_path}.txt", "w") as f:
        f.write(contents)

@task()
def evaluate_data(file_path):
    with open(f"/tmp/{file_path}.txt", "r") as f:
        contents = f.read()
    print(contents)
    return {"report": len(contents)}

@task()
def write_report(report):
    with open("/tmp/report.txt", "w") as f:
        f.write(json.dumps(report))

@dag(
    dag_id="bitshift_and_return_values",
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["sample"],
)
def bitshift_and_return_values():
    """
    A functioning example of using the taskflow API to infer an order when
    possible and using the bitshift operator to specify an order when it's not
    possible to infer one.

    1) `path = create_path()`: The `create_path` task returns a value in the
    typical Taskflow API fashion. The return value of `create_path` is captured
    and passed as an argument to downstream tasks. The taskflow API infers that
    `create_path` is upstream of all tasks that take `path` as an argument.

    2) `write_data(path)`: `write_data` is dependent on `create_path` in the
    typical Taskflow API fashion. The return value of `create_path` is passed
    as an argument to `write_data`. The taskflow API infers that `write_data`
    is downstream of `create_path`. `write_data` has no return value, but does
    have a side effect: it writes some data to a file.
    
    3) `write_data(path) >> modify_data(path)`: `modify_data` is dependent on
    `create_path` in the typical Taskflow API way as well. However,
    `modify_data` is **also** dependent on the side effects of `write_data`
    (which writes some data to file). Since `write_data` has no return value
    and `modify_data` does not take as argument a return value of `write_data`,
    the taskflow API cannot infer this dependency, so we use the bitshift
    operator to define the dependency explicitly. 
    
    4) `evaluate_data` is similarly dependent on both `create_path` in typical
    Taskflow API fashion and on the side effects of `modify_data` (which writes
    some data to file), so the upstream tasks of `evaluate_data` are specified
    in both the Taskflow API way (by passing `path` as argument to
    `evaluate_data`) and the bitshift operator way:
    `modify_data(path) >> evaluate_data(path)`. However, `evaluate data`
    **also** returns a value that must be captured for later use so the
    Taskflow API can infer downstream tasks. We would typically do that with
    `report = evaluate_data(path)` (as we did with `path = create_path()`), but
    we cannot use the bitshift operator and the assignment operator in the same
    statement, like so (this causes a syntax error): 
    `modify_data(path) >> report = evaluate_data(path)`. To get around this, we
    actually set `report = evaluate_data(path)` prior to the bitshift operator
    statement entirely, and then use `modify_data(path) >> report. 

    5) `write_report(report)`: Finally, `write_report` is dependent on
    `evaluate_data` in the typical Taskflow API fashion. The output of
    `evaluate_data` is captured in `report`, and passed to `write_report` as an
    argument, so the Taskflow API can infer that `write_report` is downstream
    of `evaluate_data`. 

    Amy notes: I'm confused about the actual type of `path` and `report` in
    this example, and the types of `write_data_task` and `modified_data_task`
    in the more verbose commented-out examples. It seems the return value of
    decorated tasks can be used as xcom values when passing arguments to
    decorated decorated tasks, but can also be used as task instances when
    defining dag edges using the bitshift operator.
    """

    # example 1
    path = create_path()
    report = evaluate_data(path)
    (
        write_data(path) >> 
        modify_data(path) >> 
        report
    )
    write_report(report)

    # example 2
    # path = create_path()
    # write_data_task = write_data(path)
    # modified_data_task = modify_data(path)
    # report = evaluate_data(path)
    # write_data_task >> modified_data_task >> report >> write_report(report)

    # example 3
    # path = create_path() 
    # modified_data_task = modify_data(path)
    # (write_data(path) >> modified_data_task)
    # report = evaluate_data(path)
    # (modified_data_task >> report >> write_report(report))


bitshift_and_return_values()