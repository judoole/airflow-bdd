# Airflow BDD

**Airflow BDD** is a testing framework for Apache Airflow that implements [Behavior Driven Development (BDD)](https://en.wikipedia.org/wiki/Behavior-driven_development) principles. It allows users to write tests for their Airflow DAGs using a clear and concise syntax, facilitating better collaboration and understanding of the system's behavior.

## Features

- BDD-style syntax (Given, When, Then) for writing tests
- Decorator for easy integration with existing tests
- Creates isolated temporary AIRFLOW_HOME for each test run
- Able run tests against "dags folder"

## Installation

No pypi yet, but you can test it with `pip install git+https://github.com/judoole/airflow-bdd.git`

## Usage

The usage is done through a decorator, `airflow_bdd`, for which you decorate your test functions.
That decorator returns a Scenario object, which has methods for `given`, `when` and `then`. These methods takes a parameter of type respective to their name, `GivenStep`, `WhenStep` or `ThenStep`. These are basically `Callable`s that are supplied a dictionary of type [Context](https://github.com/judoole/airflow-bdd/blob/main/src/airflow_bdd/core/scenario.py#L1)

### Simple example

```python
from airflow_bdd import airflow_bdd
from airflow_bdd.core.scenario import Scenario
from airflow_bdd.steps.dag_steps import a_dag, a_task, get_dag
from airflow_bdd.steps.providers.hamcrest.hamcrest_steps import it_should
from hamcrest import has_property

@airflow_bdd()
def test_given_a_tasks_on_a_dag(bdd: Scenario):
    """As a developer
    I want to add tasks to a DAG
    So that I can test the tasks
    """
    bdd.given(a_dag())
    bdd.and_given(a_task(EmptyOperator(task_id="task_1")))
    bdd.and_given(a_task(EmptyOperator(task_id="task_2")))
    bdd.when_I(get_dag())
    bdd.then(it_(has_property("task_count", 2)))
```

This example creates a DAG, adds two tasks, and then asserts, using hamcrest that the task count of the DAG is 2.

If you don't like [hamcrest]https://github.com/judoole/airflow-bdd/blob/main/README.md), or otherwise want to write all your steps yourself, you can do so.

```python
@airflow_bdd()
def test_without_steps(bdd: Scenario):
    """As a developer who only likes core methods
    I want write my own steps
    So that I use airflow_bdd to test stuff
    """
    def assert_is_cherry(context):
        assert context.it() == "cherry"

    bdd.given(lambda context: context.add("my_tuple", ("apple", "banana", "cherry")))
    bdd.when(lambda context: context.add("output", context["my_tuple"][2]))
    bdd.then(assert_is_cherry)
```

There is a lot of other ways to use the decorator as well. Take a look into the `tests` folder for more examples.

### Testing dags folder

This particular test assumes that you have set the environment variable `AIRFLOW__CORE__DAGS_FOLDER` during invocation of the test.

```python
@airflow_bdd()
def test_should_be_able_to_load_dagbag(bdd: Scenario):
    """As a developer
    I want to check that I have the correct amount of DAGs
    So that I can be sure that code changes hasn't removed any DAGs"""
    bdd.given(dagbag())
    bdd.then(it_(has_property("dags", has_length(12))))
    bdd.then(it_(has_property("import_errors", has_length(0))))

@airflow_bdd()
def test_should_be_get_task_from_dag_from_dagbag(bdd: Scenario):
    """As a developer
    I want to get a task from a dag from the DagBag
    So that I can assert that my production code works"""
    bdd.given(dagbag(TEST_DAGS_FOLDER))
    bdd.and_given(the_dag("simple_dag"))
    bdd.and_given(the_task("simpleton_task"))
    bdd.then(it_(is_(instance_of(EmptyOperator))))
```

### Testing rendering of tasks

```python
@airflow_bdd()
def test_rendering_of_a_task(bdd: Scenario):
    """As a developer
    I want to render a task
    So that I can assert the rendered template
    """
    bdd.given(a_dag())
    bdd.and_given(execution_date("2020-01-01"))
    bdd.and_given(a_task(
        BashOperator(
            task_id="task",
            bash_command="echo hello {{ ds }}")),
    )
    bdd.when_I(render_the_task())
    bdd.then(task_(has_property("bash_command", "echo hello 2020-01-01")))
    bdd.and_then(dag_(has_property("tasks", has_length(1))))
```

### Testing execute of task

```python
@airflow_bdd()
def test_execute_task(bdd: Scenario):
    """As a developer
    I want to execute a task
    So that I can test the output
    """
    bdd.given(a_dag())
    bdd.and_given(
        a_task(
            BashOperator(task_id="task", bash_command="echo hello"))
    )
    bdd.and_when(execute_the_task())
    bdd.then(it_(is_(equal_to("hello"))))
```

## Creation your own steps

Basically it is just creating a function is able to receive a [Context](https://github.com/judoole/airflow-bdd/blob/daed1195e459a8adaef281463117984de7b55a23/src/airflow_bdd/core/scenario.py#L1) object. Take inspiration from the code in [steps folder](https://github.com/judoole/airflow-bdd/tree/main/src/airflow_bdd/steps).
