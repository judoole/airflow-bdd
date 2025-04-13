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

The usage is done through a decorator, @feature, for which you decorate your test functions. The given, when, and then functions are used to define the steps of the test.

### Simple example

```python
from airflow_bdd.core.decorator import feature
from airflow_bdd.steps.dag_steps import given_a_dag, given_task, when_I_get_dag, it
from airflow.operators.empty import EmptyOperator
from hamcrest import has_property, assert_that as then

@feature()
def test_given_tasks_on_a_dag():
    """As a developer
    I want to add tasks to a DAG
    So that I can test the tasks
    """
    given_a_dag()
    given_task(EmptyOperator(task_id="task_1"))
    given_task(EmptyOperator(task_id="task_2"))
    when_I_get_dag()
    then(it(), has_property("task_count", 2))
```

This example creates a DAG, adds two tasks, and then asserts, using hamcrest that the task count of the DAG is 2.

If you don't like [hamcrest](https://github.com/judoole/airflow-bdd/blob/main/README.md), or otherwise want to write all your steps yourself, you can do so:

```python
from airflow_bdd.core.decorator import feature
from airflow_bdd.core.scenario import Scenario

@feature()
def test_without_steps(bdd: Scenario):
    """As a developer who only likes core methods
    I want to write my own steps
    So that I use airflow_bdd to test stuff
    """
    def assert_is_cherry(context):
        assert context.it() == "cherry"

    bdd.given(lambda context: context.add("my_tuple", ("apple", "banana", "cherry")))
    bdd.when(lambda context: context.add("output", context["my_tuple"][2]))
    bdd.then(assert_is_cherry)
```

There are many other ways to use the decorator as well. Take a look into the `tests` folder for more examples.

### Testing dags folder

This particular test assumes that you have set the environment variable `AIRFLOW__CORE__DAGS_FOLDER` during invocation of the test.

```python
from airflow_bdd.core.decorator import feature
from airflow_bdd.steps.dag_steps import given_dagbag, it
from hamcrest import has_property, has_length, assert_that as then

@feature()
def test_should_be_able_to_load_dagbag():
    """As a developer
    I want to check that I have the correct amount of DAGs
    So that I can be sure that code changes haven't removed any DAGs"""
    given_dagbag()
    then(it(), has_property("dags", has_length(12)))
    then(it(), has_property("import_errors", has_length(0)))
```

You can also specify the dags folder yourself and do asserts on tasks in DAGs.

```python
from airflow_bdd.core.decorator import feature
from airflow_bdd.steps.dag_steps import given_dagbag, given_dag, given_task, it
from airflow.operators.empty import EmptyOperator
from hamcrest import is_, instance_of, assert_that as then

@feature()
def test_should_be_get_task_from_dag_from_dagbag():
    """As a developer
    I want to get a task from a dag from the DagBag
    So that I can assert that my production code works"""
    given_dagbag("/tmp/my-fancy-dags-folder/")
    given_dag("simple_dag")
    given_task("simpleton_task")
    then(it(), is_(instance_of(EmptyOperator)))
```

### Testing rendering of tasks

```python
from airflow_bdd.core.decorator import feature
from airflow_bdd.steps.dag_steps import given_a_dag, given_execution_date, given_task, when_I_render_the_task, the_task, the_dag
from airflow.operators.bash import BashOperator
from hamcrest import has_property, has_length, assert_that as then

@feature()
def test_rendering_of_a_task():
    """As a developer
    I want to render a task
    So that I can assert the rendered template
    """
    given_a_dag()
    given_execution_date("2020-01-01")
    given_task(
        BashOperator(
            task_id="task",
            bash_command="echo hello {{ ds }}"))
    when_I_render_the_task()
    then(the_task(), has_property("bash_command", "echo hello 2020-01-01"))
    then(the_dag(), has_property("tasks", has_length(1)))
```

### Testing execution of a task

```python
from airflow_bdd.core.decorator import feature
from airflow_bdd.steps.dag_steps import given_a_dag, given_task, when_I_execute_the_task, it
from airflow.operators.bash import BashOperator
from hamcrest import is_, equal_to, assert_that as then

@feature()
def test_execute_task():
    """As a developer
    I want to execute a task
    So that I can test the output
    """
    given_a_dag()
    given_task(BashOperator(task_id="task", bash_command="echo hello"))
    when_I_execute_the_task()
    then(it(), is_(equal_to("hello")))
```

### Using variables

If you set the environment variable `AIRFLOW__BDD__VARIABLES_FILE` to point to your Airflow variables JSON file, it will be used during the tests.

If you want to add variables explicitly, you can do so like this:

```python
from airflow_bdd.core.decorator import feature
from airflow_bdd.steps.dag_steps import given_variable, given_task, when_I_execute_the_task, it
from airflow.operators.bash import BashOperator
from hamcrest import is_, equal_to, assert_that as then

@feature()
def test_should_support_adding_variables():
    """As a developer
    I want to add variables to the context
    So that I can use them in my tests
    """
    given_variable(key="my_key", value="my_value")
    given_task(BashOperator(task_id="task",
                  bash_command="echo {{ var.value.my_key }}"))
    when_I_execute_the_task()
    then(it(), is_(equal_to("my_value")))
```

## Create your own steps

Basically, it is just creating a function that is able to receive a [Context](https://github.com/judoole/airflow-bdd/blob/daed1195e459a8adaef281463117984de7b55a23/src/airflow_bdd/core/scenario.py#L1) object. Take inspiration from the code in the [steps folder](https://github.com/judoole/airflow-bdd/tree/main/src/airflow_bdd/steps).

## Contribute

This repo is brand new, so **any** contribution is welcomed warmly.