# Airflow BDD

**Airflow BDD** is a testing framework for Apache Airflow that implements Behavior Driven Development (BDD) principles. It allows users to write tests for their Airflow DAGs using a clear and concise syntax, facilitating better collaboration and understanding of the system's behavior.

## Features

- BDD-style syntax (Given, When, Then) for writing tests
- Decorator for easy integration with existing tests
- Reusable objects across test functions

## Installation

No pypi yet, but you can test it with `pip install git+https://github.com/judoole/airflow-bdd.git`

## Usage

Using the `airflow_bdd` decorator you can easily shorten the setup parts of your Airflow tests. For example:

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

There is a lot of other ways to use the tool as well. Take a look into the `tests` folder for more examples.

## Creation your own steps

Basically it is just creating a function is able to receive a [Context](https://github.com/judoole/airflow-bdd/blob/daed1195e459a8adaef281463117984de7b55a23/src/airflow_bdd/core/scenario.py#L1) object. Take inspiration from the code in [steps folder](https://github.com/judoole/airflow-bdd/tree/main/src/airflow_bdd/steps).
