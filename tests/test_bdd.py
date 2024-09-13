from airflow_bdd import airflow_bdd
from airflow_bdd.core.scenario import Scenario
from airflow_bdd.steps.dag_steps import a_dag, dag, a_task, get_dag, render_the_task, execution_date, execute_the_task, dagbag
from airflow_bdd.steps.providers.hamcrest.hamcrest_steps import it_should, task_should, dag_should
from hamcrest import instance_of, has_property, has_length, equal_to
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import pytest
import pendulum
from unittest import mock
import os

# Get DAGs folder relative to this file, using os.path.dirname
# The dags folder is in tests/test_dags
os.path.dirname(__file__)
TEST_DAGS_FOLDER = os.path.join(os.path.dirname(__file__), "test_dags")


@pytest.mark.skip("This test is not working")
def test_with_context_manager():
    with airflow_bdd() as bdd:
        bdd.given(a_dag())
        bdd.then(it_should(instance_of(DAG)))


@airflow_bdd()
def test_given_a_dag(bdd: Scenario):
    """As a developer
    I want to add a random DAG to the context
    So that I can use it for adding and testing tasks
    """
    bdd.given(a_dag())
    bdd.then(it_should(instance_of(DAG)))


@airflow_bdd()
def test_given_the_dag(bdd: Scenario):
    """As a developer
    I want to create a DAG from scratch
    So that I can create tests using specific DAG configurations
    """
    bdd.given(dag(DAG(
        dag_id="my_dag",
        schedule_interval=None,
        start_date=pendulum.today("UTC").add(365),
    )))
    bdd.when_I(get_dag())
    bdd.then(it_should(has_property("dag_id", equal_to("my_dag"))))


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
    bdd.then(it_should(has_property("task_count", 2)))


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
    bdd.then(task_should(has_property("bash_command", "echo hello 2020-01-01")))
    bdd.and_then(dag_should(has_property("tasks", has_length(1))))


@pytest.mark.parametrize("task_id", [
    "task_1",
    "task_2",
])
@airflow_bdd()
def test_with_pytest_params(bdd: Scenario, task_id):
    """As a developer
    I want to be able to use pytest.mark.parametrize
    So that I can run the same test with different parameters
    """
    bdd.given(a_dag())
    bdd.and_given(a_task(EmptyOperator(task_id=task_id)))
    bdd.then(it_should(has_property("task_id", equal_to(task_id))))


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
    bdd.then(it_should(equal_to("hello")))


@airflow_bdd()
def test_should_be_able_to_load_dagbag(bdd: Scenario):
    """As a developer
    I want to load a given folder with DAGs
    So that I can do asserts on my entire DagBag"""
    bdd.given(dagbag(TEST_DAGS_FOLDER))
    bdd.then(it_should(has_property("dags", has_length(1))))
    bdd.then(it_should(has_property("import_errors", has_length(1))))


@airflow_bdd()
def test_should_be_get_dag_from_dagbag(bdd: Scenario):
    """As a developer
    I want to get a dag from the DagBag
    So that I can assert that my production code works"""
    bdd.given(dagbag(TEST_DAGS_FOLDER))
    bdd.and_given(dag("simple_dag"))
    bdd.then(it_should(has_property("dag_id", "simple_dag")))
    bdd.then(it_should(has_property("task_count", 1)))


@airflow_bdd()
def test_should_pick_up_env_var_for_dags_folder(bdd: Scenario):
    """As a developer
    I want to set the dags folder using an environment variable
    And the env var should be the default AIRFLOW__CORE__DAGS_FOLDER
    So that I can specify dags folder outside tests"""

    with mock.patch.dict(os.environ, {'AIRFLOW__CORE__DAGS_FOLDER': TEST_DAGS_FOLDER}):
        bdd.given(dagbag())
        bdd.then(it_should(has_property("dags", has_length(1))))
        bdd.then(it_should(has_property("import_errors", has_length(1))))
