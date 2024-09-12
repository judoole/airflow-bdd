# test_bdd.py
from airflow_bdd import airflow_bdd
from airflow_bdd.scenario import Scenario
from airflow_bdd.steps.dag_steps import a_dag, a_task, get_dag, render_the_task, execution_date
from airflow_bdd.steps.hamcrest_steps import it_should, task_should, dag_should
from hamcrest import instance_of, has_property, has_length, equal_to
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
import pytest


@pytest.mark.skip("This test is not working")
def test_with_context_manager():
    with airflow_bdd() as bdd:
        bdd.given(a_dag())
        bdd.then(it_should(instance_of(DAG)))


@airflow_bdd()
def test_given_a_dag(bdd: Scenario):
    bdd.given(a_dag())
    bdd.then(it_should(instance_of(DAG)))


@airflow_bdd()
def test_given_a_tasks_on_a_dag(bdd: Scenario):
    bdd.given(a_dag())
    bdd.and_given(a_task(EmptyOperator(task_id="task_1")))
    bdd.and_given(a_task(EmptyOperator(task_id="task_2")))
    bdd.when_I(get_dag())
    bdd.then(it_should(has_property("tasks", has_length(2))))


@airflow_bdd()
def test_rendering_of_a_task(bdd: Scenario):
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
    bdd.given(a_dag())
    bdd.and_given(a_task(EmptyOperator(task_id=task_id)))
    bdd.then(it_should(has_property("task_id", equal_to(task_id))))
