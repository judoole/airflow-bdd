
from airflow_bdd import (
    feature,
    given_dag,
    given_a_dag,
    given_task,
    given_all_tasks_of_type,
    given_dagbag,
    given_execution_date,
    given_variable,
    given_xcom,
    when_I_get_dag,
    when_I_render_the_task,
    when_I_execute_the_task,
    when_I_render_the_tasks,
    it,
    the_task,
    the_dag,
)
from airflow_bdd.compat import DAG, BashOperator, EmptyOperator
from hamcrest import instance_of, has_property, has_length, equal_to, is_, has_items, not_none
from hamcrest import assert_that as then
import pytest
import pendulum
from unittest import mock
import os
import tempfile

# Get DAGs folder relative to this file, using os.path.dirname
# The dags folder is in tests/test_dags
TESTS_FOLDER = os.path.dirname(__file__)
TEST_DAGS_FOLDER = os.path.join(TESTS_FOLDER, "test_dags")
TEST_VARIABLES_FILE = os.path.join(TESTS_FOLDER, "test_variables.json")


#@feature()
#def test_ensure_that_we_override_airflow_home():
#    """As a developer
#    I want to override the AIRFLOW_HOME environment variable
#    So that I can run tests in an isolated environment"""
#    # Assert that the debug.airflow_home dir contains
#    # airflow.db, print content of airflow_home if not
#    assert os.path.exists(debug.airflow_home)
#    assert os.path.exists(os.path.join(debug.airflow_home, "airflow.db")
#                          ), f"Content of {debug.airflow_home}: {os.listdir(debug.airflow_home)}"


@feature()
def test_given_a_dag():
    """As a developer
    I want to add a random DAG to the context
    So that I can use it for adding and testing tasks
    """
    given_a_dag()
    then(it(), is_(instance_of(DAG)))


@feature()
def test_given_the_dag():
    """As a developer
    I want to create a DAG from scratch
    So that I can create tests using specific DAG configurations
    """
    given_dag(DAG(
        dag_id="my_dag",
        schedule=None,
        start_date=pendulum.today("UTC").add(365),
    ))
    when_I_get_dag()
    then(it(), has_property("dag_id", equal_to("my_dag")))


@feature()
def test_given_a_tasks_on_a_dag():
    """As a developer
    I want to add tasks to a DAG
    So that I can test the tasks
    """
    given_a_dag()
    given_task(EmptyOperator(task_id="task_1"))
    given_task(EmptyOperator(task_id="task_2"))
    when_I_get_dag()
    then(it(), has_property("tasks", has_length(2)))


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


@feature()
@pytest.mark.parametrize("task_id", [
    "task_1",
    "task_2",
])
def test_with_pytest_params(task_id):
    """As a developer
    I want to be able to use pytest.mark.parametrize
    So that I can run the same test with different parameters
    """
    given_a_dag()
    given_task(EmptyOperator(task_id=task_id))
    then(it(), has_property("task_id", equal_to(task_id)))


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


@feature()
def test_should_be_able_to_load_dagbag():
    """As a developer
    I want to load a given folder with DAGs
    So that I can do asserts on my entire DagBag"""
    given_dagbag(TEST_DAGS_FOLDER)
    then(it(), has_property("dags", has_length(1)))
    then(it(), has_property("import_errors", has_length(1)))


@feature()
def test_should_be_get_dag_from_dagbag():
    """As a developer
    I want to get a dag from the DagBag
    So that I can assert that my production code works"""
    given_dagbag(TEST_DAGS_FOLDER)
    given_dag("simple_dag")
    then(it(), has_property("dag_id", "simple_dag"))
    then(it(), has_property("tasks", has_length(2)))


@feature()
def test_should_be_get_task_from_dag_from_dagbag():
    """As a developer
    I want to get a task from a dag from the DagBag
    So that I can assert that my production code works"""
    given_dagbag(TEST_DAGS_FOLDER)
    given_dag("simple_dag")
    given_task("simpleton_task")
    then(it(), is_(instance_of(EmptyOperator)))


@feature()
def test_should_pick_up_env_var_for_dags_folder():
    """As a developer
    I want to set the dags folder using an environment variable
    And the env var should be the default AIRFLOW__CORE__DAGS_FOLDER
    So that I can specify dags folder outside tests"""

    with mock.patch.dict(os.environ, {'AIRFLOW__CORE__DAGS_FOLDER': TEST_DAGS_FOLDER}):
        given_dagbag()
        then(it(), has_property("dags", has_length(1)))
        then(it(), has_property("import_errors", has_length(1)))


@feature()
def test_should_support_adding_variables():
    """As a developer
    I want to add variables to the context
    So that I can use them in my tests"""
    given_variable(key="my_key", value="my_value")
    given_task(BashOperator(task_id="task",
                  bash_command="echo {{ var.value.my_key }}"))
    when_I_execute_the_task()
    then(it(), is_(equal_to("my_value")))


@mock.patch.dict(os.environ, {'AIRFLOW__BDD__VARIABLES_FILE': TEST_VARIABLES_FILE})
@feature(airflow_home=tempfile.mkdtemp())
def test_should_pick_up_env_var_for_variables():
    """As a developer
    I want to set an Airflow variables file as an environment variable
    And the env var should be the default AIRFLOW__BDD__VARIABLES_FILE
    So that I can specify variables once
    And that I can use them in my tests
    """

    given_task(BashOperator(task_id="task",
                                    bash_command="echo {{ var.value.test_key }}"))
    when_I_execute_the_task()
    then(it(), is_(equal_to("test_value")))


@feature()
def test_should_support_adding_simple_xcom():
    """As a developer
    I want to add xcom to the context
    So that I can use the xcom in rendering of my tests"""
    given_task(BashOperator(task_id="task_1",
                  bash_command="echo hello"))
    given_task(
        BashOperator(task_id="task_2",
                     bash_command="echo {{ ti.xcom_pull(task_ids='task_1') }}"))
    given_xcom(task_id="task_1", value="not hello")
    when_I_render_the_task("task_2")
    then(it(), has_property("bash_command", "echo not hello"))


@feature()
def test_should_be_able_to_find_all_tasks_of_type():
    """As a developer
    I want filter all tasks in the DagBag
    So that I can assert tasks of a specific type"""
    given_dagbag(TEST_DAGS_FOLDER)
    given_execution_date("1976-08-13")
    given_all_tasks_of_type("BashOperator")
    when_I_render_the_tasks()
    then(it(), has_items(instance_of(BashOperator)))
    then(it(), has_items(
        has_property("bash_command", "echo 19760813"),
    ))
