from airflow_bdd.core.context import Context
from typing import Any
import pendulum
import uuid
from airflow.utils.session import provide_session
from airflow.models.xcom import XCOM_RETURN_KEY
import os
from airflow_bdd.core.decorator import bdd


@bdd
def given_dag(dag_or_dag_id: Any = None, context: Context = None):
    from airflow.models.dag import DAG
    from airflow.models import DagBag
    dag = dag_or_dag_id

    if isinstance(dag, str):
        dagbag: DagBag = context["dagbag"]
        dag = dagbag.get_dag(dag)

    if not dag:
        dag = DAG(
            # create a unique dag_id
            dag_id=f"test_dag_{uuid.uuid4()}",
            # Set schedule_interval to None
            # to prevent the DAG from being scheduled
            schedule=None,
            # Set start date to 1 year ago
            start_date=pendulum.today("UTC").add(-365),
        )
    context[dag.dag_id] = dag
    context["dag"] = dag


@bdd
def given_execution_date(execution_date, context: Context):
    """Given the execution date."""
    if isinstance(execution_date, str):
        execution_date = pendulum.parse(execution_date)
    elif isinstance(execution_date, pendulum.DateTime):
        execution_date = execution_date
    else:
        raise ValueError(
            f"execution_date must be a string or a pendulum.DateTime, got {type(execution_date)}"
        )
    context["execution_date"] = execution_date


@bdd
def given_task(task: Any, context: Context):
    from airflow.models.dag import DAG
    if "dag" not in context:
        given_dag()
    dag: DAG = context["dag"]

    if isinstance(task, str):
        context["task"] = dag.get_task(task)
    else:
        dag.add_task(task)
        context["task"] = task


@bdd
def given_all_tasks_of_type(task_type: Any, context: Context):
    # If the task_type is a type, get the class name
    if isinstance(task_type, type):
        task_type = task_type.__name__
    else:
        # If the task_type is a string, use it as is
        task_type = task_type

    from airflow.models.dag import DAG
    from airflow.models import DagBag
    if "dagbag" in context:
        dagbag: DagBag = context["dagbag"]
        tasks = []
        for dag in dagbag.dags.values():
            for task in dag.tasks:
                # Check if the task's class name contains the string
                if task_type in task.__class__.__name__:
                    tasks.append(task)
                    continue

                # If not, check the task's superclasses
                for base_class in task.__class__.__mro__:
                    if task_type in base_class.__name__:
                        tasks.append(task)
                        break  # Stop checking further superclasses
        context["tasks"] = tasks
    elif "dag" in context:
        raise NotImplementedError("not implemented yet")
    else:
        raise ValueError(
            "dagbag or dag not found in context. Please provide a dagbag or dag in the context.")


@bdd
def given_variable(key: str, value: Any, context: Context):
    from airflow.models import Variable
    Variable.set(key, value)


@bdd
@provide_session
def given_dagrun(dag_id: str = None,
                 execution_date: pendulum.DateTime = None,
                 state: str = "running",
                 run_type: str = "manual",
                 conf: dict = None,
                 context: Context = None, session=None):
    if not dag_id and "dag" not in context:
        given_dag()
        dag_id = context["dag"].dag_id
    elif not dag_id and "dag" in context:
        dag_id = context["dag"].dag_id
    if not execution_date and "execution_date" not in context:
        given_execution_date(pendulum.now())
        execution_date = context["execution_date"]
    elif not execution_date and "execution_date" in context:
        execution_date = context["execution_date"]

    dag_run = context[dag_id].create_dagrun(
        run_id=f"test_dag_run_{uuid.uuid4()}",
        execution_date=execution_date,
        start_date=execution_date,
        state=state,
        run_type=run_type,
        conf=conf,
        session=session,
    )
    context[f"dag_run_{dag_id}"] = dag_run
    context["dag_run"] = dag_run


@bdd
@provide_session
def given_xcom(task_id: str,
               value: Any,
               dag_id: str = None,
               key: str = XCOM_RETURN_KEY,
               context: Context = None, session=None):
    from airflow.models.dagrun import DagRun
    if "dag_run" not in context:
        given_dagrun(dag_id=dag_id)
    dag_run: DagRun = context["dag_run"]
    # First the the task instance
    x_ti = dag_run.get_task_instance(task_id, session=session)
    assert (
        x_ti is not None
    ), f"TaskInstance with task_id {task_id} does not exist in the DagRun: {dag_run.task_instances}"
    # Refresh the task instance, from the DAG
    x_ti.refresh_from_task(context["dag"].get_task(x_ti.task_id))
    # Push the XCom
    x_ti.xcom_push(key=key, value=value, session=session)


@bdd
def given_dagbag(dags_folder: str = None, context: Context = None):
    # Capture warnings
    import warnings
    from airflow.models import DagBag

    dags_folder = dags_folder or os.environ.get(
        'AIRFLOW__CORE__DAGS_FOLDER')

    with warnings.catch_warnings(record=True) as captured_warnings:
        warnings.simplefilter("always")
        dagbag = DagBag(dag_folder=dags_folder,
                        include_examples=False)

    context["dag_bag_warnings"] = captured_warnings
    context["dagbag"] = dagbag


@bdd
def when_I_get_dag(context: Context):
    context["it"] = context["dag"]


@bdd
@provide_session
def when_I_render_the_task(task_id: str = None, context: Context = None, session=None):
    if "execution_date" not in context:
        given_execution_date(pendulum.now())

    from airflow.models.taskinstance import TaskInstance

    # Create a DagRun
    if "dag_run" not in context:
        given_dagrun()
    dag_run = context["dag_run"]
    task_id = task_id or context["task"].task_id
    ti: TaskInstance = dag_run.get_task_instance(
        task_id, session=session)
    assert (
        ti is not None
    ), f"TaskInstance with task_id {task_id} does not exist in the DagRun: {dag_run.get_task_instances(session=session)}"
    ti.refresh_from_task(context["dag"].get_task(ti.task_id))
    # Render the template fields
    # This sets the rendered variables on the self.task instance
    # so we can access them late, in the then statements
    ti.render_templates()
    context["task_instance"] = ti
    context.set_it(context["task"])


@bdd
@provide_session
def when_I_render_the_tasks(context: Context = None, session=None):
    if "execution_date" not in context:
        given_execution_date(pendulum.now())

    from airflow.models.taskinstance import TaskInstance
    from airflow.models.dagrun import DagRun

    # Iterate through all tasks in the context
    for task in context["tasks"]:
        task: TaskInstance = task
        # Check if DAG is in context
        if task.dag_id not in context:
            given_dag(dag_or_dag_id=task.dag_id)
        # Create a DagRun
        if f"dag_run_{task.dag_id}" not in context:
            given_dagrun(dag_id=task.dag_id)
        dag_run: DagRun = context[f"dag_run_{task.dag_id}"]
        task_id = task.task_id
        ti: TaskInstance = dag_run.get_task_instance(
            task_id, session=session)
        assert (
            ti is not None
        ), f"TaskInstance with task_id {task.task_id} does not exist in the DagRun: {dag_run.get_task_instances(session=session)}"
        ti.refresh_from_task(dag_run.dag.get_task(ti.task_id))
        # Render the template fields
        # This sets the rendered variables on the self.task instance
        # so we can access them late, in the then statements
        ti.render_templates()
    context.set_it(context["tasks"])


@bdd
def when_I_execute_the_task(context: Context):
    """Execute the task and save the results."""
    if "task_instance" not in context:
        when_I_render_the_task()
    ti = context["task_instance"]

    context["output"] = ti.task.execute(ti.get_template_context())


given_a_dag = given_dag
given_a_task = given_task


@bdd
def the_task(context: Context = None):
    return context["task"]

@bdd
def the_dag(context: Context = None):
    return context["dag"]

@bdd
def it(context: Context = None):
    return context.it()
