from airflow_bdd.core.scenario import Context, GivenStep, WhenStep, ThenStep
from typing import Any
import pendulum
import uuid
from airflow.utils.session import provide_session
import os


class GivenDAG(GivenStep):
    def __init__(self, dag_or_dag_id: Any = None):
        self.dag = dag_or_dag_id

    def __call__(self, context: Context):
        from airflow.models.dag import DAG
        from airflow.models import DagBag

        if isinstance(self.dag, str):
            dagbag: DagBag = context["dagbag"]
            self.dag = dagbag.get_dag(self.dag)

        if not self.dag:
            self.dag = DAG(
                # create a unique dag_id
                dag_id=f"test_dag_{uuid.uuid4()}",
                # Set schedule_interval to None
                # to prevent the DAG from being scheduled
                schedule=None,
                # Set start date to 1 year ago
                start_date=pendulum.today("UTC").add(-365),
            )
        context["dag"] = self.dag


class GivenExecutionDate(GivenStep):
    def __init__(self, execution_date: Any):
        """Given the execution date."""
        if isinstance(execution_date, str):
            self.execution_date = pendulum.parse(execution_date)
        elif isinstance(execution_date, pendulum.DateTime):
            self.execution_date = execution_date
        else:
            raise ValueError(
                f"execution_date must be a string or a pendulum.DateTime, got {type(execution_date)}"
            )

    def __call__(self, context: Context):
        context["execution_date"] = self.execution_date


class GivenTask(GivenStep):
    def __init__(self, task: Any):
        self.task = task

    def __call__(self, context: Context):
        from airflow.models.dag import DAG
        if isinstance(self.task, str):
            dag: DAG = context["dag"]
            context["task"] = dag.get_task(self.task)
        else:
            dag: DAG = context["dag"]
            dag.add_task(self.task)
            context["task"] = self.task


class GivenDagBag(GivenStep):
    def __init__(self, dags_folder: str = None):
        self.dags_folder = dags_folder or os.environ.get(
            'AIRFLOW__CORE__DAGS_FOLDER')

    def __call__(self, context: Context):
        # Capture warnings
        import warnings
        from airflow.models import DagBag

        with warnings.catch_warnings(record=True) as captured_warnings:
            warnings.simplefilter("always")
            dagbag = DagBag(dag_folder=self.dags_folder,
                            include_examples=False)

        context["dag_bag_warnings"] = captured_warnings
        context["dagbag"] = dagbag


class WhenIGetDAG(WhenStep):
    def __call__(self, context: Context):
        context["it"] = context["dag"]


class WhenIRenderTheTask(WhenStep):
    @provide_session
    def __call__(self, context: Context, session=None):
        if "execution_date" not in context:
            GivenExecutionDate(pendulum.now())(context)

        from airflow.models.dagrun import DagRun
        from airflow.models.xcom import XCom
        from airflow.utils.state import State
        from airflow.utils.types import DagRunType
        from airflow.models.taskinstance import TaskInstance
        """Render the task. This is useful for testing the templated fields."""
        # Delete all previous DagRuns and Xcoms
        session.query(DagRun).delete()
        session.query(XCom).delete()

        # Create a DagRun
        dag_run = context["dag"].create_dagrun(
            run_id="test_dag_run",
            execution_date=context["execution_date"],
            start_date=context["execution_date"],
            state=State.RUNNING,
            run_type=DagRunType.MANUAL,
            session=session,
            conf=None,  # TODO: self.dag_run_conf,
        )
        ti: TaskInstance = dag_run.get_task_instance(
            context["task"].task_id, session=session)
        assert (
            ti is not None
        ), f"TaskInstance with task_id {context['task'].task_id} does not exist in the DagRun: {dag_run.get_task_instances(session=session)}"
        ti.refresh_from_task(context["dag"].get_task(ti.task_id))
        # Render the template fields
        # This sets the rendered variables on the self.task instance
        # so we can access them late, in the then statements
        ti.render_templates()
        context["task_instance"] = ti
        context.set_it(context["task"])


class WhenIExecuteTheTask(WhenStep):
    def __call__(self, context: Context):
        """Execute the task and save the results."""
        if "task_instance" not in context:
            WhenIRenderTheTask()(context)
        ti = context["task_instance"]
        
        context["output"] = ti.task.execute(ti.get_template_context())


a_dag = GivenDAG
the_dag = GivenDAG
a_task = GivenTask
the_task = GivenTask
dagbag = GivenDagBag
execution_date = GivenExecutionDate
get_dag = WhenIGetDAG
render_the_task = WhenIRenderTheTask
execute_the_task = WhenIExecuteTheTask
