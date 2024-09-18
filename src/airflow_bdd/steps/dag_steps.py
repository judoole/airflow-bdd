from airflow_bdd.core.scenario import Context, GivenStep, WhenStep
from typing import Any
import pendulum
import uuid
from airflow.utils.session import provide_session
from airflow.models.xcom import XCOM_RETURN_KEY
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
        context[self.dag.dag_id] = self.dag
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
        if "dag" not in context:
            GivenDAG()(context)
        dag: DAG = context["dag"]

        if isinstance(self.task, str):
            context["task"] = dag.get_task(self.task)
        else:
            dag.add_task(self.task)
            context["task"] = self.task


class GivenVariable(GivenStep):
    def __init__(self, key: str, value: Any):
        self.key = key
        self.value = value

    def __call__(self, context: Context):
        from airflow.models import Variable
        Variable.set(self.key, self.value)


class GivenDagRun(GivenStep):
    def __init__(self,
                 dag_id: str = None,
                 execution_date: pendulum.DateTime = None,
                 state: str = "running",
                 run_type: str = "manual",
                 conf: dict = None):
        self.dag_id = dag_id
        self.execution_date = execution_date
        self.state = state
        self.run_type = run_type
        self.conf = conf

    @provide_session
    def __call__(self, context: Context, session=None):
        if not self.dag_id and "dag" not in context:
            GivenDAG()(context)
            self.dag_id = context["dag"].dag_id
        elif not self.dag_id and "dag" in context:
            self.dag_id = context["dag"].dag_id
        if not self.execution_date and "execution_date" not in context:
            GivenExecutionDate(pendulum.now())(context)
            self.execution_date = context["execution_date"]
        elif not self.execution_date and "execution_date" in context:
            self.execution_date = context["execution_date"]

        dag_run = context[self.dag_id].create_dagrun(
            run_id=f"test_dag_run_{uuid.uuid4()}",
            execution_date=self.execution_date,
            start_date=self.execution_date,
            state=self.state,
            run_type=self.run_type,
            conf=self.conf,
            session=session,
        )
        context["dag_run"] = dag_run


class GivenXCom(GivenStep):
    def __init__(self,
                 task_id: str,
                 value: Any,
                 dag_id: str = None,
                 key: str = XCOM_RETURN_KEY):
        self.task_id = task_id
        self.value = value
        self.dag_id = dag_id
        self.key = key

    @provide_session
    def __call__(self, context: Context, session=None):
        from airflow.models.dagrun import DagRun
        if "dag_run" not in context:
            GivenDagRun(dag_id=self.dag_id)(context)
        dag_run: DagRun = context["dag_run"]
        # First the the task instance
        x_ti = dag_run.get_task_instance(self.task_id, session=session)
        assert (
            x_ti is not None
        ), f"TaskInstance with task_id {self.task_id} does not exist in the DagRun: {dag_run.task_instances}"
        # Refresh the task instance, from the DAG
        x_ti.refresh_from_task(context["dag"].get_task(x_ti.task_id))
        # Push the XCom
        x_ti.xcom_push(key=self.key, value=self.value, session=session)


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
    def __init__(self, task_id: str = None) -> None:
        self.task_id = task_id

    @provide_session
    def __call__(self, context: Context, session=None):
        if "execution_date" not in context:
            GivenExecutionDate(pendulum.now())(context)

        from airflow.models.taskinstance import TaskInstance

        # Create a DagRun
        if "dag_run" not in context:
            GivenDagRun()(context)
        dag_run = context["dag_run"]
        self.task_id = self.task_id or context["task"].task_id
        ti: TaskInstance = dag_run.get_task_instance(
            self.task_id, session=session)
        assert (
            ti is not None
        ), f"TaskInstance with task_id {self.task_id} does not exist in the DagRun: {dag_run.get_task_instances(session=session)}"
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
the_xcom = GivenXCom
variable = GivenVariable
dagbag = GivenDagBag
execution_date = GivenExecutionDate
get_dag = WhenIGetDAG
render_the_task = WhenIRenderTheTask
execute_the_task = WhenIExecuteTheTask
