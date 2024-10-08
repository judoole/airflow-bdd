from airflow_bdd.core.scenario import Context, ThenStep
from hamcrest import assert_that


class ThenItShould(ThenStep):
    def __init__(self, matcher):
        self.matcher = matcher

    def __call__(self, context: Context):
        assert_that(context.it(), self.matcher)


class ThenTaskShould(ThenStep):
    def __init__(self, matcher):
        self.matcher = matcher

    def __call__(self, context: Context):
        assert_that(context["task"], self.matcher)


class ThenTaskInstanceShould(ThenStep):
    def __init__(self, matcher):
        self.matcher = matcher

    def __call__(self, context: Context):
        assert_that(context["task_instance"], self.matcher)


class ThenDAGShould(ThenStep):
    def __init__(self, matcher):
        self.matcher = matcher

    def __call__(self, context: Context):
        assert_that(context["dag"], self.matcher)


it_ = ThenItShould
it_should = ThenItShould
task_should = ThenTaskShould
task_ = ThenTaskShould
task_instance_ = ThenTaskInstanceShould
task_instance_should_ = ThenTaskInstanceShould
dag_should = ThenDAGShould
dag_ = ThenDAGShould
