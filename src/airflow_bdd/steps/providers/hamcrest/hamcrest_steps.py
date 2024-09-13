from airflow_bdd.scenario import Context, ThenStep
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

class ThenDAGShould(ThenStep):
    def __init__(self, matcher):
        self.matcher = matcher

    def __call__(self, context: Context):
        assert_that(context["dag"], self.matcher)

it_should = ThenItShould
task_should = ThenTaskShould
dag_should = ThenDAGShould