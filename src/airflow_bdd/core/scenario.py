from airflow_bdd.core.context import Context


class GivenStep:
    def __call__(self, context: Context):
        pass


class WhenStep:
    def __call__(self, context: Context):
        pass


class ThenStep:
    def __call__(self, context: Context):
        pass


class Scenario:
    """A scenario is in BDD terms an acceptance criteria.
    We therefore have methods for given, when and then.
    """
    def __init__(self):
        self.context = Context()

    def given(self, step: GivenStep):
        """A given step is a precondition to the test.
        """
        step(context=self.context)

    def when(self, step: WhenStep):
        """The when step is the action that triggers the test.
        """
        step(context=self.context)

    def then(self, step: ThenStep):
        """The then step is the assertion.
        """
        step(context=self.context)

    # Shorts to make things more readable
    given_I = given
    when_I = when
    then_I = then
    and_given = given
    and_given_I = given
    and_when = when
    and_when_I = when
    and_then = then
    and_then_I = then
