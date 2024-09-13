class Context:
    def __init__(self):
        self.context = {}

    def __getitem__(self, key):
        return self.context[key]

    def __setitem__(self, key, value):
        self.context[key] = value
        self.context["it"] = value
    
    def __contains__(self, key):
        return key in self.context

    def it(self):
        return self.context["it"]


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

    def given(self, GivenStep):
        """A given step is a precondition to the test.
        """
        GivenStep(context=self.context)

    def when(self, WhenStep):
        """The when step is the action that triggers the test.
        """
        WhenStep(context=self.context)

    def then(self, ThenStep):
        """The then step is the assertion.
        """
        ThenStep(context=self.context)

    and_given = given
    and_when = when
    and_then = then
    when_I = when
    then_I = then
