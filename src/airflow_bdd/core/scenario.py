class Context:
    """The Context object is just a dict, which has notion of "it".
    "it" is the last value that was put on the context.

    The context object itself keeps all your variables in between steps.
    So that you can reuse values in later steps.
    """
    def __init__(self):
        self.context = {}

    def __getitem__(self, key):
        return self.context[key]

    def __setitem__(self, key, value):
        self.context[key] = value
        self.context["it"] = value
    
    def __contains__(self, key):
        return key in self.context
    
    def set_it(self, value):
        self.context["it"] = value

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
