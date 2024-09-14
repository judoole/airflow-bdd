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
    
    def add(self, key, value):
        self[key] = value

    def it(self):
        return self.context["it"]