from hamcrest.core.base_matcher import BaseMatcher
from hamcrest import has_property, has_entry


class HasQuery(BaseMatcher):
    """Class for inspecting the SQL query in a BigQueryInsertJobOperator.
    It iterates through the configuration object of the class and finds the query.
    """

    def __init__(self, value_matcher):
        self.value_matcher = value_matcher

    def _matches(self, item):
        # Use nested matchers and pass the user-supplied matcher to the innermost match
        return has_property(
            "configuration", has_entry(
                "query", has_entry("query", self.value_matcher))
        )._matches(item)

    def describe_to(self, description):
        description.append_text(
            "an object with a 'configuration' property containing a 'query' entry where 'query' matches: "
        )
        self.value_matcher.describe_to(description)


def has_query(value_matcher):
    """
    Convenience function to create a HasQueryWithCustomCondition matcher.

    Args:
        value_matcher: The matcher to apply to the innermost 'query' value.

    Returns:
        A HasQueryWithCustomCondition matcher.
    """
    return HasQuery(value_matcher)
