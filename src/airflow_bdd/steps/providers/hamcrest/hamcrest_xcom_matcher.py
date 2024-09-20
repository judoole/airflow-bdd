from hamcrest.core.base_matcher import BaseMatcher


class HasXcom(BaseMatcher):
    def __init__(self, key, value_matcher):
        self.key = key
        self.value_matcher = value_matcher

    def _matches(self, item):
        from airflow.models.xcom import XCom
        from airflow.models.taskinstance import TaskInstance

        item: TaskInstance
        self._xcom = XCom.get_value(ti_key=item.key, key=self.key)
        if self._xcom is None:
            return False  # Handle case when XCom does not exist
        return self.value_matcher.matches(self._xcom)

    def describe_to(self, description):
        """
       Describe the matcher in a human-readable way.
       """
        description.append_text(f"TaskInstance with XCom key 1{self.key}'")
        description.append_text(f" and the value({self._xcom}) matching ")
        self.value_matcher.describe_to(description)


def has_xcom(key, value_matcher):
    return HasXcom(key, value_matcher)
