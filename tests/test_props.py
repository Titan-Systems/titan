import unittest

from titan.props import AlertConditionProp


class TestProps(unittest.TestCase):
    def validate_identity(self, prop, sql):
        assert prop.render(prop.parse(sql)) == sql

    def test_alert_condition_prop(self):
        assert AlertConditionProp().parse("IF(EXISTS(SELECT 1))") == "SELECT 1"
        assert AlertConditionProp().parse("IF(EXISTS(SELECT func() from tbl))") == "SELECT func() from tbl"
        self.validate_identity(AlertConditionProp(), "IF(EXISTS( SELECT 1 ))")
