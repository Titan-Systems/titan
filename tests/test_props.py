import unittest

from titan.props import (
    AlertConditionProp,
    BoolProp,
    EnumProp,
    FlagProp,
    IdentifierProp,
    IntProp,
    Props,
    StringProp,
    TagsProp,
    TimeTravelProp,
)


class TestProps(unittest.TestCase):
    def validate_identity(self, prop, sql):
        assert prop.render(prop.parse(sql)) == sql

    def test_alert_condition_prop(self):
        assert AlertConditionProp().parse("IF(EXISTS(SELECT 1))") == "SELECT 1"
        assert AlertConditionProp().parse("IF(EXISTS(SELECT func() from tbl))") == "SELECT func() from tbl"
        self.validate_identity(AlertConditionProp(), "IF(EXISTS( SELECT 1 ))")

    def test_bool_prop(self):
        assert BoolProp("foo").parse("FOO = TRUE") is True
        assert BoolProp("bar").parse("bar = FALSE") is False
        self.validate_identity(BoolProp("boolprop"), "boolprop = TRUE")

    def test_identifier_prop(self):
        assert IdentifierProp("label").parse("label = value") == "value"
        assert IdentifierProp("label").parse('label = "value"') == '"value"'
        assert IdentifierProp("label").parse('label = "schema"."table"') == '"schema"."table"'
        assert IdentifierProp("label").parse('label = database."schema"."table"') == 'database."schema"."table"'
        assert (
            IdentifierProp("request_translator").parse('request_translator = "DB"."SCHEMA".function')
            == '"DB"."SCHEMA".function'
        )

    def test_string_prop(self):
        assert StringProp("multi label").parse("MULTI LABEL = VALUE") == "VALUE"

    def test_flag_prop(self):
        assert FlagProp("this is a flag").parse("this is a flag") is True
        assert FlagProp("this is another flag").parse("") is None

    def test_time_travel_prop(self):
        assert TimeTravelProp("at").parse("AT(TIMESTAMP => 123)") == {"TIMESTAMP": "123"}
