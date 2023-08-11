import unittest

from titan.enums import DataType
from titan.resources import Database
from titan.props import (
    AlertConditionProp,
    BoolProp,
    EnumProp,
    FlagProp,
    IdentifierProp,
    IntProp,
    StringProp,
    TagsProp,
    TimeTravelProp,
)


class TestProp(unittest.TestCase):
    def validate_identity(self, prop, sql):
        assert prop.render(prop.parse(sql)) == sql

    def test_prop_alert_condition(self):
        assert AlertConditionProp().parse("IF(EXISTS(SELECT 1))") == "SELECT 1"
        assert AlertConditionProp().parse("IF(EXISTS(SELECT func() from tbl))") == "SELECT func() from tbl"
        self.validate_identity(AlertConditionProp(), "IF(EXISTS( SELECT 1 ))")

    def test_prop_bool(self):
        assert BoolProp("foo").parse("FOO = TRUE") is True
        assert BoolProp("bar").parse("bar = FALSE") is False
        self.validate_identity(BoolProp("boolprop"), "boolprop = TRUE")

    def test_prop_enum(self):
        self.assertEqual(EnumProp("data_type", DataType).parse("DATA_TYPE = VARCHAR"), DataType.VARCHAR)

    def test_prop_flag(self):
        self.assertEqual(FlagProp("this is a flag").parse("this is a flag"), True)
        self.assertEqual(FlagProp("this is another flag").parse(""), None)

    def test_prop_identifier(self):
        assert IdentifierProp("label").parse("label = value") == "value"
        assert IdentifierProp("label").parse('label = "value"') == '"value"'
        assert IdentifierProp("label").parse('label = "schema"."table"') == '"schema"."table"'
        assert IdentifierProp("label").parse('label = database."schema"."table"') == 'database."schema"."table"'
        assert (
            IdentifierProp("request_translator").parse('request_translator = "DB"."SCHEMA".function')
            == '"DB"."SCHEMA".function'
        )

    def test_prop_int(self):
        self.assertEqual(IntProp("int_prop").parse("int_prop = 42"), 42)

    def test_prop_string(self):
        self.assertEqual(StringProp("string_prop").parse("STRING_PROP = 'quoted value'"), "quoted value")
        self.assertEqual(StringProp("multi label").parse("MULTI LABEL = VALUE"), "VALUE")

    def test_prop_tags(self):
        self.assertDictEqual(TagsProp().parse("TAG (moon_phase = 'waxing')"), {"moon_phase": "waxing"})
        # self.assertDictEqual(TagsProp().parse("WITH TAG (a = 'b')"), {"a": "b"})

    def test_prop_time_travel(self):
        self.assertDictEqual(TimeTravelProp("at").parse("AT(TIMESTAMP => 123)"), {"TIMESTAMP": "123"})


class TestProps(unittest.TestCase):
    def test_props_render(self):
        db = Database(name="foo", comment="bar")
        rendered = db.props.render(db)
        self.assertEqual(
            rendered, "data_retention_time_in_days = 1 max_data_extension_time_in_days = 14 COMMENT = 'bar'"
        )
