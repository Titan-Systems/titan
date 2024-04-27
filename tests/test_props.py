import unittest

from pyparsing import ParseException

from titan.enums import DataType
from titan.resources import Database
from titan.props import (
    AlertConditionProp,
    BoolProp,
    ArgsProp,
    EnumProp,
    EnumFlagProp,
    FlagProp,
    IdentifierProp,
    IntProp,
    StringProp,
    TagsProp,
    TimeTravelProp,
)


class TestProp(unittest.TestCase):
    def validate_identity(self, prop, sql):
        self.assertEqual(prop.render(prop.parse(sql)), sql)

    def test_prop_alert_condition(self):
        assert AlertConditionProp().parse("IF(EXISTS(SELECT 1))") == "SELECT 1"
        assert AlertConditionProp().parse("IF(EXISTS(SELECT func() from tbl))") == "SELECT func() from tbl"
        self.validate_identity(AlertConditionProp(), "IF(EXISTS( SELECT 1 ))")

    def test_prop_bool(self):
        self.assertTrue(BoolProp("foo").parse("FOO = TRUE"))
        self.assertFalse(BoolProp("bar").parse("bar = FALSE"))
        self.validate_identity(BoolProp("boolprop"), "BOOLPROP = TRUE")

    def test_prop_args(self):
        self.assertEqual(ArgsProp().parse("(id INT)"), [{"name": "id", "data_type": DataType.INT}])
        self.assertEqual(ArgsProp().parse("(somestr VARCHAR)"), [{"name": "somestr", "data_type": DataType.VARCHAR}])
        self.assertEqual(ArgsProp().parse("(floaty FLOAT8)"), [{"name": "floaty", "data_type": DataType.FLOAT8}])
        self.assertEqual(
            ArgsProp().parse("(multiple INT, columns VARCHAR)"),
            [
                {"name": "multiple", "data_type": DataType.INT},
                {"name": "columns", "data_type": DataType.VARCHAR},
            ],
        )
        self.assertEqual(
            ArgsProp().parse("(id INT, name STRING, created_at TIMESTAMP)"),
            [
                {"name": "id", "data_type": DataType.INT},
                {"name": "name", "data_type": DataType.STRING},
                {"name": "created_at", "data_type": DataType.TIMESTAMP},
            ],
        )

    def test_prop_enum(self):
        self.assertEqual(EnumProp("data_type", DataType).parse("DATA_TYPE = VARCHAR"), DataType.VARCHAR)

    def test_prop_flag(self):
        self.assertEqual(FlagProp("this is a flag").parse("this is a flag"), True)
        self.assertRaises(ParseException, lambda: FlagProp("this is another flag").parse(""))

    def test_prop_enum_flag(self):
        self.assertEqual(EnumFlagProp(DataType).parse("VARCHAR"), DataType.VARCHAR)

    def test_prop_identifier(self):
        self.assertEqual(IdentifierProp("label").parse("label = value"), "value")
        self.assertEqual(IdentifierProp("label").parse('label = "value"'), '"value"')
        self.assertEqual(IdentifierProp("label").parse('label = "schema"."table"'), '"schema"."table"')
        self.assertEqual(
            IdentifierProp("label").parse('label = database."schema"."table"'), 'database."schema"."table"'
        )
        self.assertEqual(
            IdentifierProp("request_translator").parse('request_translator = "DB"."SCHEMA".function'),
            '"DB"."SCHEMA".function',
        )

    def test_prop_int(self):
        self.assertEqual(IntProp("int_prop").parse("int_prop = 42"), 42)

    def test_prop_string(self):
        self.assertEqual(StringProp("string_prop").parse("STRING_PROP = 'quoted value'"), "quoted value")
        self.assertEqual(StringProp("multi label").parse("MULTI LABEL = VALUE"), "VALUE")

    def test_prop_tags(self):
        self.assertDictEqual(TagsProp().parse("TAG (moon_phase = 'waxing')"), {"moon_phase": "waxing"})
        self.assertDictEqual(TagsProp().parse("WITH TAG (a = 'b')"), {"a": "b"})

    def test_prop_time_travel(self):
        self.assertDictEqual(TimeTravelProp("at").parse("AT(TIMESTAMP => 123)"), {"TIMESTAMP": "123"})


class TestProps(unittest.TestCase):
    def test_props_render(self):
        db = Database(name="foo", comment="bar")
        rendered = db.props.render(db.to_dict())
        self.assertEqual(
            rendered, "DATA_RETENTION_TIME_IN_DAYS = 1 MAX_DATA_EXTENSION_TIME_IN_DAYS = 14 COMMENT = $$bar$$"
        )
