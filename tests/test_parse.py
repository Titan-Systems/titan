import unittest

import pyparsing as pp

from titan.parse import FullyQualifiedIdentifier

IDENTIFIER_TEST_CASES = [
    ("tbl", ["tbl"]),
    ("schema.tbl", ["schema", "tbl"]),
    ("DB.SCHEMA", ["DB", "SCHEMA"]),
    ("db.schema.tbl", ["db", "schema", "tbl"]),
    ('"tbl"', ['"tbl"']),
    ('"schema"."tbl"', ['"schema"', '"tbl"']),
    ('"db"."schema"."tbl"', ['"db"', '"schema"', '"tbl"']),
    ('db."schema"."tbl"', ["db", '"schema"', '"tbl"']),
    ('"db".schema."tbl"', ['"db"', "schema", '"tbl"']),
    ('"db"."schema".tbl', ['"db"', '"schema"', "tbl"]),
]


class TestParse(unittest.TestCase):
    def test_parse_identifier(self):
        for test_case, result in IDENTIFIER_TEST_CASES:
            self.assertEqual(FullyQualifiedIdentifier.parse_string(test_case).as_list(), result)

    def test_parse_identifier_wrapped(self):
        for test_case, result in IDENTIFIER_TEST_CASES:
            self.assertEqual(pp.MatchFirst(FullyQualifiedIdentifier).parse_string(test_case).as_list(), result)
