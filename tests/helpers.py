import os

from titan.parse import _split_statements

FIXTURES_DIR = os.path.join(os.path.dirname(__file__), "fixtures")


def load_sql_fixtures(filename):
    with open(os.path.join(FIXTURES_DIR, filename), encoding="utf-8") as f:
        yield from _split_statements(f.read())
