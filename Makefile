.PHONY: install install-dev test integration style check

install:
	pip install -e .

install-dev:
	pip install -e ".[dev]"

test:
	python -m pytest

integration:
	python -m pytest --snowflake

style:
	python -m black .
	codespell .

check: style test