.PHONY: install install-dev test integration style check clean

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

clean:
	find . -name "__pycache__" -type d -exec rm -rf {} +