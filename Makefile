.PHONY: install install-dev test style check

install:
	pip install -e .

install-dev:
	pip install -e ".[dev]"

test:
	python -m pytest

style:
	python -m black .

check: style test