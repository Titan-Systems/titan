.PHONY: install install-dev test integration style check clean build
EDITION ?= standard

install:
	pip install -e .

install-dev:
	pip install -e ".[dev]"

test:
	python -m pytest

integration:
	python -m pytest --snowflake -m $(EDITION)

style:
	python -m black .
	codespell .

check: style test

clean:
	rm -rf build dist *.egg-info
	find . -name "__pycache__" -type d -exec rm -rf {} +

build:
	mkdir -p dist
	zip -vrX dist/titan-$(shell python setup.py -V).zip titan/

docs:
	python tools/generate_resource_docs.py