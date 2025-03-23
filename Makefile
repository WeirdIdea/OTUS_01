.PHONY: test, typing, lint, format, import-sort

test:
	poetry run pytest

typing:
	poetry run mypy .

lint:
	poetry run ruff check

format:
	poetry run black .

import-sort:
	poetry run isort .
