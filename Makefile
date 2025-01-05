deps-sync:
	uv sync

test:
	pytest .

format:
	ruff format .

check:
	ruff check . --fix

docs:
	uv export -o docs/requirements.txt && uv run sphinx-build -M html docs/source docs/build
