deps-compile:
	uv pip compile requirements.in >> requirements.txt
	uv pip compile requirements-dev.in >> requirements-dev.txt

deps-sync:
	uv pip sync requirements.txt requirements-dev.txt

test:
	pytest .

format:
	ruff format .

check:
	ruff check . --fix