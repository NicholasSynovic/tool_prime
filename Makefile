build:
	git describe --tags --abbrev=0 | tail -n 1 | xargs -I % uv version %
	rm -rf dist/
	uv build
	uv pip install dist/*.tar.gz

create-dev:
	pre-commit install
	pre-commit autoupdate
	uv sync
	uv build
