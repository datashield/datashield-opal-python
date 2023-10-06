install:
	poetry install

test:
	poetry run pytest

test-api-admin:
	poetry run pytest tests/test_api_admin.py

test-api-analysis:
	poetry run pytest tests/test_api_analysis.py

build:
	poetry build

publish:
	poetry publish --build

clean:
	rm -rf dist

local-install:
	pip install ./dist/datashield_opal-*.tar.gz 
