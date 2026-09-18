.PHONY: test integration clean-fixtures lock
test:
	docker build --target test -t mysql-bigquery-etl:test .
	docker run --rm mysql-bigquery-etl:test
integration:
	docker compose up --build --abort-on-container-exit --exit-code-from tests tests
clean-fixtures:
	docker compose down --volumes
lock:
	docker run --rm --user $$(id -u):$$(id -g) -e HOME=/tmp -v "$$(pwd):/app" -w /app python:3.11.16-slim@sha256:9534e5a8e315485d4061ed659af0fd78a284c015f9b73661b41d6bab25604534 sh -c 'pip install --user pip-tools==7.5.3 && /tmp/.local/bin/pip-compile --strip-extras --output-file=requirements.txt requirements.in && /tmp/.local/bin/pip-compile --strip-extras --output-file=requirements-dev.txt requirements-dev.in'
