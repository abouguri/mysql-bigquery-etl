.PHONY: test integration clean-fixtures lock
test:
	docker build --target test -t mysql-bigquery-etl:test .
	docker run --rm mysql-bigquery-etl:test
integration:
	docker compose run --build --rm tests
clean-fixtures:
	docker compose down --volumes
lock:
	docker run --rm --user $$(id -u):$$(id -g) -e HOME=/tmp -v "$$(pwd):/app" -w /app python:3.11.16-slim@sha256:9534e5a8e315485d4061ed659af0fd78a284c015f9b73661b41d6bab25604534 sh -c 'pip install --user pip-tools==7.5.3 && /tmp/.local/bin/pip-compile --strip-extras --output-file=requirements.txt requirements.in && /tmp/.local/bin/pip-compile --strip-extras --output-file=requirements-dev.txt requirements-dev.in'

.PHONY: demo demo-report
demo:
	LOCAL_UID=$$(id -u) LOCAL_GID=$$(id -g) docker compose --profile demo run --build --rm demo
demo-report:
	LOCAL_UID=$$(id -u) LOCAL_GID=$$(id -g) docker compose --profile demo run --rm --no-deps --entrypoint python demo -c 'import json; from etl.local_warehouse import LocalWarehouse; print(json.dumps(LocalWarehouse("/data/warehouse.sqlite").report(), indent=2))'

.PHONY: recovery-demo
recovery-demo:
	docker build --target test -t mysql-bigquery-etl-tests .
	docker run --rm --network none --user "$$(id -u):$$(id -g)" -v "$$(pwd):/app:ro" -v "$$(pwd)/docs/evidence:/evidence" mysql-bigquery-etl-tests python -m scripts.local_recovery --output /evidence/local-recovery.csv
