FROM python:3.11.16-slim@sha256:9534e5a8e315485d4061ed659af0fd78a284c015f9b73661b41d6bab25604534 AS base
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN useradd --create-home --uid 10001 app

FROM base AS test
COPY requirements-dev.txt .
RUN pip install --no-cache-dir -r requirements-dev.txt
COPY --chown=app:app . .
USER app
CMD ["python", "-m", "pytest", "-q"]

FROM base AS runtime
COPY --chown=app:app config ./config
COPY --chown=app:app etl ./etl
COPY --chown=app:app main.py etl_pipeline.py ./
USER app
ENTRYPOINT ["python", "main.py"]
