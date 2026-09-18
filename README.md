# MySQL → BigQuery ETL Pipeline

Welcome to the **mysql-bigquery-etl** project! This is a robust, flexible, and developer-friendly ETL pipeline that extracts data from MySQL, transforms it, and loads it into Google BigQuery. Perfect for analytics, reporting, and data warehousing.

---

## Current correctness scope

The worker stages and validates batches before publishing destination changes,
checkpoint progress and run completion in one BigQuery transaction. It uses
versioned state tables, stable job IDs and a fenced writer lease. Products use
snapshot replacement, including an intentionally empty source. Users and orders
currently extract by ID and upsert by key; updates and deletes still require the
next cursor/reconciliation task. Explicit version-1 commerce schemas reject drift,
invalid required values and duplicate batch keys. Money uses decimal arithmetic.

Local tests pass; the distributed guarantees still require the opt-in real-cloud
suite. See [recovery and cloud validation](docs/runbooks/recovery.md). Existing
warehouse tables require a deliberate schema/data migration; use a fresh sandbox
for first validation.

## Features
- Incremental & full data loads
- Modular transformations (add your own!)
- Configurable via `.env` or runtime-injected Secret Manager values
- Logging, error handling, and metadata tracking
- Ready for local dev, Docker, or Google Cloud Build

---

## Prerequisites
- Python 3.11+
- MySQL server (local or remote)
- Google Cloud project with BigQuery enabled
- [gcloud CLI](https://cloud.google.com/sdk/docs/install) (for authentication)

---

## Reproducible development checks

Docker and Docker Compose provide the same Python 3.11 runtime as CI:

```sh
make test          # cloud-free unit and regression tests
make integration   # disposable MySQL + deterministic commerce fixtures
make clean-fixtures
```

For native Python 3.11 development, install `requirements-dev.txt` and run
`python -m pytest`. `requirements.in` and `requirements-dev.in` are dependency
inputs; `make lock` regenerates exact transitive pins. Remaining work and validation gates are tracked in [the backlog](docs/backlog.md).
See [baseline evidence](docs/evidence/baseline.md) and [the roadmap](docs/portfolio-roadmap.html).

## Quickstart
1. **Clone the repo:**
   ```sh
   git clone https://github.com/abouguri/mysql-bigquery-etl.git
   cd mysql-bigquery-etl
   ```
2. **Set up Python & venv:**
   ```sh
   pyenv install 3.11.7  # if needed
   pyenv local 3.11.7
   python -m venv venv
   source venv/bin/activate
   pip install --upgrade pip
   pip install -r requirements.txt
   ```
3. **Configure environment:**
   - Copy and edit the example:
     ```sh
     cp .env.example .env
     # Edit .env with your MySQL & GCP details
     ```
4. **Authenticate with Google Cloud:**
   ```sh
   gcloud auth application-default login
   gcloud config set project <your-gcp-project-id>
   ```
5. **Create your BigQuery dataset:**
   - Go to the [BigQuery Console](https://console.cloud.google.com/bigquery) and create the dataset (e.g., `mysql_etl`).

6. **Run the pipeline!**
   ```sh
   python main.py
   ```

---

## Configuration
- All config is in `config/config.py` and `.env`.
- Uses the same environment-variable contract locally and in production. Cloud Run injects Secret Manager values; the application does not fetch secrets itself.
- Production validates required connection settings and SQL identifiers before opening clients. Error logs contain exception types, not raw driver errors or credentials.
- Edit `etl_tables` in `Config` to add/remove tables or transformations.

---

## Docker & Cloud Build
- Build and run with Docker:
  ```sh
  docker build -t mysql-bigquery-etl .
  docker run --env-file .env mysql-bigquery-etl
  ```
- Use `cloudbuild.yaml` for Google Cloud Build CI/CD.

---

## Extending & Hacking
- Add new transformations: just add a function or string key in `etl_pipeline.py`.
- Add more tables: update `etl_tables` in `Config`.
- Use your own secrets backend: extend `get_secret` in `Config`.

---

## Troubleshooting
- **MySQL connection errors?**
  - Is MySQL running and accessible from your machine?
  - Are your credentials in `.env` correct?
- **BigQuery errors?**
  - Is your dataset created?
  - Is your GCP project/billing enabled?
- **Dependency issues?**
  - Use `pip install --force-reinstall --no-cache-dir -r requirements.txt`
  - Downgrade numpy if needed: `pip install 'numpy<2'`

---

## Contributing
PRs, issues, and ideas are welcome! Make it yours, make it better, and have fun.

---

## License
MIT

---

> Made with Python and caffeine.
