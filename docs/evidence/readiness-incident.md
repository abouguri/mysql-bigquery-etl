# Local integration incident: healthy process, unavailable database

Date: 2026-09-18. Environment: disposable local Docker Compose fixture. This was a development incident, not a production outage.

**Symptom:** the initial integration suite failed with a refused TCP connection to `mysql:3306` even though Compose considered the database healthy.

**Cause:** `mysqladmin ping` reached the temporary initialization server over its local socket. That proved a MySQL process was alive, but did not prove the application could connect over TCP or that fixture initialization had finished.

**Resolution:** the health check now executes `SELECT COUNT(*) FROM orders` over TCP as the fixture application user. Compose starts tests only after network access, authentication and schema readiness succeed together.

**Verification:** a fresh fixture teardown/start completed with five passing baseline tests and three explicitly expected correctness failures. Later suites run against the same readiness contract and pass their expanded checks. No retry loop was added to hide the startup race.

**Tradeoff:** the readiness check depends on the fixture's orders table. That is appropriate for this deterministic test environment, but a generic production database health check should be designed around its own migration/readiness lifecycle.

**Presentation point:** process liveness and dependency readiness are different properties. A useful readiness probe tests the behavior the application actually needs.
