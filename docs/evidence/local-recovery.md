# Local process-crash evidence

Run `make recovery-demo`. Raw measured results are in [local-recovery.csv](local-recovery.csv); the same assertions run in the automated suite. Recorded on 2026-09-19 with Python 3.11.16 in the test container.

Each scenario uses a fresh SQLite file. A separate Python worker calls `os._exit(86)` at a selected publication boundary, bypassing exception handlers, `finally` blocks and normal connection cleanup. The parent verifies the durable destination, checkpoint and audit state before recovery.

| Failure boundary | Durable state before recovery | Recovery |
|---|---|---|
| After staging | Empty target, checkpoint zero, unfinished run | Wait for actual lease expiry, acquire a new generation, publish once |
| Inside publication, before commit | Destination and checkpoint both rolled back | Recover journal, wait for expiry, publish once |
| After commit, before return | One row, checkpoint one, successful run | Resolve the original identity without consuming source again |
| Two separate workers acquiring | Exactly one successful owner, one busy response | No second run inserted |

The two pre-commit cases also verify that the old claim cannot renew after takeover and that no staging tables remain after successful recovery. Recovery timing starts after the parent observes the crashed child exit, and includes polling, takeover and republishing. Tests use a **two-second lease** to keep reproduction fast; the normal local default is 30 seconds. This small fixture does not establish a production recovery SLO. Process termination is not a simulation of disk loss, power failure or BigQuery behavior.

Interview explanation: “I put data, progress and success in one transaction, then killed the worker on both sides of commit. Before commit, recovery retries after lease expiry; after commit, the durable run identity prevents rereading and republishing. SQLite serializes writers, so these results validate the local implementation. BigQuery needs its own live tests.”
