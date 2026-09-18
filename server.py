"""Legacy entry point retained for CLI compatibility; HTTP execution was removed."""
from main import main

if __name__ == "__main__":
    raise SystemExit(main())
