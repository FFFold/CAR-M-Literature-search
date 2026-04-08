#!/usr/bin/env python3

import sys
from pathlib import Path


if __package__:
    from .pubmed_pipeline.cli import main
else:
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from pubmed_pipeline.cli import main


if __name__ == "__main__":
    raise SystemExit(main())
