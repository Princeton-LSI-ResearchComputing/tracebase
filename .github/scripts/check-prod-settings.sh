#!/usr/bin/env bash
set -euo pipefail

export DEBUG=True
export SECRET_KEY=dummy

python -c "
from TraceBase.settings import prod
print('prod.DEBUG =', prod.DEBUG)
assert prod.DEBUG is False
"
