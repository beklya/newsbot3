"""
conftest.py at project root.

Auto-loaded by pytest. Adds the project root to sys.path so that
tests can import 'src.*' modules without needing PYTHONPATH or
package installation.

This is a standard pattern for src-layout Python projects.
"""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
