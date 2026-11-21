"""Pytest bootstrap.

The self-tests are run with ``python -I -m pytest`` (isolated mode), which does
not add the current directory to ``sys.path``; insert the project root here so
the ``app`` package is importable.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
