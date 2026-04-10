import sys
from pathlib import Path

# Make tests/ importable so test resource modules (testdata/) can be imported.
sys.path.insert(0, str(Path(__file__).parent / "tests"))
