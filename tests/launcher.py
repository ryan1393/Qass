"""Run a copy of main.py in a subprocess with ``pylogix.PLC`` replaced by the fake.

Usage: python launcher.py <app_dir> <out.json> <serial> [main.py args...]

At exit it writes the recorded PLC calls and the root logger's handler setup to
<out.json> so the parent test can assert on them.
"""

import atexit
import json
import logging
import os
import runpy
import sys

app_dir, out_path, serial, *main_args = sys.argv[1:]
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from fakes import PlcSim  # noqa: E402

import pylogix  # noqa: E402

sim = PlcSim(write_delay=0)
sim.tags[os.environ["FAKE_SERIAL_TAG"]] = serial
pylogix.PLC = sim.factory()


def _dump() -> None:
    handlers = []
    for h in logging.getLogger().handlers:
        handlers.append({
            "cls": type(h).__name__,
            "maxBytes": getattr(h, "maxBytes", None),
            "backupCount": getattr(h, "backupCount", None),
            "fmt": h.formatter._fmt if h.formatter else None,
            "file": getattr(h, "baseFilename", None),
        })
    with open(out_path, "w") as f:
        json.dump({
            "calls": [[c.kind, c.tag, c.value] for c in sim.calls],
            "handlers": handlers,
            "level": logging.getLogger().level,
        }, f)


atexit.register(_dump)
sys.path.insert(0, app_dir)
os.chdir(app_dir)
sys.argv = ["main.py", *main_args]
runpy.run_path(os.path.join(app_dir, "main.py"), run_name="__main__")
