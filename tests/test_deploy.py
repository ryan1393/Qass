"""Guards for deployment files and configs (they are part of the contract)."""

from __future__ import annotations

import json
import re
from pathlib import Path

import yaml

from conftest import REPO_ROOT

GOLDEN = Path(__file__).with_name("golden")


def test_configs_unchanged():
    """Tag names, IPs, ports and switches must not drift (comments may)."""
    want = json.loads((GOLDEN / "configs.json").read_text())
    got = {p.name: yaml.safe_load(p.read_text())
           for p in sorted((REPO_ROOT / "configs").glob("*.yaml"))}
    assert got == want


def test_compose_effective_services_unchanged():
    """Anchors/merge keys may be used, but each service must resolve to the same thing."""
    compose = yaml.safe_load((REPO_ROOT / "docker-compose.yml").read_text())
    compose.pop("version", None)
    for key in [k for k in compose if k.startswith("x-")]:
        compose.pop(key)
    want = json.loads((GOLDEN / "compose.json").read_text())
    assert compose == want


def test_dockerfile_contract():
    text = (REPO_ROOT / "Dockerfile").read_text()
    assert re.search(r"^FROM python:3\.11-slim\s*$", text, re.M)
    assert "curl -sf http://localhost:${HEALTH_PORT}/health" in text
    assert re.search(r"^HEALTHCHECK --interval=10s --timeout=3s --retries=3", text, re.M)
    assert 'CMD ["python3", "main.py"]' in text
    assert "COPY configs/ configs/" in text
    # every runtime module in the repo root must be copied into the image
    copied = set(re.findall(r"^COPY (\w+\.py) \.\s*$", text, re.M))
    modules = {p.name for p in REPO_ROOT.glob("*.py")}
    assert modules <= copied, f"not copied into image: {modules - copied}"
