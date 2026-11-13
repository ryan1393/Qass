"""Startup config validation (config.py)."""

from __future__ import annotations

import copy
import logging
from pathlib import Path

import pytest

from config import ConfigError, load_config, resolve_config_path, validate_config
from conftest import REPO_ROOT, make_cfg
from paths import CONFIG_DIR

MACHINE_CONFIGS = sorted((REPO_ROOT / "configs").glob("config_machine*.yaml"))


@pytest.mark.parametrize("path", MACHINE_CONFIGS, ids=lambda p: p.name)
def test_shipped_machine_configs_are_valid(path):
    cfg = load_config(path)
    assert cfg["qass"]["type"] in ("auto", "manual")


def test_default_yaml_is_rejected_with_a_clear_message():
    # default.yaml is type 'auto' without plc.current_tag. The old code failed
    # on it too (KeyError: 'current_tag'); now the message says why.
    with pytest.raises(ConfigError, match=r"plc\.current_tag \(required when qass\.type is 'auto'\)"):
        load_config(REPO_ROOT / "configs" / "default.yaml")


def test_validate_returns_config_unchanged():
    cfg = make_cfg(1, 2)
    before = copy.deepcopy(cfg)
    assert validate_config(cfg) == before
    assert cfg == before  # no defaults injected


def test_optional_keys_are_optional():
    cfg = make_cfg(1, 2)
    del cfg["plc"]["enable_writes"]
    del cfg["qass"]["disconnect_timeout_sec"]
    validate_config(cfg)


def test_manual_does_not_need_current_tag():
    cfg = make_cfg(1, 2, qass_type="manual")
    del cfg["plc"]["current_tag"]
    validate_config(cfg)


def test_all_missing_keys_listed_in_one_message():
    cfg = make_cfg(1, 2)
    del cfg["qass"]["ip"]
    del cfg["plc"]["heartbeat_tag"]
    del cfg["reconnect"]
    with pytest.raises(ConfigError) as e:
        validate_config(cfg, "configs/x.yaml")
    msg = str(e.value)
    assert msg.startswith("configs/x.yaml: missing required key(s):")
    for key in ("qass.ip", "plc.heartbeat_tag", "reconnect (section)"):
        assert key in msg


@pytest.mark.parametrize("bad", [None, [], "text"])
def test_non_mapping_rejected(bad):
    with pytest.raises(ConfigError, match="expected a YAML mapping"):
        validate_config(bad)


def test_unknown_type_only_warns(caplog):
    caplog.set_level(logging.WARNING)
    validate_config(make_cfg(1, 2, qass_type="semi"))
    assert "qass.type='semi'" in caplog.text


@pytest.mark.parametrize("arg,expected", [
    (None, CONFIG_DIR / "default.yaml"),
    ("configs/config_machine1.yaml", CONFIG_DIR / "config_machine1.yaml"),
    ("config_machine1.yaml", CONFIG_DIR / "config_machine1.yaml"),
    ("x/y/z/config_machine2.yaml", CONFIG_DIR / "config_machine2.yaml"),
    ("/etc/qass/m.yaml", Path("/etc/qass/m.yaml")),
])
def test_resolve_config_path(arg, expected):
    assert resolve_config_path(arg) == expected
