from __future__ import annotations

import importlib.metadata

import scf_tools as m


def test_version():
    assert importlib.metadata.version("scf_tools") == m.__version__
