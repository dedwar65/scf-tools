from __future__ import annotations

import importlib.metadata

import fedsurvey as m


def test_version():
    assert importlib.metadata.version("fedsurvey") == m.__version__
