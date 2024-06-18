"""Test configuration merging."""

# SPDX-FileCopyrightText: 2024 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

from typing import Dict

from kukur.config import merge_fragment


def test_config_value() -> None:
    config: Dict = {}
    merge_fragment(config, {"k": "v"})
    assert config["k"] == "v"


def test_config_override_value() -> None:
    config: Dict = {"k": "old"}
    merge_fragment(config, {"k": "v"})
    assert config["k"] == "v"


def test_config_list() -> None:
    config: Dict = {}
    merge_fragment(config, {"list": [1]})
    assert config["list"] == [1]


def test_config_list_extend() -> None:
    config: Dict = {"list": [1]}
    merge_fragment(config, {"list": [2]})
    assert config["list"] == [1, 2]


def test_config_dict() -> None:
    config: Dict = {}
    merge_fragment(config, {"dict": {"k": "v"}})
    assert config["dict"] == {"k": "v"}


def test_config_dict_update() -> None:
    config: Dict = {"dict": {"a": "1"}}
    merge_fragment(config, {"dict": {"b": "2"}})
    assert config["dict"] == {"a": "1", "b": "2"}


def test_config_dict_of_dict_update() -> None:
    config: Dict = {"source": {"name": {"query": "q"}}}
    merge_fragment(config, {"source": {"name": {"connection": {"username": "user"}}}})
    assert config["source"] == {
        "name": {"connection": {"username": "user"}, "query": "q"}
    }
