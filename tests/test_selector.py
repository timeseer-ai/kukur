"""Tests for SeriesSelector."""

# SPDX-FileCopyrightText: 2023 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

from kukur import SeriesSelector


def test_name_series_name() -> None:
    selector = SeriesSelector("source", {"series name": "a"}, "field")
    assert selector.name == "a::field"
    assert selector == SeriesSelector.from_name("source", selector.name)


def test_name_series_name_value() -> None:
    selector = SeriesSelector("source", {"series name": "a"}, "value")
    assert selector.name == "a"
    assert selector == SeriesSelector.from_name("source", selector.name)


def test_name() -> None:
    selector = SeriesSelector("source", {"tag-a": "a"}, "field")
    assert selector.name == "tag-a=a::field"
    assert selector == SeriesSelector.from_name("source", selector.name)


def test_name_value() -> None:
    selector = SeriesSelector("source", {"tag-a": "a"}, "value")
    assert selector.name == "tag-a=a"
    assert selector == SeriesSelector.from_name("source", selector.name)


def test_name_multiple_tags() -> None:
    selector = SeriesSelector("source", {"tag-a": "a", "tag-b": "b"}, "field")
    assert selector.name == "tag-a=a,tag-b=b::field"
    assert selector == SeriesSelector.from_name("source", selector.name)


def test_name_multiple_tags_series_name_first() -> None:
    selector = SeriesSelector(
        "source", {"tag-a": "a", "tag-b": "b", "series name": "c"}, "field"
    )
    assert selector.name == "c,tag-a=a,tag-b=b::field"
    assert selector == SeriesSelector.from_name("source", selector.name)


def test_from_name_strip_whitespace() -> None:
    selector = SeriesSelector(
        "source", {"tag-a": "a", "tag-b": "b", "series name": "c"}, "field"
    )
    assert selector == SeriesSelector.from_name("source", " c,tag-a=a,tag-b=b::field ")
