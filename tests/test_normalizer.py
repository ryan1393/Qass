"""qass_normalizer.split_and_parse_qass_json contract."""

import pytest

from qass_normalizer import split_and_parse_qass_json as parse


def test_single_object():
    assert parse('{"measuring": "true", "e_current": 5}') == [
        {"measuring": "true", "e_current": 5}
    ]


def test_surrounding_whitespace_is_ignored():
    assert parse('  \r\n{"a": 1}\n ') == [{"a": 1}]


def test_list_is_returned_as_is():
    assert parse('[{"a": 1}, {"a": 2}]') == [{"a": 1}, {"a": 2}]


def test_concatenated_objects_are_recovered():
    assert parse('{"a": 1}{"a": 2}{"a": 3}') == [{"a": 1}, {"a": 2}, {"a": 3}]


@pytest.mark.parametrize(
    "garbage",
    [
        "",
        "not json",
        '{"a": 1',  # truncated (first half of a split read)
        '"a": 1}',  # second half of a split read
        '{"a": 1}{"a": 2',  # whole object + truncated object
        "\x00\x01\x02",
    ],
)
def test_garbage_returns_empty_list(garbage):
    assert parse(garbage) == []


def test_scalar_json_is_wrapped_in_a_list():
    # Current behaviour: any valid JSON that is not a list is wrapped.
    assert parse("42") == [42]
