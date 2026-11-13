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


# ---------------------------------------------------------------------------
# QassStreamFramer: reassembles records across TCP reads
# ---------------------------------------------------------------------------

import json  # noqa: E402

from qass_normalizer import QassStreamFramer  # noqa: E402

RECORDS = [
    {"measuring": "true", "e_current": 1, "note": "a {brace} and \"quote\""},
    {"measuring": "true", "e_current": 2, "note": "Prüfung ✓"},
    {"measuring": "false", "nested": {"x": [1, {"y": "}"}]}},
]
STREAM = ("\r\n".join(json.dumps(r, ensure_ascii=False) for r in RECORDS)).encode()


def parse_all(chunks):
    f = QassStreamFramer()
    texts = [t for c in chunks for t in f.feed(c)]
    return [json.loads(t) for t in texts], f


def test_framer_whole_stream_in_one_read():
    assert parse_all([STREAM])[0] == RECORDS


@pytest.mark.parametrize("cut", range(1, len(STREAM)))
def test_framer_any_split_point(cut):
    assert parse_all([STREAM[:cut], STREAM[cut:]])[0] == RECORDS


def test_framer_byte_by_byte():
    assert parse_all([STREAM[i:i + 1] for i in range(len(STREAM))])[0] == RECORDS


def test_framer_skips_junk_between_records():
    assert parse_all([b'garbage }} {"a": 1} xx \n {"b": 2}'])[0] == [{"a": 1}, {"b": 2}]


def test_framer_keeps_partial_until_completed():
    f = QassStreamFramer()
    assert f.feed(b'{"a": ') == []
    assert f.feed(b'1}{"b"') == ['{"a": 1}']
    assert f.feed(b': 2}') == ['{"b": 2}']


def test_framer_reset_drops_partial():
    f = QassStreamFramer()
    f.feed(b'{"a": 1, "b": ')
    f.reset()
    assert f.feed(b'{"c": 3}') == ['{"c": 3}']


def test_framer_overflow_drops_pending():
    f = QassStreamFramer(max_pending=50)
    assert f.feed(b'{"a": "' + b"x" * 100) == []
    assert f.overflowed == 1
    assert f.feed(b'{"ok": 1}') == ['{"ok": 1}']


def test_framer_output_is_accepted_by_normalizer():
    texts = QassStreamFramer().feed(STREAM)
    assert [r for t in texts for r in parse(t)] == RECORDS
