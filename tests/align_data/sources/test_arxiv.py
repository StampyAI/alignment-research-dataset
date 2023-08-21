import pytest
from align_data.sources.arxiv_papers import get_id, canonical_url, get_version, merge_dicts


@pytest.mark.parametrize(
    "url, expected",
    (
        ("https://arxiv.org/abs/2001.11038", "2001.11038"),
        ("https://arxiv.org/abs/2001.11038/", "2001.11038"),
        ("https://bla.bla/2001.11038/", None),
    ),
)
def test_get_id(url, expected):
    assert get_id("https://arxiv.org/abs/2001.11038") == "2001.11038"


@pytest.mark.parametrize('url, expected', (
    ("http://bla.bla", "http://bla.bla"),
    ("http://arxiv.org/abs/2001.11038", "https://arxiv.org/abs/2001.11038"),
    ("https://arxiv.org/abs/2001.11038", "https://arxiv.org/abs/2001.11038"),
    ("https://arxiv.org/abs/2001.11038/", "https://arxiv.org/abs/2001.11038"),
    ("https://arxiv.org/pdf/2001.11038", "https://arxiv.org/abs/2001.11038"),
    ("https://arxiv.org/pdf/2001.11038.pdf", "https://arxiv.org/abs/2001.11038"),
    ("https://arxiv.org/pdf/2001.11038v3.pdf", "https://arxiv.org/abs/2001.11038"),
    ("https://arxiv.org/abs/math/2001.11038", "https://arxiv.org/abs/math/2001.11038"),
))
def test_canonical_url(url, expected):
    assert canonical_url(url) == expected


@pytest.mark.parametrize('id, version', (
    ('123.123', None),
    ('math/312', None),
    ('3123123v1', '1'),
    ('3123123v123', '123'),
))
def test_get_version(id, version):
    assert get_version(id) == version


def test_merge_dicts_no_args():
    """Test merge_dicts function with no arguments."""
    result = merge_dicts()
    assert result == {}


def test_merge_dicts_single_dict():
    """Test merge_dicts function with a single dictionary."""
    result = merge_dicts({'a': 1, 'b': 2})
    assert result == {'a': 1, 'b': 2}


def test_merge_dicts_dicts_with_no_overlap():
    """Test merge_dicts function with multiple dictionaries with no overlapping keys."""
    result = merge_dicts({'a': 1}, {'b': 2}, {'c': 3})
    assert result == {'a': 1, 'b': 2, 'c': 3}


def test_merge_dicts_dicts_with_overlap():
    """Test merge_dicts function with multiple dictionaries with overlapping keys."""
    result = merge_dicts({'a': 1, 'b': 2}, {'b': 3, 'c': 4}, {'c': 5, 'd': 6})
    assert result == {'a': 1, 'b': 3, 'c': 5, 'd': 6}


@pytest.mark.parametrize("input_dicts, expected", [
    ([{'a': 1, 'b': None}, {'b': 3}], {'a': 1, 'b': 3}),
    ([{'a': 0, 'b': 2}, {'b': None}], {'b': 2}),
    ([{'a': ''}, {'b': 'test'}], {'b': 'test'}),
])
def test_merge_dicts_with_none_or_falsey_values(input_dicts, expected):
    """Test merge_dicts function with dictionaries containing None or falsey values."""
    result = merge_dicts(*input_dicts)
    assert result == expected
