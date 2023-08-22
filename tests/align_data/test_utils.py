import pytest
from align_data.sources.utils import merge_dicts


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
    ([{'a': 0, 'b': 2}, {'b': None}], {'a': 0, 'b': 2}),
    ([{'a': None}, {'b': 'test'}], {'b': 'test'}),
])
def test_merge_dicts_with_none_values(input_dicts, expected):
    """Test merge_dicts function with dictionaries containing None or falsey values."""
    result = merge_dicts(*input_dicts)
    assert result == expected
