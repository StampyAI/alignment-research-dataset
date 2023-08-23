import pytest
from align_data.sources.arxiv_papers import get_id, canonical_url, get_version


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


@pytest.mark.parametrize(
    "url, expected",
    (
        ("http://bla.bla", "http://bla.bla"),
        ("http://arxiv.org/abs/2001.11038", "https://arxiv.org/abs/2001.11038"),
        ("https://arxiv.org/abs/2001.11038", "https://arxiv.org/abs/2001.11038"),
        ("https://arxiv.org/abs/2001.11038/", "https://arxiv.org/abs/2001.11038"),
        ("https://arxiv.org/pdf/2001.11038", "https://arxiv.org/abs/2001.11038"),
        ("https://arxiv.org/pdf/2001.11038.pdf", "https://arxiv.org/abs/2001.11038"),
        ("https://arxiv.org/pdf/2001.11038v3.pdf", "https://arxiv.org/abs/2001.11038"),
        ("https://arxiv.org/abs/math/2001.11038", "https://arxiv.org/abs/math/2001.11038"),
    ),
)
def test_canonical_url(url, expected):
    assert canonical_url(url) == expected


@pytest.mark.parametrize(
    "id, version",
    (
        ("123.123", None),
        ("math/312", None),
        ("3123123v1", "1"),
        ("3123123v123", "123"),
    ),
)
def test_get_version(id, version):
    assert get_version(id) == version
