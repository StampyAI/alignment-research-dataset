from unittest.mock import Mock, patch

from align_data import settings
from align_data.sources.articles.pdf import get_arxiv_link


def test_get_arxiv_link_passes_timeout():
    """The DOI handle lookup is a plain requests.get; it must carry a timeout so it cannot hang the run (#254)."""
    resp = Mock(status_code=200)
    resp.json.return_value = {
        "values": [{"type": "URL", "data": {"value": "https://arxiv.org/abs/1234.5678"}}]
    }
    with patch("requests.get", return_value=resp) as mock_get:
        assert get_arxiv_link("10.48550/arXiv.1234.5678") == "https://arxiv.org/pdf/1234.5678.pdf"
    assert mock_get.call_args.kwargs["timeout"] == settings.REQUEST_TIMEOUT
