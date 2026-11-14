import yaml

from upload_to_huggingface import FEATURES, make_card, normalize


def test_normalize_maps_onto_shared_schema():
    row = normalize(
        {
            "id": "1",
            "text": "hello",
            "summaries": ["a summary"],
            "karma": "5",
            "tags": "single",
            "comments": [{"nested": {"x": 1}}],
            "bibliography_bib": [{"title": "b", "link": "l"}],
            "abstract": {"not": "a string"},
        }
    )
    assert list(row) == [f["name"] for f in FEATURES]
    assert row["summary"] == ["a summary"]
    assert row["karma"] == 5
    assert row["tags"] == ["single"]
    assert row["authors"] == []
    assert row["bibliography_bib"] == [{"title": "b"}]
    assert row["abstract"] == '{"not": "a string"}'
    assert row["doi"] is None
    assert "comments" not in row


def test_make_card_lists_configs():
    card = make_card(["arbital", "lesswrong"])
    metadata = yaml.safe_load(card.split("---")[1])
    configs = {c["config_name"]: c["data_files"] for c in metadata["configs"]}
    assert configs == {
        "all": ["arbital.jsonl", "lesswrong.jsonl"],
        "arbital": "arbital.jsonl",
        "lesswrong": "lesswrong.jsonl",
    }
    assert metadata["license"] == "other"
    assert {i["config_name"] for i in metadata["dataset_info"]} == set(configs)
    assert "## Licence" in card
