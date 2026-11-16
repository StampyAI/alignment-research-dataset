import pytest
from align_data.db.models import Article
import jsonlines
from unittest.mock import patch
from datetime import datetime
from dataclasses import dataclass
from pathlib import Path
from typing import List
from align_data.common.alignment_dataset import AlignmentDataset, MultiDataset


@pytest.fixture
def data_entries():
    dataset = AlignmentDataset(name="blaa")
    entries = [
        dataset.make_data_entry(
            {
                "text": f"line {i}",
                "date_published": f"day {i}",
                "source": f"source {i}",
                "title": str(i),
                "url": f"http://bla.bla.bla?page={i}",
            }
        )
        for i in range(5)
    ]
    for entry in entries:
        Article.before_write(None, None, entry)
    return entries


@pytest.fixture
def dataset():
    return AlignmentDataset(name="blaa")


def test_data_entry_default_fields():
    dataset = AlignmentDataset(name="blaa")
    entry = dataset.make_data_entry({})

    assert entry.to_dict() == {
        "date_published": None,
        "source": None,
        "source_type": None,
        "title": None,
        "url": None,
        "id": None,
        "text": None,
        "summaries": [],
        "authors": [],
    }


def test_data_entry_id_from_urls_and_title():
    data = {
        "key1": 12,
        "key2": 312,
        "url": "www.arbital.org",
        "title": "once upon a time",
    }
    dataset = AlignmentDataset(name="blaa")
    entry = dataset.make_data_entry(data)
    Article.before_write(None, None, entry)
    assert entry.to_dict() == dict(
        {
            "date_published": None,
            "id": "761edb1a245f56b2ece52d652658b8eb",
            "source": None,
            "source_type": None,
            "text": None,
            "summaries": [],
            "authors": [],
        },
        **data,
    )


@pytest.mark.parametrize(
    "item, error",
    (
        (
            {
                "key1": 12,
                "key2": 312,
                "title": "wikipedia goes to war on porcupines",
                "url": "asd",
            },
            "missing fields: date_published, source, text",
        ),
        (
            {
                "key1": 12,
                "key2": 312,
                "url": "www.wikipedia.org",
                "text": "asdasd",
                "title": "asdasd",
            },
            "missing fields: date_published, source",
        ),
        (
            {
                "key1": 12,
                "key2": 312,
                "url": "www.wikipedia.org",
                "title": "bla",
                "source": "dwe",
                "date_published": "dwe",
            },
            "missing fields: text",
        ),
        (
            {
                "key1": 12,
                "key2": 312,
                "url": "www.wikipedia.org",
                "title": "bla",
                "text": "asdasd",
                "date_published": "dwe",
            },
            "missing fields: source",
        ),
        (
            {
                "key1": 12,
                "key2": 312,
                "url": "www.wikipedia.org",
                "title": "bla",
                "text": "asdasd",
                "source": "dwe",
            },
            "missing fields: date_published",
        ),
    ),
)
def test_data_entry_missing(item, error):
    dataset = AlignmentDataset(name="blaa")
    entry = dataset.make_data_entry(item)
    Article.before_write(None, None, entry)
    assert entry.status == "Missing fields"
    assert entry.comments == error


def test_data_entry_verify_id_passes():
    dataset = AlignmentDataset(name="blaa")
    entry = dataset.make_data_entry(
        {
            "source": "arbital",
            "text": "once upon a time",
            "url": "www.arbital.org",
            "title": "once upon a time",
            "id": "761edb1a245f56b2ece52d652658b8eb",
        }
    )
    entry.verify_id()


def test_data_entry_verify_id_fails():
    dataset = AlignmentDataset(name="blaa")
    entry = dataset.make_data_entry(
        {
            "url": "www.arbital.org",
            "title": "once upon a time",
            "id": "f2b4e02fc1dd8ae43845e4f930f2d84f",
        }
    )
    expected = "Entry id f2b4e02fc1dd8ae43845e4f930f2d84f does not match id from id_fields: 761edb1a245f56b2ece52d652658b8eb"
    with pytest.raises(AssertionError, match=expected):
        entry.verify_id()

def test_generate_id_string():
    dataset = AlignmentDataset(name="blaa")
    entry = dataset.make_data_entry(
        {
            "url": "www.arbital.org",
            "title": "once upon a time",
            "id": "f2b4e02fc1dd8ae43845e4f930f2d84f",
        }
    )
    assert entry.generate_id_string() == b"wwwarbitalorg"


@pytest.mark.parametrize(
    "data, error",
    (
        ({"id": "123"}, "Entry is missing the following fields: \\['url'\\]"),
        (
            {"id": "123", "url": None},
            "Entry is missing the following fields: \\['url'\\]",
        ),
        (
            {"id": "123", "url": "", "title": ""},
            "Entry is missing the following fields: \\['url'\\]",
        ),
    ),
)
def test_data_entry_verify_fields_fails(data, error):
    dataset = AlignmentDataset(name="blaa")
    entry = dataset.make_data_entry(data)
    with pytest.raises(AssertionError, match=error):
        entry.verify_id_fields()


def test_data_entry_id_fields():
    dataset = AlignmentDataset(name="blaa")
    entry = dataset.make_data_entry(
        {"url": "https://www.google.ca/once_upon_a_time", "title": "bla"}
    )

    Article.before_write(None, None, entry)
    assert entry.id


def test_data_entry_different_id_from_different_url():
    dataset = AlignmentDataset(name="blaa")
    entry1 = dataset.make_data_entry({"url": " https://aisafety.info?state=6478"})
    entry2 = dataset.make_data_entry(
        {
            "source": "arbital",
            "text": "once upon a time",
            "url": " https://aisafety.info?state=6479",
        }
    )
    assert entry1.generate_id_string() != entry2.generate_id_string()


@pytest.fixture
def numbers_dataset():
    """Make a dataset that raises its items to the power of 2."""

    @dataclass
    class NumbersDataset(AlignmentDataset):
        nums: List[int]
        done_key = "number"

        @property
        def items_list(self):
            return self.nums

        def get_item_key(self, item) -> str:
            return str(item)

        def process_entry(self, item):
            return self.make_data_entry(
                {
                    "text": f"line {item}",
                    "date_published": f"day {item}",
                    "source": f"source {item}",
                    "title": str(item),
                    "url": f"http://bla.bla.bla?page={item}",
                    "number": item,
                    "value": item**2,
                    "authors": [],
                }
            )

    return NumbersDataset(name="numbers", nums=list(range(10)))


def test_unprocessed_items_fresh(numbers_dataset):
    """Getting the unprocessed items from a dataset that hasn't written anything should get all items."""
    seen = set()
    with patch.object(numbers_dataset, "_load_outputted_items", return_value=seen):
        assert list(numbers_dataset.unprocessed_items()) == list(range(10))


def test_unprocessed_items_all_done(numbers_dataset):
    """Getting the unprocessed items from a dataset that has already processed everything should return an empty list."""
    seen = set(str(i) for i in range(0, 10))
    with patch.object(numbers_dataset, "_load_outputted_items", return_value=seen):
        assert list(numbers_dataset.unprocessed_items()) == []


def test_unprocessed_items_some_done(numbers_dataset):
    """Getting the uprocessed items from a dataset that has partially completed should return the items that haven't been processed."""
    seen = set(str(i) for i in range(0, 10, 2))
    with patch.object(numbers_dataset, "_load_outputted_items", return_value=seen):
        assert list(numbers_dataset.unprocessed_items()) == list(range(1, 10, 2))


def test_fetch_entries(numbers_dataset):
    assert [i.meta["value"] for i in numbers_dataset.fetch_entries()] == [i**2 for i in range(10)]


def test_format_datatime(dataset):
    assert dataset._format_datetime(datetime(2022, 1, 1, 12, 23, 43)) == "2022-01-01T12:23:43Z"


def test_format_datatime_ignore_timezone(dataset):
    dt = datetime.fromisoformat("2022-01-01T00:00:00+04:00")
    assert dataset._format_datetime(dt) == "2022-01-01T00:00:00Z"


# --- MultiDataset: one failing sub-dataset must not stop the others (#254) -----
#
# A MultiDataset runs its sub-datasets one after another. With `continue_on_error`
# a sub-dataset that raises is skipped, the ones after it still run, and the run
# is failed at the end by `finish()` so the failure is not silent. Without it (the
# default) the first error still stops the run straight away, as before.


@dataclass
class _GroupedDataset(AlignmentDataset):
    """A sub-dataset yielding one article per number, failing where it is told to."""

    nums: List[int]
    fail_listing: bool = False  # raise while listing items, like a dead feed
    fail_on: int | None = None  # raise while processing this item
    finish_error: str | None = None
    finished: bool = False
    done_key = "number"

    @property
    def items_list(self):
        if self.fail_listing:
            raise ConnectionError(f"{self.name} is unreachable")
        return self.nums

    def get_item_key(self, item) -> str:
        return str(item)

    def _load_outputted_items(self):
        return set()

    def process_entry(self, item):
        if item == self.fail_on:
            raise ValueError(f"cannot process {item}")
        return self.make_data_entry(
            {
                "text": f"line {item}",
                "source": self.name,
                "title": str(item),
                "url": f"http://{self.name}.bla?page={item}",
                "number": item,
                "authors": [],
            }
        )

    def finish(self):
        self.finished = True
        if self.finish_error:
            raise RuntimeError(self.finish_error)


def _group(*datasets, **kwargs):
    return MultiDataset(name="group", datasets=list(datasets), **kwargs)


def _numbers(articles):
    return [a.meta["number"] for a in articles]


def test_multidataset_continues_past_a_sub_dataset_that_cannot_list_its_items():
    group = _group(
        _GroupedDataset(name="first", nums=[1, 2]),
        _GroupedDataset(name="dead", nums=[3, 4], fail_listing=True),
        _GroupedDataset(name="last", nums=[5, 6]),
        continue_on_error=True,
    )
    articles = list(group.fetch_entries())
    assert _numbers(articles) == [1, 2, 5, 6]
    # the healthy ones are stored the same way as in a run without failures
    assert {a.source for a in articles} == {"group"}
    assert [a.meta["initial_source"] for a in articles] == ["first", "first", "last", "last"]


def test_multidataset_keeps_what_a_sub_dataset_yielded_before_it_failed():
    group = _group(
        _GroupedDataset(name="first", nums=[1, 2]),
        _GroupedDataset(name="flaky", nums=[3, 4, 5], fail_on=4),
        _GroupedDataset(name="last", nums=[6]),
        continue_on_error=True,
    )
    assert _numbers(group.fetch_entries()) == [1, 2, 3, 6]


def test_multidataset_finish_fails_the_run_naming_what_failed_to_fetch():
    datasets = [
        _GroupedDataset(name="first", nums=[1]),
        _GroupedDataset(name="dead", nums=[2], fail_listing=True),
        _GroupedDataset(name="flaky", nums=[3], fail_on=3),
        _GroupedDataset(name="last", nums=[4]),
    ]
    group = _group(*datasets, continue_on_error=True)
    list(group.fetch_entries())
    with pytest.raises(RuntimeError) as err:
        group.finish()
    message = str(err.value)
    assert "dead: ConnectionError: dead is unreachable" in message
    assert "flaky: ValueError: cannot process 3" in message
    assert "first" not in message and "last" not in message
    # every sub-dataset still gets its own finish()
    assert all(d.finished for d in datasets)


def test_multidataset_finish_reports_fetch_and_finish_failures_together():
    group = _group(
        _GroupedDataset(name="dead", nums=[1], fail_listing=True),
        _GroupedDataset(name="last", nums=[2], finish_error="last: too many failed"),
        continue_on_error=True,
    )
    list(group.fetch_entries())
    with pytest.raises(RuntimeError, match="dead is unreachable.*last: too many failed"):
        group.finish()


def test_multidataset_finish_passes_when_nothing_failed():
    group = _group(
        _GroupedDataset(name="first", nums=[1]),
        _GroupedDataset(name="last", nums=[2]),
        continue_on_error=True,
    )
    assert _numbers(group.fetch_entries()) == [1, 2]
    group.finish()


def test_multidataset_logs_the_failure_when_it_happens(caplog):
    group = _group(
        _GroupedDataset(name="dead", nums=[1], fail_listing=True), continue_on_error=True
    )
    with caplog.at_level("ERROR"):
        list(group.fetch_entries())
    assert any(
        r.levelname == "ERROR" and "dead" in r.getMessage() and r.exc_info for r in caplog.records
    )


def test_multidataset_stops_at_the_first_error_by_default():
    """Unchanged behaviour for groups that don't opt in (e.g. YouTube, which raises on
    purpose to stop the whole run when its proxy is down)."""
    last = _GroupedDataset(name="last", nums=[5, 6])
    group = _group(
        _GroupedDataset(name="first", nums=[1, 2]),
        _GroupedDataset(name="dead", nums=[3, 4], fail_listing=True),
        last,
    )
    seen = []
    with pytest.raises(ConnectionError, match="dead is unreachable"):
        for article in group.fetch_entries():
            seen.append(article.meta["number"])
    assert seen == [1, 2]


def test_multidataset_does_not_swallow_an_interrupted_run():
    """Only errors are isolated: stopping the run (Ctrl-C, a cancelled job) still stops it."""
    dataset = _GroupedDataset(name="first", nums=[1])
    group = _group(dataset, _GroupedDataset(name="last", nums=[2]), continue_on_error=True)
    with patch.object(_GroupedDataset, "process_entry", side_effect=KeyboardInterrupt):
        with pytest.raises(KeyboardInterrupt):
            list(group.fetch_entries())
