from datetime import datetime
from itertools import islice
import logging
import time
from dataclasses import dataclass, field, KW_ONLY
from pathlib import Path
from typing import List, Optional, Dict, Any, Set, Iterable, Tuple, Generator

import pytz
from sqlalchemy import select, Select, JSON
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import joinedload, Session
import jsonlines
from dateutil.parser import parse, ParserError
from tqdm import tqdm

from align_data.db.models import Article, Summary
from align_data.db.session import make_session
from align_data.settings import ARTICLE_MAIN_KEYS
from align_data.sources.utils import merge_dicts


logger = logging.getLogger(__name__)


@dataclass
class AlignmentDataset:
    """The base dataset class."""

    name: str
    """The name of the dataset."""

    _: KW_ONLY

    data_path: Path = Path(__file__).parent / "../../data/"
    """The path where data can be found. Usually a folder."""

    # Derived paths
    raw_data_path: Path = field(init=False)
    files_path: Path = field(init=False)

    # Internal housekeeping variables
    _outputted_items: Set[str] = field(default_factory=set, init=False)
    """A set of the ids of all previously processed items."""

    done_key = "id"
    """The key of the entry to use as the id when checking if already processed."""

    COOLDOWN = 0
    """An optional cool down between processing entries."""

    lazy_eval = False
    """Whether to lazy fetch items. This is nice in that it will start processing, but messes up the progress bar."""

    batch_size = 20
    """The number of items to collect before flushing to the database."""

    def __post_init__(self):
        self.data_path = self.data_path.resolve()

        self.raw_data_path = self.data_path / "raw"
        self.files_path = self.raw_data_path / self.name

    def __str__(self) -> str:
        return self.name

    def _add_authors(self, article: Article, authors: List[str]) -> Article:
        # TODO: Don't keep adding the same authors - come up with some way to reuse them
        article.authors = ",".join(authors)
        if len(article.authors) > 1024:
            article.authors = ",".join(article.authors[:1024].split(",")[:-1])
        return article

    def make_data_entry(self, data, **kwargs) -> Article:
        data = merge_dicts(data, kwargs)
        summary = data.pop("summary", None)
        authors = data.pop("authors", [])

        article = Article(
            pinecone_update_required=True,
            meta={k: v for k, v in data.items() if k not in ARTICLE_MAIN_KEYS and v is not None},
            **{k: v for k, v in data.items() if k in ARTICLE_MAIN_KEYS},
        )
        self._add_authors(article, authors)
        if summary:
            article.summaries.append(Summary(text=summary, source=self.name))
        return article

    def to_jsonl(self, out_path: Path | None = None, filename: str | None = None) -> Path:
        out_path = out_path or self.data_path
        filename = filename or f"{self.name}.jsonl"
        filepath = out_path / filename

        with jsonlines.open(filepath, "w") as jsonl_writer:
            for article in self.read_entries():
                jsonl_writer.write(article.to_dict())
        return filepath.resolve()

    @property
    def _query_items(self) -> Select[Tuple[Article]]:
        return select(Article).where(Article.source == self.name)

    def read_entries(self, sort_by=None) -> Iterable[Article]:
        """Iterate through all the saved entries."""
        with make_session() as session:
            query = self._query_items.options(joinedload(Article.summaries))
            if sort_by is not None:
                query = query.order_by(sort_by)
            
            result = session.scalars(query)
            for article in result.unique(): # removes duplicates
                yield article

    def _add_batch(self, session: Session, batch: tuple):
        session.add_all(batch)

    def add_entries(self, entries):
        def commit() -> bool:
            try:
                session.commit()
                return True
            except IntegrityError:
                session.rollback()
                return False

        with make_session() as session:
            items = iter(entries)
            while batch := tuple(islice(items, self.batch_size)):
                self._add_batch(session, batch)
                # there might be duplicates in the batch, so if they cause
                # an exception, try to commit them one by one
                if not commit():
                    for entry in batch:
                        session.add(entry)
                        if not commit():
                            logger.error(f"found duplicate of {entry}")

    def setup(self):
        self._outputted_items = self._load_outputted_items()

    @property
    def items_list(self) -> Iterable:
        """Returns a collection of items to be processed."""
        return []

    def get_item_key(self, item):
        """Get the identifier of the given `item` so it can be checked to see whether it's been output.

        The default assumption is that the `item` is a Path to a file.
        """
        return item.name

    def _load_outputted_items(self) -> Set[str]:
        """
        Loads the outputted items from the database and returns them as a set.
        
        if the done_key is not an attribute of Article, it will try to load it from the meta field.
        """
        with make_session() as session:
            if hasattr(Article, self.done_key):
                # This doesn't filter by self.name. The good thing about that is that it should handle a lot more
                # duplicates. The bad thing is that this could potentially return a massive amount of data if there
                # are lots of items.
                return set(session.scalars(select(getattr(Article, self.done_key))).all())
            # TODO: Properly handle this - it should create a proper SQL JSON select
            return {item.get(self.done_key) for item in session.scalars(select(Article.meta)).all()}

    def not_processed(self, item):
        # NOTE: `self._outputted_items` reads in all items. Which could potentially be a lot. If this starts to
        # cause problems (e.g. massive RAM usage, big slow downs) then it will have to be switched around, so that
        # this function runs a query to check if the item is in the database rather than first getting all done_keys.
        # If it get's to that level, consider batching it somehow
        return self.get_item_key(item) not in self._outputted_items

    def unprocessed_items(self, items=None) -> list | filter:
        """Return a list of all items to be processed.

        This will automatically remove any items that have already been processed,
        based on the contents of the output file.
        """
        self.setup()
        items = items or self.items_list

        items_to_process = filter(self.not_processed, items)

        # greedily fetch all items if not lazy eval. This makes the progress bar look nice
        if not self.lazy_eval:
            return list(items_to_process)

        return items_to_process

    def fetch_entries(self) -> Generator[Article, None, None]:
        """Get all entries to be written to the file."""
        for item in tqdm(self.unprocessed_items(), desc=f"Processing {self.name}"):
            entry = self.process_entry(item)
            if not entry:
                continue

            try:
                entry.verify_id_fields()
            except AssertionError as e:
                logger.error(e)
                continue

            yield entry

            if self.COOLDOWN:
                time.sleep(self.COOLDOWN)

    def process_entry(self, entry) -> Article | None:
        """Process a single entry."""
        raise NotImplementedError

    @staticmethod
    def _format_datetime(date: datetime) -> str:
        return date.strftime("%Y-%m-%dT%H:%M:%SZ")

    @staticmethod
    def _get_published_date(date) -> datetime | None:
        try:
            # Totally ignore any timezone info, forcing everything to UTC
            return parse(str(date)).replace(tzinfo=pytz.UTC)
        except ParserError:
            pass
        return None


class SummaryDataset(AlignmentDataset):
    def unprocessed_items(self, items=None) -> Iterable:
        # This breaks the possible lazy loading of the items. Should be fine...
        items = list(super().unprocessed_items(items))

        urls = map(self.get_item_key, items)
        with make_session() as session:
            articles = (
                session.query(Article)
                .options(joinedload(Article.summaries))
                .filter(Article.url.in_(urls))
            )
            self.articles = {a.url: a for a in articles if a.url}

        return items

    def _load_outputted_items(self) -> Set[str]:
        """Load the output file (if it exists) in order to know which items have already been output."""
        with make_session() as session:
            return set(
                session.scalars(
                    select(Article.url).join(Article.summaries).filter(Summary.source == self.name)
                )
            )

    def _add_batch(self, session: Session, batch: tuple):
        def merge(item):
            if prev := self.articles.get(item.url):
                return session.merge(item.update(prev))
            return item

        session.add_all(map(merge, batch))


@dataclass
class MultiDataset(AlignmentDataset):

    datasets: List[AlignmentDataset]

    @property
    def names(self):
        return [dataset.name for dataset in self.datasets]

    @property
    def items_list(self) -> Iterable:
        """Returns a collection of items to be processed."""
        return ((item, dataset) for dataset in self.datasets for item in dataset.items_list)

    def setup(self):
        for dataset in self.datasets:
            dataset.setup()

    def get_item_key(self, entry):
        item, dataset = entry
        return dataset.get_item_key(item)

    def process_entry(self, entry) -> Optional[Article]:
        item, dataset = entry
        article = dataset.process_entry(item)
        article.add_meta('initial_source', article.source)
        article.source = self.name

    def fetch_entries(self):
        for dataset in self.datasets:
            for article in dataset.fetch_entries():
                if article.source != self.name:
                    article.add_meta('initial_source', article.source)
                    article.source = self.name
                yield article
