import logging
from collections import namedtuple
from dataclasses import dataclass
from typing import List, Optional, Union, Tuple, NamedTuple
from pathlib import Path

import pandas as pd
from sqlalchemy import select, or_, Select
from align_data.common.alignment_dataset import AlignmentDataset
from align_data.db.models import Article
from align_data.sources.articles.parsers import item_metadata
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

class Item(NamedTuple):
    updates: NamedTuple
    article: Article

@dataclass
class ReplacerDataset(AlignmentDataset):
    csv_path: str | Path
    delimiter: str
    done_key = "url"

    def get_item_key(self, item):
        raise NotImplementedError

    @staticmethod
    def maybe(item, key):
        val = getattr(item, key, None)
        if pd.isna(val) or (isinstance(val, str) and not val.strip()):
            return None
        return val

    @property
    def items_list(self) -> List[Item]:
        df = pd.read_csv(self.csv_path, delimiter=self.delimiter)
        self.csv_items = [
            item for item in df.itertuples()
            if self.maybe(item, 'id') or self.maybe(item, 'hash_id')
        ]
        by_id = {id: item for item in self.csv_items if (id := self.maybe(item, 'id'))}
        by_hash_id = {hash_id: item for item in self.csv_items if (hash_id := self.maybe(item, 'hash_id'))}

        return [
            Item(
                updates=by_id.get(article._id) or by_hash_id.get(article.id),
                article=article
            )
            for article in self.read_entries()
        ]

    @property
    def _query_items(self) -> Select[Tuple[Article]]:
        ids = [i.id for i in self.csv_items if self.maybe(i, 'id')]
        hash_ids = [i.hash_id for i in self.csv_items if self.maybe(i, 'hash_id')]
        return select(Article).where(or_(Article.id.in_(hash_ids), Article._id.in_(ids)))

    def update_text(self, updates: NamedTuple, article: Article):
        # If the url is the same as it was before, and there isn't a source url provided, assume that the
        # previous text is still valid
        if article.url == self.maybe(updates, 'url') and not self.maybe(updates, 'source_url'):
            return

        # If no url found, then don't bother fetching the text - assume it was successfully fetched previously
        url = self.maybe(updates, 'source_url') or self.maybe(updates, 'url')
        if not url:
            return

        if article.url != url:
            article.add_meta('source_url', url)

        metadata = item_metadata(url)
        # Only change the text if it could be fetched - better to have outdated values than none
        if metadata.get('text'):
            article.text = metadata['text']
        article.status = metadata.get('error')

    def process_entry(self, item: Item) -> Article:
        updates, article = item

        for key in ['url', 'title', 'source', 'authors', 'comment', 'confidence']:
            value = self.maybe(updates, key)
            if value and getattr(article, key, None) != value:
                setattr(article, key, value)

        if date := getattr(updates, 'date_published', None):
            article.date_published = self._get_published_date(date)

        self.update_text(updates, article)
        article._set_id()

        return article

    def _add_batch(self, session: Session, batch: tuple):
        session.add_all(map(session.merge, batch))
