from dataclasses import dataclass
from typing import Any, Callable, Dict, Iterable, Optional, Union

from airtable import airtable
from align_data.db.models import Article

from align_data.settings import AIRTABLE_API_KEY, ARTICLE_MAIN_KEYS
from align_data.common.alignment_dataset import AlignmentDataset
from align_data.sources.articles.parsers import item_metadata
from align_data.sources.utils import merge_dicts


@dataclass
class AirtableDataset(AlignmentDataset):
    base_id: str
    table_id: str
    mappings: Dict[str, str]
    processors: Dict[str, Callable[[Any], str]]
    done_key = "url"

    def setup(self):
        if not AIRTABLE_API_KEY:
            raise ValueError("No AIRTABLE_API_KEY provided!")
        if not self.base_id:
            raise ValueError("No airtable base ID provided!")
        if not self.table_id:
            raise ValueError("No airtable table ID provided!")
        super().setup()
        self.at = airtable.Airtable(self.base_id, AIRTABLE_API_KEY)

    def map_cols(self, item: Dict[str, Dict[str, str]]) -> Optional[Dict[str, Optional[str]]]:
        fields = item.get("fields", {})

        def map_col(k):
            val = fields.get(self.mappings.get(k) or k)
            if processor := self.processors.get(k):
                val = processor(val)
            return val

        mapped = {k: map_col(k) for k in ARTICLE_MAIN_KEYS + ["summary"]}
        if (mapped.get("url") or "").startswith("http"):
            return mapped

    def get_item_key(self, item) -> str | None:
        return item.get("url")

    @property
    def items_list(self) -> Iterable[Dict[str, Union[str, None]]]:
        return filter(None, map(self.map_cols, self.at.iterate(self.table_id)))

    def process_entry(self, entry) -> Optional[Article]:
        contents = item_metadata(self.get_item_key(entry))
        if not contents:
            return None

        entry["date_published"] = self._get_published_date(entry.get("date_published"))
        return self.make_data_entry(merge_dicts(entry, contents), source=self.name)
