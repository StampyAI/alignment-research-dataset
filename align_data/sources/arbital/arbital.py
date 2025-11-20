import logging
import time
from dataclasses import dataclass
from typing import Dict, Iterator, List

import requests

from align_data.common.alignment_dataset import AlignmentDataset
from align_data.settings import LW_GRAPHQL_ACCESS

logger = logging.getLogger(__name__)


def _merge_authors(*author_lists) -> List[str]:
    """Collect unique non-empty author display names."""
    names = []
    seen = set()
    for authors in author_lists:
        if not authors:
            continue
        for a in authors:
            if not a:
                continue
            name = a.get("displayName") if isinstance(a, dict) else a
            if name and name not in seen:
                names.append(name)
                seen.add(name)
    return names


@dataclass
class Arbital(AlignmentDataset):
    """
    Arbital dataset backed by LessWrong wikitags (isArbitalImport=true) via GraphQL.
    """

    base_url: str = "https://www.lesswrong.com/graphql"
    limit: int = 100  # paginated page size for GraphQL
    COOLDOWN = 1.0  # courteous pause between pages
    done_key = "slug"

    def _headers(self) -> Dict[str, str]:
        if not LW_GRAPHQL_ACCESS:
            raise ValueError(
                "LW_GRAPHQL_ACCESS env var is required (format 'header-name: header-value')"
            )

        headers = {
            "Content-Type": "application/json",
            "User-Agent": "alignment-research-dataset/arbital-lw",
        }
        header_name, header_val = LW_GRAPHQL_ACCESS.split(":", 1)
        headers[header_name.strip()] = header_val.strip()
        return headers

    def _fetch_page(self, offset: int) -> Dict:
        query = """
        query ArbitalTags($limit:Int!, $offset:Int!) {
          tags(selector:{allArbitalTags:{excludedTagIds:[]}}, limit:$limit, offset:$offset) {
            results {
              _id
              slug
              name
              createdAt
              textLastUpdatedAt
              isArbitalImport
              wikiOnly
              description {
                markdown
                plaintextMainText
                editedAt
                user { displayName slug }
              }
              description_latest
              contributors {
                contributors { user { displayName slug } }
                totalCount
              }
              arbitalLinkedPages {
                faster
                slower
                moreTechnical
                lessTechnical
                requirements
                teaches
                parents
                children
              }
            }
            totalCount
          }
        }
        """

        res = requests.post(
            self.base_url,
            headers=self._headers(),
            json={"query": query, "variables": {"limit": self.limit, "offset": offset}},
            timeout=30,
        )
        if res.status_code != 200:
            raise Exception(
                f"GraphQL request failed with status {res.status_code}: {res.text[:200]}"
            )
        data = res.json()
        if "errors" in data:
            raise Exception(f"GraphQL errors: {data['errors']}")
        if "data" not in data or "tags" not in data["data"]:
            raise Exception(f"Unexpected response structure: {data}")
        return data["data"]["tags"]

    @property
    def items_list(self) -> Iterator[Dict]:
        offset = 0
        while True:
            page = self._fetch_page(offset)
            results = page.get("results") or []
            for tag in results:
                yield tag
            offset += self.limit
            total = page.get("totalCount") or 0
            if offset >= total or not results:
                break
            time.sleep(self.COOLDOWN)

    def get_item_key(self, item: Dict) -> str:
        return item.get("slug") or item.get("_id")

    def _choose_text(self, tag: Dict) -> str:
        desc = tag.get("description") or {}
        return (
            desc.get("markdown")
            or desc.get("plaintextMainText")
            or tag.get("description_latest")
            or ""
        ).strip()

    def _get_published_date(self, tag: Dict):
        desc = tag.get("description") or {}
        return super()._get_published_date(
            desc.get("editedAt") or tag.get("textLastUpdatedAt") or tag.get("createdAt")
        )

    def _extract_authors(self, tag: Dict) -> List[str]:
        desc_user = (tag.get("description") or {}).get("user")
        contribs = (tag.get("contributors") or {}).get("contributors") or []
        contrib_users = [c.get("user") for c in contribs if c.get("user")]
        return _merge_authors([desc_user] if desc_user else [], contrib_users) or [
            "anonymous"
        ]

    def process_entry(self, tag: Dict):
        try:
            text = self._choose_text(tag)
            if not text:
                logger.warning("Skipping tag %s: no text", tag.get("slug"))
                return None

            return self.make_data_entry(
                {
                    "title": tag.get("name", ""),
                    "text": text,
                    "date_published": self._get_published_date(tag),
                    "url": f"https://www.lesswrong.com/tag/{tag.get('slug')}",
                    "source": self.name,
                    "source_type": "wikitag",
                    "authors": self._extract_authors(tag),
                    "slug": tag.get("slug"),
                    "is_arbital_import": tag.get("isArbitalImport", False),
                    "wiki_only": tag.get("wikiOnly", False),
                    "arbital_linked_pages": tag.get("arbitalLinkedPages"),
                }
            )
        except Exception as e:
            logger.error("Error processing Arbital wikitag %s: %s", tag.get("slug"), e)
            return None
