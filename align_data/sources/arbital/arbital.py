import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
import logging
from typing import List, Tuple, Iterator, Dict, Union, Any, TypedDict

import requests
from dateutil.parser import parse

from align_data.common.alignment_dataset import AlignmentDataset

logger = logging.getLogger(__name__)


class Page(TypedDict, total=False):
    text: str
    likeableId: str
    likeableType: str
    title: str
    editCreatedAt: str
    pageCreatedAt: str
    alias: str
    userId: str
    tagIds: str
    changeLogs: List[Dict[str, Any]] 


def parse_arbital_link(internal_link: str) -> str:
    """
    Parses the Arbital internal link.
    :param str internal_link: The internal link to parse.

    :return: The parsed link.
    :rtype: str

    Typical format: `123 Some title` -> `[Some title](https://arbital.com/p/123)`
    Special cases: 
        `toc:` -> `toc:`
        `https://www.gwern.net/ Gwern Branwen` -> `[Gwern Branwen](https://www.gwern.net/)`
    """
    page_id, *title_parts = internal_link.split(" ")
    if not page_id or page_id.startswith("toc:"):
        # could be a regular text bracket, ignore it
        return internal_link
    if page_id.startswith("http"):
        # could be a regular link, ignore it
        return f"[{' '.join(title_parts)}]({page_id})"
    url = f"https://arbital.com/p/{page_id}"
    title = " ".join(title_parts) if title_parts else url
    return f"[{title}]({url})"


def flatten(val: Union[List[str], Tuple[str], str]) -> List[str]:
    """Flattens a nested list."""
    if isinstance(val, (list, tuple)):
        return [item for sublist in val for item in flatten(sublist)]
    return [val]


def markdownify_text(current: List[str], view: Iterator[Tuple[str, str]]) -> Tuple[List[str], str]:
    """
    Recursively parse text segments from `view` to generate a markdown Abstract Syntax Tree (AST).
    
    This function helps in transitioning from Arbital's specific markdown extensions to standard markdown. It specifically
    handles two main features:
    - "[summary: <contents>]" blocks, which are used in Arbital to add summaries.
    - "[123 <title>]" which are Arbital's internal links pointing to https://arbital.com/p/123, with link title <title>.
    
    Args:
    :param List[str] current: A list of parsed items. Should generally be initialized as an empty list.
    :param Iterator[Tuple[str, str]] view: An iterator that returns pairs of `part` and `next_part`, where `part` is the 
        current segment and `next_part` provides a lookahead.
    
    :return: <summaries>, <text>, where <summaries> are the summaries extracted from the text, and <text> is the text with all
        Arbital-specific markdown extensions replaced with standard markdown.
    :rtype: Tuple[List[str], str]
    
    Example:
    From the text: "[summary: A behaviorist [6w genie]]"
    We get the input:
        current = []
        view = iter([('[', 'summary: A behaviorist '), ('summary: A behaviorist ', '['), ('[', '6w genie'), ('6w genie', ']'), (']', ']'), (']', None)])
    The function should return:
        `(['A behaviorist [genie](https://arbital.com/p/6w)'], '')`
    
    Note:
    This function assumes that `view` provides a valid Arbital markdown sequence. Malformed sequences might lead to 
    unexpected results.
    """
    in_link = False
    summaries = []

    for part, next_part in view:
        if part == "[":
            # Recursively try to parse this new section - it's probably a link, but can be something else
            sub_summaries, text = markdownify_text([part], view)
            summaries.extend(sub_summaries)
            current.append(text)

        elif part == "]":
            if next_part == "(":
                # Indicate that it's in the URL part of a markdown link.
                current.append(part)
                in_link = True
            else:
                # Extract the descriptor, which might be a summary tag, TODO tag, or an Arbital internal link's "<page_id> <title>".
                descriptor = current[1]

                # Handle Arbital summary.
                if descriptor.startswith("summary"):
                    # descriptor will be something like "summary(Technical): <contents>", so we split by `:`
                    summary_tag, summary_content = "".join(current[1:]).split(":", 1)
                    return [f"{summary_tag}: {summary_content.strip()}"], ""

                # Handle TODO section (ignore it).
                if descriptor.startswith("todo"):
                    return [], ""

                # Handle Arbital link (e.g., "6w genie" -> "[6w genie](https://arbital.com/p/6w)").
                return [], parse_arbital_link(descriptor)

        elif in_link and part == ")":
            # this is the end of a markdown link - just join the contents, as they're already correct
            return [], "".join(current + [part])

        elif in_link and current[-1] == "(" and next_part != ")":
            # This link is strange... looks like it could be malformed?
            # Assuming that it's malformed and missing a closing `)`
            # This will remove any additional info in the link, but that seems a reasonable price?
            words = part.split(" ")
            return [], "".join(current + [words[0], ") ", " ".join(words[1:])])

        else:
            # Just your basic text - add it to the processed parts and go on your merry way
            current.append(part)

    # Otherwise just join all the parts back together
    return summaries, "".join(flatten(current)).strip()


def extract_text(text: str) -> Tuple[str, str]:
    parts = [i for i in re.split(r"([\[\]()])", text) if i]
    return markdownify_text([], zip(parts, parts[1:] + [None]))


@dataclass
class Arbital(AlignmentDataset):

    ARBITAL_SUBSPACES = ["ai_alignment", "math", "rationality"]
    done_key = "alias"
    headers = {
        "authority": "arbital.com",
        "accept": "application/json, text/plain, */*",
        "content-type": "application/json;charset=UTF-8",
        "sec-ch-ua-mobile": "?0",
        "origin": "https://arbital.com",
        "sec-fetch-site": "same-origin",
        "sec-fetch-mode": "cors",
        "sec-fetch-dest": "empty",
        "accept-language": "en-US,en;q=0.9",
    }
    titles_map: Dict[str, str] = field(default_factory=dict)

    @property
    def items_list(self) -> List[str]:
        logger.info("Getting page aliases")
        items = [
            alias
            for subspace in self.ARBITAL_SUBSPACES
            for alias in self.get_arbital_page_aliases(subspace)
        ]
        logger.info("Got %s page aliases", len(items))
        return items

    def get_item_key(self, item: str) -> str:
        return item

    def process_entry(self, alias: str):
        try:
            page = self.get_page(alias)
            summaries, text = extract_text(page["text"])

            return self.make_data_entry(
                {
                    "title": page.get("title") or "",
                    "text": text,
                    "date_published": self._get_published_date(page),
                    "url": f'https://arbital.com/p/{page.get("alias") or alias}',
                    "source": self.name,
                    "source_type": "text",
                    "authors": self.extract_authors(page),
                    "alias": alias,
                    "tags": list(filter(None, map(self.get_title, page["tagIds"]))),
                    "summaries": summaries,
                }
            )
        except Exception as e:
            logger.error(f"Error getting page {alias}: {e}")
        return None
    
    def send_post_request(self, url: str, page_alias: str, referer_base: str) -> requests.Response:
        headers = self.headers.copy()
        headers['referer'] = f"{referer_base}{page_alias}/"
        data = f'{{"pageAlias":"{page_alias}"}}'
        return requests.post(url, headers=headers, data=data)

    def get_arbital_page_aliases(self, subspace: str) -> List[str]:
        response = self.send_post_request(
            url='https://arbital.com/json/explore/',
            page_alias=subspace,
            referer_base='https://arbital.com/explore/'
        )
        return list(response.json()['pages'].keys())

    def get_page(self, alias: str) -> Page:
        response = self.send_post_request(
            url='https://arbital.com/json/primaryPage/',
            page_alias=alias,
            referer_base='https://arbital.com/p/'
        )
        return response.json()['pages'][alias]

    @staticmethod
    def _get_published_date(page: Page) -> datetime | None:
        date_published = page.get("editCreatedAt") or page.get("pageCreatedAt")
        if date_published:
            return parse(date_published).astimezone(timezone.utc)
        return None

    def get_title(self, itemId: str) -> str | None:
        if title := self.titles_map.get(itemId):
            return title

        try:
            page = self.get_page(itemId)
        except Exception as e:
            # give up on this item - maybe next time will work
            logger.error(e)
            return None

        if title := page.get("title"):
            self.titles_map[itemId] = title
            return title
        return None

    def extract_authors(self, page: Page) -> List[str]:
        """Get all authors of this page.

        This will work faster the more its used, as it only fetches info for authors it hasn't yet seen.
        """
        authors = {c["userId"] for c in page.get("changeLogs", [])}

        return list(filter(None, map(self.get_title, authors)))
