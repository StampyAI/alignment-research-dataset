import re
import logging
import requests
from datetime import datetime, timezone
from dateutil.parser import parse

from align_data.common.alignment_dataset import AlignmentDataset
from dataclasses import dataclass

logger = logging.getLogger(__name__)


def parse_arbital_link(contents):
    text = contents[1].split(' ')
    url = f'https://arbital.com/p/{text[0]}'
    if len(text) > 1:
        title = ' '.join(text[1:])
    else:
        title = url
    return f'[{title}]({url})'


def flatten(val):
    if isinstance(val, (list, tuple)):
        return [item for i in val for item in flatten(i)]
    return [val]


def markdownify_text(current, view):
    """Recursively parse the text parts in `view` to create a markdown AST from them.

    Arbital adds some funky extra stuff to markdown. The known things are:
    * "[summary: <contents>]" blocks to add summaries
    * "[123 <title>]" are internal links to `<123>`

    The `view` parameter should be a generator, so recursive calls can iterate over it without needing
    to mess about with indexes etc.

    :param List[str] current: the list of parsed items. Should generally be passed in as `[]`
    :param generator(str, str) view: a generator that returns `part` and `next_part`, where `part` is the current item
                                     and `next_part` is a lookahead

    :returns: a tuple of `(<summary string>, <markdown contents>)`
    """
    in_link = False

    for part, next_part in view:
        if part == '[':
            # Recursively try to parse this new section - it's probably a link, but can be something else
            current.append(markdownify_text([part], view))
        elif part == ']' and next_part == '(':
            # mark that it's now in the url part of a markdown link
            current.append(']')
            in_link = True
        elif part == ']':
            # this is the arbital summary - just join it for now, but it'll have to be handled later
            if current[1].startswith('summary'):
                return ''.join(current[1:])
            # if this was a TODO section, then ignore it
            if current[1].startswith('todo'):
                return ''
            # Otherwise it's an arbital link
            return parse_arbital_link(current)
        elif in_link and part == ')':
            # this is the end of a markdown link - just join the contents, as they're already correct
            return ''.join(current + [part])
        elif in_link and current[-1] == '(' and next_part != ')':
            # This link is strange... looks like it could be malformed?
            # Assuming that it's malformed and missing a closing `)`
            # This will remove any additional info in the link, but that seems a reasonable price?
            words = part.split(' ')
            return ''.join(current + [words[0], ') ', ' '.join(words[1:])])
        else:
            # Just your basic text - add it to the processed parts and go on your merry way
            current.append(part)

    # Check if the first item is the summary - if so, extract it
    summary = ''
    if current[0].startswith('summary'):
        _, summary = re.split(r'summary[()\w]*:', current[0], 1)
        current = current[1:]

    # Otherwise just join all the parts back together
    return summary.strip(), ''.join(flatten(current)).strip()


def extract_text(text):
    parts = [i for i in re.split('([\[\]()])', text) if i]
    return markdownify_text([], zip(parts, parts[1:] + [None]))

@dataclass
class Arbital(AlignmentDataset):
    summary_key: str = 'summary'

    ARBITAL_SUBSPACES = ['ai_alignment', 'math', 'rationality']
    done_key = "alias"
    headers = {
        'authority': 'arbital.com',
        'accept': 'application/json, text/plain, */*',
        'content-type': 'application/json;charset=UTF-8',
        'sec-ch-ua-mobile': '?0',
        'origin': 'https://arbital.com',
        'sec-fetch-site': 'same-origin',
        'sec-fetch-mode': 'cors',
        'sec-fetch-dest': 'empty',
        'accept-language': 'en-US,en;q=0.9',
    }
    titles_map = {}

    @property
    def items_list(self):
        logger.info('Getting page aliases')
        items = [alias for subspace in self.ARBITAL_SUBSPACES for alias in self.get_arbital_page_aliases(subspace)]
        logger.info('Got %s page aliases', len(items))
        return items

    def get_item_key(self, item):
        return item

    def process_entry(self, alias):
        try:
            page = self.get_page(alias)
            summary, text = extract_text(page['text'])

            return self.make_data_entry({
                'title': page.get('title') or '',
                'text': text,
                'date_published': self._get_published_date(page),
                'url': f'https://arbital.com/p/{page.get("alias") or alias}',
                'source': self.name,
                'source_type': 'text',
                'authors': self.extract_authors(page),
                'alias': alias,
                'tags': list(filter(None, map(self.get_title, page['tagIds']))),
                'summary': [summary] if summary else [],
            })
        except Exception as e:
            logger.error(f"Error getting page {alias}: {e}")
        return None

    def get_arbital_page_aliases(self, subspace):
        headers = self.headers.copy()
        headers['referer'] = f'https://arbital.com/explore/{subspace}/'
        data = f'{{"pageAlias":"{subspace}"}}'
        response = requests.post('https://arbital.com/json/explore/', headers=headers, data=data).json()
        return list(response['pages'].keys())

    @staticmethod
    def _get_published_date(page):
        date_published = page.get('editCreatedAt') or page.get('pageCreatedAt')
        if date_published:
            dt = parse(date_published).astimezone(timezone.utc)
            return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        return ''

    def get_page(self, alias):
        headers = self.headers.copy()
        headers['referer'] = 'https://arbital.com/'
        data = f'{{"pageAlias":"{alias}"}}'
        response = requests.post('https://arbital.com/json/primaryPage/', headers=headers, data=data)
        return response.json()['pages'][alias]

    def get_title(self, itemId):
        if title := self.titles_map.get(itemId):
            return title

        try:
            page = self.get_page(itemId)
        except Exception as e:
            # give up on this item - maybe next time will work
            logger.error(e)
            return None

        if title := page.get('title'):
            self.titles_map[itemId] = title
            return title
        return None

    def extract_authors(self, page):
        """Get all authors of this page.

        This will work faster the more its used, as it only fetches info for authors it hasn't yet seen.
        """
        authors = {c['userId'] for c in page.get('changeLogs', [])}

        return list(filter(None, map(self.get_title, authors)))
