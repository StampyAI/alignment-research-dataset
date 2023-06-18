import logging
import requests

from align_data.common.alignment_dataset import AlignmentDataset, DataEntry
from dataclasses import dataclass

logger = logging.getLogger(__name__)

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
        except Exception as e:
            logger.error(f"Error getting page {alias}: {e}")
            page = {
                'title': 'Error getting page',
                'text': 'Error getting page',
                'date_published': 'Error getting page',
            }
        return DataEntry({
            'title': page['title'] if 'title' in page else 'n/a',
            'text': page['text'] if 'text' in page else 'n/a',
            'date_published': page.get('editCreatedAt') or page.get('pageCreatedAt') or 'n/a',
            'url': f'https://arbital.com/p/{page["alias"]}',
            'source': self.name,
            'source_filetype': 'text',
            'authors': [],
            'alias': alias,
        })

    def get_arbital_page_aliases(self, subspace):
        headers = self.headers.copy()
        headers['referer'] = f'https://arbital.com/explore/{subspace}/'
        data = f'{{"pageAlias":"{subspace}"}}'
        response = requests.post('https://arbital.com/json/explore/', headers=headers, data=data).json()
        return list(response['pages'].keys())

    def get_page(self, alias):
        headers = self.headers.copy()
        headers['referer'] = 'https://arbital.com/'
        data = f'{{"pageAlias":"{alias}"}}'
        response = requests.post('https://arbital.com/json/primaryPage/', headers=headers, data=data).json()
        return response['pages'][alias]
