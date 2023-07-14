from dataclasses import dataclass
from markdownify import markdownify
from align_data.common.html_dataset import RSSDataset


@dataclass
class Distill(RSSDataset):
    source_type = 'html'
    done_key = 'url'
    summary_key = 'summary'

    def extract_authors(self, item):
        return [a.text for a in item['soup'].select('.authors-affiliations p.author a')]

    def _get_text(self, item):
        article = item['soup'].find('d-article') or item['soup'].find('dt-article')
        return self._extract_markdown(article)

    def _extra_values(self, item):
        soup = item['soup']

        doi_elem = soup.find('h3', string='DOI')
        doi_elem = doi_elem and doi_elem.find_next_sibling('p')

        return {
            'doi': doi_elem and doi_elem.text,
            'summary': item['summary'],
            'journal_ref': 'distill-pub',
            'bibliography': [
                {'title': el.find('span').text, 'link': el.find('a').get('href')}
                for el in soup.select('.references li') if el.find('a')
            ]
        }
