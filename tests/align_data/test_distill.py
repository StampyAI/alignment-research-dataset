from unittest.mock import patch, Mock

import pytest
from bs4 import BeautifulSoup

from align_data.distill import Distill


def test_extract_authors():
    dataset = Distill(name='distill', url='bla.bla')

    contents = """
    <div class="authors-affiliations grid">
        <p class="author">
            <a class="name" href="https://ameya98.github.io/">Ameya Daigavane</a>
        </p>
        <p class="author">
            <a class="name" href="https://www.cse.iitm.ac.in/~ravi/">Balaraman Ravindran</a>
        </p>
        <p class="author">
            <a class="name" href="https://research.google/people/GauravAggarwal/">Gaurav Aggarwal</a>
        </p>
    </div>
    """
    soup = BeautifulSoup(contents, "html.parser")
    assert dataset.extract_authors({'soup': soup}) == ['Ameya Daigavane', 'Balaraman Ravindran', 'Gaurav Aggarwal']


@pytest.mark.parametrize('text', (
    '<d-article> bla bla <a href="bla.com">a link</a> ble \n\n</d-article>',
    '<dt-article> bla bla <a href="bla.com">a link</a> ble \n\n</dt-article>',
))
def test_get_text(text):
    dataset = Distill(name='distill', url='bla.bla')

    soup = BeautifulSoup(text, "html.parser")
    assert dataset._get_text({'soup': soup}) == "bla bla [a link](bla.com) ble"


def test_extra_values():
    dataset = Distill(name='distill', url='bla.bla')

    contents = """
    <div>
      <div>
        <h3>DOI</h3>
        <p><a href="https://doi.org/10.23915/distill.00032">10.23915/distill.00032</a></p>
      </div>
      <ol id="references-list" class="references">
        <li id="gnn-intro">
           <span class="title">A Gentle Introduction to Graph Neural Networks</span>
           <br>Sanchez-Lengeling, B., Reif, E., Pearce, A. and Wiltschko, A., 2021. Distill.
           <a href="https://doi.org/10.23915/distill.00033" style="text-decoration:inherit;">DOI: 10.23915/distill.00033</a>
        </li>
        <li id="graph-kernels">
           <span class="title">Graph Kernels</span>
           <a href="http://jmlr.org/papers/v11/vishwanathan10a.html">[HTML]</a>
           <br>Vishwanathan, S., Schraudolph, N.N., Kondor, R. and Borgwardt, K.M., 2010. Journal of Machine Learning Research, Vol 11(40), pp. 1201-1242.
        </li>
      </ol>
    </div>
    """

    soup = BeautifulSoup(contents, "html.parser")
    assert dataset._extra_values({'soup': soup, 'summary': 'A wild summary has appeared!'}) == {
        'bibliography': [
            {
                'link': 'https://doi.org/10.23915/distill.00033',
                'title': 'A Gentle Introduction to Graph Neural Networks'
            }, {
                'link': 'http://jmlr.org/papers/v11/vishwanathan10a.html',
                'title': 'Graph Kernels'
            }
        ],
        'doi': '10.23915/distill.00032',
        'journal_ref': 'distill-pub',
        'summary': 'A wild summary has appeared!',
    }


def test_process_entry():
    dataset = Distill(name='distill', url='bla.bla')
    contents = """
    <div>
      <div class="authors-affiliations grid">
        <p class="author">
            <a class="name" href="https://ameya98.github.io/">Ameya Daigavane</a>
        </p>
        <p class="author">
            <a class="name" href="https://www.cse.iitm.ac.in/~ravi/">Balaraman Ravindran</a>
        </p>
        <p class="author">
            <a class="name" href="https://research.google/people/GauravAggarwal/">Gaurav Aggarwal</a>
        </p>
      </div>
      <div>
        <h3>DOI</h3>
        <p><a href="https://doi.org/10.23915/distill.00032">10.23915/distill.00032</a></p>
      </div>
      <d-article> bla bla <a href="bla.com">a link</a> ble \n\n</d-article>
      <ol id="references-list" class="references">
        <li id="gnn-intro">
           <span class="title">A Gentle Introduction to Graph Neural Networks</span>
           <br>Sanchez-Lengeling, B., Reif, E., Pearce, A. and Wiltschko, A., 2021. Distill.
           <a href="https://doi.org/10.23915/distill.00033" style="text-decoration:inherit;">DOI: 10.23915/distill.00033</a>
        </li>
        <li id="graph-kernels">
           <span class="title">Graph Kernels</span>
           <a href="http://jmlr.org/papers/v11/vishwanathan10a.html">[HTML]</a>
           <br>Vishwanathan, S., Schraudolph, N.N., Kondor, R. and Borgwardt, K.M., 2010. Journal of Machine Learning Research, Vol 11(40), pp. 1201-1242.
        </li>
      </ol>
    </div>
    """

    items = {
        'entries': [
            {
                'link': 'http://example.org/bla',
                'title': 'the article title',
                'pubDate': 'Mon, 26 Jun 2023 13:40:01 GMT',
                'summary': 'A wild summary has appeared!',
            }
        ]
    }
    # Setup the items list contents
    with patch('feedparser.parse', return_value=items):
        dataset.items_list

    with patch('requests.get', return_value=Mock(content=contents)):
        assert dataset.process_entry('http://example.org/bla').to_dict() == {
            'authors': ['Ameya Daigavane', 'Balaraman Ravindran', 'Gaurav Aggarwal'],
            'bibliography': [
                {
                    'link': 'https://doi.org/10.23915/distill.00033',
                    'title': 'A Gentle Introduction to Graph Neural Networks'
                }, {
                    'link': 'http://jmlr.org/papers/v11/vishwanathan10a.html',
                    'title': 'Graph Kernels'
                }
            ],
            'date_published': '2023-06-26T13:40:01Z',
            'doi': '10.23915/distill.00032',
            'id': None,
            'journal_ref': 'distill-pub',
            'source': 'distill',
            'source_type': 'blog',
            'summaries': ['A wild summary has appeared!'],
            'text': 'bla bla [a link](bla.com) ble',
            'title': 'the article title',
            'url': 'http://example.org/bla',
        }
