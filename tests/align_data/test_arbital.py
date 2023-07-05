import pytest

from align_data.arbital.arbital import parse_arbital_link, flatten, markdownify_text, extract_text


@pytest.mark.parametrize('contents, expected', (
    (['[', '123'], '[https://arbital.com/p/123](https://arbital.com/p/123)'),
    (['[', '123 Some title'], '[Some title](https://arbital.com/p/123)'),
    (['[', '123 Some title with multiple words'], '[Some title with multiple words](https://arbital.com/p/123)'),
))
def test_parse_arbital_link(contents, expected):
   assert parse_arbital_link(contents) == expected


@pytest.mark.parametrize('input, expected', (
    ([1, 2, 3], [1, 2, 3]),
    ([1, [2, [3], 4]], [1, 2, 3, 4]),
    ((1, (2, 3), 4), [1, 2, 3, 4]),
    ([], []),
    ([5], [5]),
    ([1, 'a', [2, ['b'], 3]], [1, 'a', 2, 'b', 3]),
    ([1, None, [2, [3], None]], [1, None, 2, 3, None]),
))
def test_flatten(input, expected):
    assert flatten(input) == expected


@pytest.mark.parametrize('text', (
    ''
    'asdasd asd asd as',
    'Stuff that is in parenthesizes (like this) should be left alone'
    'Markdown links [like this](https://bla.bla.com) should not be changed',
))
def test_markdownify_text_contents_basic_markdown(text):
    _, result = extract_text(text)
    assert result == text


@pytest.mark.parametrize('text, expected', (
    ('Arbital links [123 like this] should be transformed', 'Arbital links [like this](https://arbital.com/p/123) should be transformed'),
    ('[summary: summaries should be removed] bla bla bla', 'bla bla bla'),

    ('    \n \t \n contents get stripped of whitespace    \t \n', 'contents get stripped of whitespace'),
    ('malformed [links](http://bla.bla are handled somewhat', 'malformed [links](http://bla.bla) are handled somewhat')
))
def test_markdownify_text_contents_arbital_markdown(text, expected):
    _, result = extract_text(text)
    assert result == expected


@pytest.mark.parametrize('text, expected', (
    ('[summary: summaries should be extracted] bla bla bla', 'summaries should be extracted'),
    ('[summary: \n    whitespace should be stripped       \n] bla bla bla', 'whitespace should be stripped'),

    ('[summary(Bold): special summaries should be extracted] bla bla bla', 'special summaries should be extracted'),
    ('[summary(Markdown): special summaries should be extracted] bla bla bla', 'special summaries should be extracted'),
    ('[summary(BLEEEE): special summaries should be extracted] bla bla bla', 'special summaries should be extracted'),

    ('[summary: markdown is handled: [bla](https://bla.bla)] bla bla bla', 'markdown is handled: [bla](https://bla.bla)'),
    ('[summary: markdown is handled: [123 ble ble]] bla bla bla', 'markdown is handled: [ble ble](https://arbital.com/p/123)'),
))
def test_markdownify_text_summary(text, expected):
    summary, _ = extract_text(text)
    assert summary == expected
