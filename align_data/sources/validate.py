import logging
from datetime import datetime, timedelta
from typing import Any, List

from tqdm import tqdm
from sqlalchemy.exc import IntegrityError
from align_data.db.session import make_session
from align_data.db.models import Article
from align_data.common.alignment_dataset import normalize_url, normalize_text, article_dict
from align_data.sources.articles.parsers import item_metadata
from align_data.sources.articles.html import fetch


logger = logging.getLogger(__name__)


def update_article_field(article: Article, field: str, value: Any):
    if not value:
        return

    if field == 'url' and normalize_url(article.url) == normalize_url(value):
        # This is pretty much the same url, so don't modify it
        return
    if field == 'title' and normalize_text(article.title) == normalize_text(value):
        # If there are slight differences in the titles (e.g. punctuation), assume the
        # database version is more correct
        return
    if field == 'meta':
        article.meta = article.meta or {}
        for k, v in value.items():
            meta_val = article.meta.get(k)
            if not meta_val or v > meta_val:
                article.meta[k] = v
        return

    article_val = getattr(article, field, None)
    # Assume that if the provided value is larger (or later, in the case of dates), then it's
    # better. This might very well not hold, but it seems like a decent heuristic?
    if not article_val:
        setattr(article, field, value)
    elif isinstance(value, datetime) and value > article_val:
        setattr(article, field, value)
    elif isinstance(value, str) and len(normalize_text(value) or '') > len(normalize_text(article_val) or ''):
        setattr(article, field, normalize_text(value))


def check_article(article: Article) -> Article:
    source_url = article.meta.get('source_url') or article.url
    contents = {}
    if source_url:
        contents = item_metadata(source_url)

    if 'error' not in contents:
        for field, value in article_dict(contents).items():
            update_article_field(article, field, value)
    else:
        logger.info('Error getting contents for %s: %s', article, contents.get('error'))

    if 400 <= fetch(article.url).status_code < 500:
        logger.info('Could not get url for %s', article)
        article.status = 'Unreachable url'

    article.date_checked = datetime.utcnow()

    return article


def check_articles(sources: List[str], batch_size=100):
    logger.info('Checking %s articles for %s', batch_size, ', '.join(sources))
    with make_session() as session:
        for article in tqdm(
            session.query(Article)
            .filter(Article.date_checked < datetime.now() - timedelta(weeks=4))
            .filter(Article.source.in_(sources))
            .limit(batch_size)
            .all()
        ):
            check_article(article)
            session.add(article)
        logger.debug('commiting')
        try:
            session.commit()
        except IntegrityError as e:
            logger.error(e)
