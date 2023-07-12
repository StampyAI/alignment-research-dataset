import bs4
import jsonlines
import os
import re
import time
import html2text
from urllib.parse import urlparse
import os
from functools import reduce
import operator
import unicodedata
import re


def random_delay():
    import random
    time.sleep(random.randint(1, 10))


def url_to_filename(url):
    """
    Convert a URL to a suitable filename.
    """
    url = urlparse(url)
    path = url.path.lstrip(os.sep).rstrip(os.sep).split(os.sep)
    return "-".join([url.netloc] + list(filter(None, path)))


class ExitCodeError(Exception):
    pass


def sh(x):
    if os.system(x):
        raise ExitCodeError()


def ls(x):
    return [x + "/" + fn for fn in os.listdir(x)]


def lsr(x):
    if os.path.isdir(x):
        return reduce(operator.add, map(lsr, ls(x)), [])
    else:
        return [x]


def fwrite(fname, content):
    with open(fname, "w") as fh:
        fh.write(content)


def fread(fname):
    with open(fname) as fh:
        return fh.read()


def chdir_up_n(n):
    """Goes up n times in the directory tree."""
    for i in range(n):
        os.chdir("..")


def slugify(value, allow_unicode=False):
    """
    Taken from https://github.com/django/django/blob/master/django/utils/text.py
    Convert to ASCII if 'allow_unicode' is False. Convert spaces or repeated
    dashes to single dashes. Remove characters that aren't alphanumerics,
    underscores, or hyphens. Convert to lowercase. Also strip leading and
    trailing whitespace, dashes, and underscores.
    """
    value = str(value)
    if allow_unicode:
        value = unicodedata.normalize('NFKC', value)
    else:
        value = unicodedata.normalize('NFKD', value).encode('ascii', 'ignore').decode('ascii')
    value = re.sub(r'[^\w\s-]', '', value.lower())
    return re.sub(r'[-\s]+', '-', value).strip('-_')
