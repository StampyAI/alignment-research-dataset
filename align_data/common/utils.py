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

htmlformatter = html2text.HTML2Text()
htmlformatter.ignore_links = True
htmlformatter.ignore_images = True
htmlformatter.body_width = 0


class HtmlCleaner:
    def __init__(self, rgx_list=[], target_list=[], DOTALL=False):
        """
        rgx_list: list of regexes to strip from the HTML
        """
        if isinstance(rgx_list, str):
            rgx_list = [rgx_list]

        self.DOTALL = DOTALL
        self.regexes = rgx_list if self.DOTALL else [re.compile(r) for r in rgx_list]
        self.target_list = [""]*len(self.regexes) if len(target_list) == 0 else target_list

    def clean(self, html, markdown=False):
        soup = bs4.BeautifulSoup(html, features="html.parser")
        text = htmlformatter.handle(html) if markdown else  soup.get_text()

        for ii , rgx in enumerate(self.regexes):
            text = re.sub(rgx, self.target_list[ii], text, flags=re.DOTALL) if self.DOTALL else rgx.sub(self.target_list[ii], text)

        return text


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
