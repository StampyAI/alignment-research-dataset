from dataclasses import dataclass
import logging

from align_data.common.html_dataset import HTMLDataset

logger = logging.getLogger(__name__)


@dataclass
class MediumBlog(HTMLDataset):
    """
    Fetches articles from a Medium blog.

    Pulls Medium articles by walking the archive. Depending on the activity of the blog
    during a particular year, the archive for the year may consist of a single page only, or
    may have daily pages. A single blog can use different layouts for different years.

    Also, if the blog had few posts overall, an archive may not exist at all. In that case,
    the main page is used to fetch the articles. The entries are assumed to fit onto
    a single page, which seems to be the case for blogs without an archive.

    It is possible that there is additional variation in the layout that hasn't been represented
    in the blogs tested so far. In that case, additional fixes to this code may be needed.

    This implementation was originally based on
    https://dorianlazar.medium.com/scraping-medium-with-python-beautiful-soup-3314f898bbf5,
    but various fixes were added to handle a wider range of Medium blogs.
    """

    source_type = "medium_blog"
    ignored_selectors = ["div:first-child span"]

    def _get_published_date(self, contents):
        possible_date_elements = contents.select("article div:first-child span")
        return self._find_date(possible_date_elements)