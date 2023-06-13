from .markdown_blogs import MarkdownBlogs
from .wp_blog import WordpressBlog
from .medium_blog import MediumBlog
from .gwern_blog import GwernBlog
from .html_blog import ColdTakes, GenerativeInk

BLOG_REGISTRY = [
    WordpressBlog(name="aiimpacts.org", url="https://aiimpacts.org"),
    WordpressBlog(name="aisafety.camp", url="https://aisafety.camp"),
    WordpressBlog(name="intelligence.org", url="https://intelligence.org"),
    WordpressBlog(name="jsteinhardt.wordpress.com", url="https://jsteinhardt.wordpress.com"),
    WordpressBlog(
        name="qualiacomputing.com",
        url="https://qualiacomputing.com",
        strip=["^by [^\n].*\n"]
    ),
    WordpressBlog(name="vkrakovna.wordpress.com", url="https://vkrakovna.wordpress.com"),
    WordpressBlog(
        name="yudkowsky.net",
        url="https://yudkowsky.net",
        strip=["^\s*Download as PDF\n"]
    ),
    MediumBlog(name="deepmind.blog", url="https://deepmindsafetyresearch.medium.com/"),
    GwernBlog(name="gwern_blog", url='https://www.gwern.net/'),
    ColdTakes(
        name="cold.takes",
        url="https://www.cold-takes.com/",
    ),
    GenerativeInk(
        name="generative.ink",
        url="https://generative.ink/posts/",
    ),
    MarkdownBlogs(
        name="carado.moe",
        gdrive_address="https://drive.google.com/uc?id=1Acom6FGTnulru3_Ek-Qnii8Hg_D-VHoz",
    ),
    MarkdownBlogs(
        name="waitbutwhy",
        gdrive_address="https://drive.google.com/uc?id=1z0kbDd8vDsgOH6vV9z0XBnTQutXFJ6x-",
    ),
]
