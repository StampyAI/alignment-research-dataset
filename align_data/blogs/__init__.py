from align_data.blogs.markdown_blogs import MarkdownBlogs
from align_data.blogs.wp_blog import WordpressBlog
from align_data.blogs.medium_blog import MediumBlog
from align_data.blogs.gwern_blog import GwernBlog
from align_data.blogs.blogs import ColdTakes, GenerativeInk, CaradoMoe
from align_data.blogs.substack_blog import SubstackBlog


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
    MediumBlog(name="deepmind.blog", url="https://deepmindsafetyresearch.medium.com/", authors=["DeepMind Safety Research"]),
    GwernBlog(name="gwern_blog", url='https://www.gwern.net/', authors=["Gwern Branwen"]),
    ColdTakes(
        name="cold.takes",
        url="https://www.cold-takes.com/",
        authors=['Holden Karnofsky'],
    ),
    GenerativeInk(
        name="generative.ink",
        url="https://generative.ink/posts/",
        authors=['janus'],
    ),
    CaradoMoe(
        name="carado.moe",
        url='https://carado.moe/rss.xml',
        authors=['Tamsin Leake'],
    ),
    MarkdownBlogs(
        name="waitbutwhy",
        gdrive_address="https://drive.google.com/uc?id=1z0kbDd8vDsgOH6vV9z0XBnTQutXFJ6x-",
        authors=['Tim Urban'],
    ),
    SubstackBlog(
        name="import.ai",
        url="https://importai.substack.com"
    ),
    SubstackBlog(
        name="ml.safety.newsletter",
        url="https://newsletter.mlsafety.org"
    ),
]
