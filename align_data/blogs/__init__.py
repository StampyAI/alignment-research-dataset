from align_data.blogs.markdown_blogs import MarkdownBlogs
from align_data.blogs.wp_blog import WordpressBlog
from align_data.blogs.medium_blog import MediumBlog
from align_data.blogs.gwern_blog import GwernBlog
from align_data.blogs.blogs import ColdTakes, GenerativeInk, CaradoMoe
from align_data.blogs.substack_blog import SubstackBlog


BLOG_REGISTRY = [
    WordpressBlog(name="aiimpacts", url="https://aiimpacts.org"),
    WordpressBlog(name="aisafety.camp", url="https://aisafety.camp"),
    WordpressBlog(name="miri", url="https://intelligence.org"),
    WordpressBlog(name="jsteinhardt_blog", url="https://jsteinhardt.wordpress.com"),
    WordpressBlog(
        name="qualiacomputing",
        url="https://qualiacomputing.com",
        strip=["^by [^\n].*\n"]
    ),
    WordpressBlog(name="vkrakovna_blog", url="https://vkrakovna.wordpress.com"),
    WordpressBlog(
        name="yudkowsky_blog",
        url="https://yudkowsky.net",
        strip=["^\s*Download as PDF\n"]
    ),
    MediumBlog(name="deepmind_blog", url="https://deepmindsafetyresearch.medium.com/", authors=["DeepMind Safety Research"]),
    GwernBlog(name="gwern_blog", url='https://www.gwern.net/', authors=["Gwern Branwen"]),
    ColdTakes(
        name="cold_takes",
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
        url='https://carado.moe',
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
        name="ml_safety_newsletter",
        url="https://newsletter.mlsafety.org"
    ),
]
